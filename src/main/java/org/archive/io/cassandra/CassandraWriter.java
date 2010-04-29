package org.archive.io.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.modules.CrawlURI;

/**
 * Cassandra implementation for Heritrix writing.
 *
 * @author greglu
 */
public class CassandraWriter extends WriterPoolMember {

	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	private CassandraParameters cassandraParameters;
	private Cassandra.Client _client;
	private TSocket _socket;

	/**
	 * @see org.archive.io.cassandra.CassandraParameters
	 */
	public CassandraParameters getCassandraParameters() {
		return cassandraParameters;
	}

	public Cassandra.Client getClient() {
		return _client;
	}

	public CassandraWriter(String cassandraServers, int cassandraPort, CassandraParameters parameters) throws IOException, TTransportException {
		super(null, null, null, false, null);

		this.cassandraParameters = parameters;

		// Randomly chosing a server from the list
		String [] seeds = cassandraServers.split(",");
		String seed;

		if (seeds != null && seeds.length > 0) {
			seed = seeds[new Random().nextInt(seeds.length)];
		} else throw new RuntimeException("No seeds found in configuration.");

		_socket = new TSocket(seed, cassandraPort);

		TBinaryProtocol binaryProtocol = new TBinaryProtocol(_socket, false, false);

		_client = new Cassandra.Client(binaryProtocol);
		_socket.open();
	}

	/**
     * Write the crawled output to the configured HBase table.
     * Write each row key as the url with reverse domain and optionally process any content.
     * 
     * @param curi URI of crawled document
     * @param ip IP of remote machine.
     * @param recordingOutputStream recording input stream that captured the response
     * @param recordingInputStream recording output stream that captured the GET request
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void write(final CrawlURI curi, final String ip, final RecordingOutputStream recordingOutputStream, 
            final RecordingInputStream recordingInputStream) throws IOException {
        // Generate the target url of the crawled document
        String url = curi.toString();

        // Create the key (reverse url)
        String key = UrlKey.createKey(url);

        if (LOG.isTraceEnabled())
            LOG.trace("Writing " + url + " as " + key);

        // List for the 'curi' super column
        List<Column> curiColumns = new ArrayList<Column>();

        // List for the 'content' super column
        List<Column> contentColumns = new ArrayList<Column>();

        // The timestmap is the curi fetch time in microseconds
        long timestamp = curi.getFetchBeginTime()*1000;

        // The encoding scheme
        String encoding = getCassandraParameters().getEncodingScheme();


        // write the target url to the url column
        curiColumns.add(new Column(getCassandraParameters().getUrlSubColumn().getBytes(encoding), url.getBytes(encoding), timestamp));

        // write the target ip to the ip column
        curiColumns.add(new Column(getCassandraParameters().getIpSubColumn().getBytes(encoding), ip.getBytes(encoding), timestamp));

        // is the url part of the seed url (the initial url(s) used to start the crawl)
        if (curi.isSeed()) {
        	curiColumns.add(new Column(getCassandraParameters().getIsSeedSubColumn().getBytes(encoding), new byte[]{(byte)-1}, timestamp));
        	if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0) {
        		curiColumns.add(new Column(getCassandraParameters().getPathFromSeedSubColumn().getBytes(encoding), curi.getPathFromSeed().trim().getBytes(encoding), timestamp));
        	}
        }

        // write the Via string
        String viaStr = (curi.getVia() != null) ? curi.getVia().toString().trim() : null;
        if (viaStr != null && viaStr.length() > 0) {
        	curiColumns.add(new Column(getCassandraParameters().getViaSubColumn().getBytes(encoding), viaStr.getBytes(encoding), timestamp));
        }
        
        // Write the Crawl Request to the Put object
        if (recordingOutputStream.getSize() > 0) {
        	curiColumns.add(new Column(	getCassandraParameters().getRequestSubColumn().getBytes(encoding),
        								getByteArrayFromInputStream(recordingOutputStream.getReplayInputStream(), (int)recordingOutputStream.getSize()),
        								timestamp));
        }
        
        // Write the Crawl Response to the Put object
        ReplayInputStream replayInputStream = recordingInputStream.getReplayInputStream();
        try {
        	// add the raw content to the table record
        	contentColumns.add(new Column(	getCassandraParameters().getContentSubColumn().getBytes(encoding),
        									getByteArrayFromInputStream(replayInputStream, (int) recordingInputStream.getSize()),
        									timestamp));

        	// reset the input steam for the content processor
        	replayInputStream = recordingInputStream.getReplayInputStream();
        	replayInputStream.setToResponseBodyStart();
        } finally {
        	closeStream(replayInputStream);
        }

        // Wrapping everything up and writing to Cassandra

        // Representing the 'curi' and 'content' super columns
        SuperColumn curiSuperColumn = new SuperColumn(getCassandraParameters().getCuriSuperColumn().getBytes(encoding), curiColumns);
        ColumnOrSuperColumn curiColumnOrSuperColumn = new ColumnOrSuperColumn();
        curiColumnOrSuperColumn.setSuper_column(curiSuperColumn);

        SuperColumn contentSuperColumn = new SuperColumn(getCassandraParameters().getContentSuperColumn().getBytes(encoding), contentColumns);
        ColumnOrSuperColumn contentColumnOrSuperColumn = new ColumnOrSuperColumn();
        contentColumnOrSuperColumn.setSuper_column(contentSuperColumn);

        // Creating the list of 'mutation' objects for Cassandra
        List<Mutation> mutations = new ArrayList<Mutation>();

        Mutation curiMutation = new Mutation();
        curiMutation.setColumn_or_supercolumn(curiColumnOrSuperColumn);
        mutations.add(curiMutation);

        Mutation contentMutation = new Mutation();
        contentMutation.setColumn_or_supercolumn(contentColumnOrSuperColumn);
        mutations.add(contentMutation);

        Map<String, List<Mutation>> mutationsForColumnFamily = new HashMap<String, List<Mutation>>();
        mutationsForColumnFamily.put(getCassandraParameters().getCrawlColumnFamily(), mutations);

        Map<String, Map<String, List<Mutation>>> job = new HashMap<String, Map<String, List<Mutation>>>();
        job.put(key, mutationsForColumnFamily);

        // Submitting the writes to the Cassandra client
        try {
			_client.batch_mutate(getCassandraParameters().getKeyspace(), job, ConsistencyLevel.ONE);
		} catch (Exception e) {
			LOG.error("The following exception was encountered while writing key '" + key + "':\n" + e.getMessage());
		}
    }

	@Override
	public void close() throws IOException {
		this._socket.close();
		super.close();
	}

    /**
     * Read the ReplayInputStream and write it to the given BatchUpdate with the given column.
     *
     * @param replayInputStream the ris the cell data as a replay input stream
     * @param streamSize the size
     *
     * @return the byte array from input stream
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected byte[] getByteArrayFromInputStream(final ReplayInputStream replayInputStream, final int streamSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(streamSize);
        try {
            // read the InputStream to the ByteArrayOutputStream
            replayInputStream.readFullyTo(baos);
        } finally {
            replayInputStream.close();
        }
        baos.close();
        return baos.toByteArray();
    }

    protected void closeStream(Closeable c) {
    	if (c != null) {
    		try {
    			c.close();
    		} catch(IOException e) {
    			if (LOG.isDebugEnabled())
    				LOG.debug("Exception in closing " + c, e);
    		}
    	}
    }
}
