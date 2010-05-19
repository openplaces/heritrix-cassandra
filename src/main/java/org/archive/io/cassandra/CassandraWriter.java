package org.archive.io.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.log4j.Logger;
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

	private CassandraParameters _cassandraParameters;
	private Connection _connection;

	/**
	 * @see org.archive.io.cassandra.CassandraParameters
	 */
	public CassandraParameters getCassandraParameters() {
		return _cassandraParameters;
	}

	public Connection getConnection() {
		return _connection;
	}

	public CassandraWriter(Connection connection, CassandraParameters parameters)
		throws IOException, TTransportException {

		super(null, null, null, false, null);

		this._cassandraParameters = parameters;
		this._connection = connection;
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

        // The timestmap is the curi fetch time in microseconds
        long timestamp = curi.getFetchBeginTime()*1000;

        // The encoding scheme
        String encoding = getCassandraParameters().getEncodingScheme();

        // Stores all the columns
        List<Column> columnList = new ArrayList<Column>();

        // write the target url to the url column
        columnList.add(
        		new Column(getCassandraParameters().getUrlColumnName().getBytes(encoding),
        				serialize(url.getBytes(encoding)), timestamp));

        // write the target ip to the ip column
        columnList.add(new Column(getCassandraParameters().getIpColumnName().getBytes(encoding),
        						serialize(ip.getBytes(encoding)), timestamp));

        // is the url part of the seed url (the initial url(s) used to start the crawl)
        if (curi.isSeed()) {
        	columnList.add(
        			new Column(getCassandraParameters().getIsSeedColumnName().getBytes(encoding),
        					serialize(new byte[]{(byte)-1}), timestamp));

        	if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0) {
        		columnList.add(
        				new Column(getCassandraParameters().getPathFromSeedColumnName().getBytes(encoding),
        						serialize(curi.getPathFromSeed().trim().getBytes(encoding)), timestamp));
        	}
        }

        // write the Via string
        String viaStr = (curi.getVia() != null) ? curi.getVia().toString().trim() : null;
        if (viaStr != null && viaStr.length() > 0) {
        	columnList.add(
        			new Column(getCassandraParameters().getViaColumnName().getBytes(encoding),
        					serialize(viaStr.getBytes(encoding)), timestamp));
        }
        
        // Write the Crawl Request to the Put object
        if (recordingOutputStream.getSize() > 0) {
        	columnList.add(
        			new Column(getCassandraParameters().getRequestColumnName().getBytes(encoding),
        				serialize(getByteArrayFromInputStream(recordingOutputStream.getReplayInputStream(),
        						(int)recordingOutputStream.getSize())), timestamp));
        }
        
        // Write the Crawl Response to the Put object
        ReplayInputStream replayInputStream = recordingInputStream.getReplayInputStream();
        try {
        	// add the raw content to the table record
        	columnList.add(
        			new Column(getCassandraParameters().getContentColumnName().getBytes(encoding),
        				serialize(getByteArrayFromInputStream(replayInputStream,
        						(int) recordingInputStream.getSize())), timestamp));

        	// reset the input steam for the content processor
        	replayInputStream = recordingInputStream.getReplayInputStream();
        	replayInputStream.setToResponseBodyStart();
        } finally {
        	closeStream(replayInputStream);
        }

        // Wrapping everything up and writing to Cassandra

        // Creating the list of 'mutation' objects for Cassandra
        List<Mutation> mutations = generateMutations(columnList);

        Map<String, List<Mutation>> mutationsForColumnFamily = new HashMap<String, List<Mutation>>();
        mutationsForColumnFamily.put(getCassandraParameters().getCrawlColumnFamily(), mutations);

        Map<String, Map<String, List<Mutation>>> job = new HashMap<String, Map<String, List<Mutation>>>();
        job.put(key, mutationsForColumnFamily);

        // Submitting the writes to the Cassandra client
        try {
			this._connection.client().batch_mutate(getCassandraParameters().getKeyspace(), job, ConsistencyLevel.ONE);
		} catch (Exception e) {
			LOG.error("The following exception was encountered while writing key '" + key + "':\n" + e.getMessage());
		}
    }

    private List<Mutation> generateMutations(List<Column> columns) {
    	List<Mutation> mutations = new ArrayList<Mutation>();

    	for (Column column : columns) {
    		ColumnOrSuperColumn c = new ColumnOrSuperColumn();
    		c.setColumn(column);

    		Mutation mutation = new Mutation();
    		mutation.setColumn_or_supercolumn(c);
    		mutations.add(mutation);
    	}

    	return mutations;
    }

	@Override
	public void close() throws IOException {
		this._connection.close();
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
    protected byte[] getByteArrayFromInputStream(final ReplayInputStream replayInputStream, final int streamSize)
    	throws IOException {

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
    
    /**
     * Override if you want to serialize bytes in a custom manner.
     * @param bytes
     * @return serialized bytes
     */
    protected byte[] serialize(byte[] bytes) {
    	return bytes;
    }
}
