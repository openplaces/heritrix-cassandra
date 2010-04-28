package org.archive.io.cassandra;

import java.io.IOException;
import java.util.Random;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.modules.CrawlURI;

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
        // generate the target url of the crawled document
        String url = curi.toString();

        // create the key (reverse url)
        String rowKey = UrlKey.createKey(url);

        if (LOG.isTraceEnabled())
            LOG.trace("Writing " + url + " as " + rowKey);


        // write the target url to the url column
//        batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getUrlColumnName()), curi.getFetchBeginTime(), Bytes.toBytes(url));

        // write the target ip to the ip column
//        batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getIpColumnName()), curi.getFetchBeginTime(), Bytes.toBytes(ip));

        // is the url part of the seed url (the initial url(s) used to start the crawl)
//        if (curi.isSeed()) {
//            batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getIsSeedColumnName()), Bytes.toBytes(Boolean.TRUE));
//            if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0) {
//                batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getPathFromSeedColumnName()), Bytes.toBytes(curi.getPathFromSeed().trim()));
//            }
//        }

        // write the Via string
//        String viaStr = (curi.getVia() != null) ? curi.getVia().toString().trim() : null;
//        if (viaStr != null && viaStr.length() > 0) {
//            batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getViaColumnName()), Bytes.toBytes(viaStr));
//        }
        
        // Write the Crawl Request to the Put object
//        if (recordingOutputStream.getSize() > 0) {
//            batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getRequestColumnName()), 
//                    getByteArrayFromInputStream(recordingOutputStream.getReplayInputStream(), (int) recordingOutputStream.getSize()));
//        }
        
        // Write the Crawl Response to the Put object
//        ReplayInputStream replayInputStream = recordingInputStream.getReplayInputStream();
//        try {
            // add the raw content to the table record.
//            batchPut.add(Bytes.toBytes(getHbaseOptions().getContentColumnFamily()), Bytes.toBytes(getHbaseOptions().getContentColumnName()),
//                    getByteArrayFromInputStream(replayInputStream, (int) recordingInputStream.getSize()));
            // reset the input steam for the content processor.
//            replayInputStream = recordingInputStream.getReplayInputStream();
//            replayInputStream.setToResponseBodyStart();

            // process the content (optional)
//            processContent(batchPut, replayInputStream, (int) recordingInputStream.getSize());

            // Set crawl time as the timestamp to the Put object.
//            batchPut.setTimeStamp(curi.getFetchBeginTime());

            // write the Put object to the HBase table
//            getClient().put(batchPut);
//        } finally {
//            IOUtils.closeStream(replayInputStream);
//        }

    }

	@Override
	public void close() throws IOException {
		this._socket.close();
		super.close();
	}

}
