package org.archive.io.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.modules.CrawlURI;
import org.archive.util.ArchiveUtils;

/**
 * Cassandra implementation for Heritrix writing.
 *
 * @author greglu
 */
public class CassandraWriter extends WriterPoolMember implements Serializer {

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
	 * Write the crawled output to the configured Cassandra table.
	 * Write each row key as the url with reverse domain and optionally process any content.
	 *
	 * @param curi URI of crawled document
	 * @param ip IP of remote machine.
	 * @param recordingOutputStream recording input stream that captured the response
	 * @param recordingInputStream recording output stream that captured the GET request
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 */
	public void write(final CrawlURI curi, final String ip, final RecordingOutputStream recordingOutputStream,
			final RecordingInputStream recordingInputStream) throws IOException, InterruptedException {

		// Generate the target url of the crawled document
		String url = curi.toString();

		// Create the key (reverse url)
		String key = UrlKey.createKey(url);

		// The encoding scheme
		String encoding = getCassandraParameters().getEncodingScheme();

		if (getCassandraParameters().isRemoveMissingPages() &&
				(curi.getFetchStatus() == HttpURLConnection.HTTP_NOT_FOUND || curi.getFetchStatus() == HttpURLConnection.HTTP_GONE)) {
			if (LOG.isDebugEnabled())
				LOG.debug("Removing key " + key);

			ColumnPath path = new ColumnPath(getCassandraParameters().getCrawlColumnFamily());
			try {
				this._connection.getClient().remove(ByteBuffer.wrap(key.getBytes(encoding)), path,
						currentMicroseconds(), ConsistencyLevel.QUORUM);
			} catch (Exception e) {
				// An exception should usually mean that the key didn't exist in the first place.
				// It's quicker to just try the delete rather than check first.
				LOG.debug("Exception occurred while removing '" + key + "'\n" + e.getMessage());
			}
		} else {
			if (LOG.isDebugEnabled())
				LOG.debug("Writing " + url + " as " + key);

			// The timestmap is the curi fetch time in microseconds
			long timestamp = curi.getFetchBeginTime()*1000;

			// Stores all the columns
			List<Column> columnList = new ArrayList<Column>();

			// write the target url to the url column
			columnList.add(
					new Column(ByteBuffer.wrap(getCassandraParameters().getUrlColumnName().getBytes(encoding)),
							ByteBuffer.wrap(serialize(url.getBytes(encoding))), timestamp));

			// write the target ip to the ip column
			columnList.add(new Column(ByteBuffer.wrap(getCassandraParameters().getIpColumnName().getBytes(encoding)),
					ByteBuffer.wrap(serialize(ip.getBytes(encoding))), timestamp));

			// is the url part of the seed url (the initial url(s) used to start the crawl)
			if (curi.isSeed()) {
				columnList.add(
						new Column(ByteBuffer.wrap(getCassandraParameters().getIsSeedColumnName().getBytes(encoding)),
								ByteBuffer.wrap(serialize(new byte[]{(byte)-1})), timestamp));
			}

			if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0) {
				columnList.add(
						new Column(ByteBuffer.wrap(getCassandraParameters().getPathFromSeedColumnName().getBytes(encoding)),
								ByteBuffer.wrap(serialize(curi.getPathFromSeed().trim().getBytes(encoding))), timestamp));
			}

			// write the Via string
			String viaStr = (curi.getVia() != null) ? curi.getVia().toString().trim() : null;
			if (viaStr != null && viaStr.length() > 0) {
				columnList.add(
						new Column(ByteBuffer.wrap(getCassandraParameters().getViaColumnName().getBytes(encoding)),
								ByteBuffer.wrap(serialize(viaStr.getBytes(encoding))), timestamp));
			}

			String fetchTime = ArchiveUtils.get14DigitDate(curi.getFetchBeginTime());
			if (fetchTime != null && !fetchTime.isEmpty()) {
				columnList.add(
						new Column(ByteBuffer.wrap(getCassandraParameters().getProcessedAtColumnName().getBytes(encoding)),
								ByteBuffer.wrap(serialize(fetchTime.getBytes(encoding))), timestamp));
			}

			// Write the Crawl Request to the Put object
			if (recordingOutputStream.getSize() > 0) {
				columnList.add(
						new Column(ByteBuffer.wrap(getCassandraParameters().getRequestColumnName().getBytes(encoding)),
								ByteBuffer.wrap(serialize(getByteArrayFromInputStream(recordingOutputStream.getReplayInputStream(),
										(int)recordingOutputStream.getSize()))), timestamp));
			}

			// Write the Crawl Response to the Put object
			ReplayInputStream replayInputStream = recordingInputStream.getReplayInputStream();
			try {
				// add the raw content to the table record
				columnList.add(
						new Column(ByteBuffer.wrap(getCassandraParameters().getContentColumnName().getBytes(encoding)),
								ByteBuffer.wrap(serialize(getByteArrayFromInputStream(replayInputStream,
										(int) recordingInputStream.getSize()))), timestamp));

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

			Map<ByteBuffer, Map<String, List<Mutation>>> job = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
			job.put(ByteBuffer.wrap(key.getBytes(encoding)), mutationsForColumnFamily);

			// Submitting the writes to the Cassandra client
			while (true) {
				try {
					this._connection.getClient().batch_mutate(job, ConsistencyLevel.ONE);
					break;
				} catch (Exception e) {
					IOException ex = new IOException("The following exception was encountered while " +
							"writing key '" + key + "':\n" + e.getMessage(), e);
					LOG.error(ex.getMessage());
					this._connection.close();
					try {
						this._connection.connect();
					} catch (TTransportException e1) {
						LOG.error(e1.getMessage());
					} catch (InvalidRequestException e1) {
						LOG.error(e1.getMessage());
					} catch (TException e1) {
						LOG.error(e1.getMessage());
					}
					Thread.sleep(5000);
				}
			}
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

	public byte[] serialize(byte[] bytes) {
		if (getCassandraParameters().getSerializer() != null)
			return getCassandraParameters().getSerializer().serialize(bytes);

		return bytes;
	}

	public static long currentMicroseconds() {
		return microseconds(System.currentTimeMillis());
	}

	public static long microseconds(long milliseconds) {
		return milliseconds * 1000; // convert milliseconds to microseconds
	}
}
