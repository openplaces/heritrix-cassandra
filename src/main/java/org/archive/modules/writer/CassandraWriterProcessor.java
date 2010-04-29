package org.archive.modules.writer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.io.cassandra.CassandraParameters;
import org.archive.io.cassandra.CassandraWriter;
import org.archive.io.cassandra.CassandraWriterPool;
import org.archive.io.cassandra.UrlKey;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.util.ArchiveUtils;

/**
 * A <a href="http://crawler.archive.org">Heritrix 3</a> processor that writes
 * to <a href="http://cassandra.apache.org/">Cassandra</a>.
 *
 * The following example shows how to configure the crawl job configuration.
 *
 * <pre>
 * {@code
 * <!-- DISPOSITION CHAIN -->
 * <bean id="cassandraParameters" class="org.archive.io.cassandra.CassandraParameters">
 *   <property name="keyspace" value="MyApplication" />
 *   <property name="crawlColumnFamily" value="crawled_pages" />
 * </bean>
 *
 * <bean id="cassandraWriterProcessor" class="org.archive.modules.writer.CassandraWriterProcessor">
 *   <property name="cassandraServers" value="localhost,127.0.0.1" />
 *   <property name="cassandraPort" value="9160" />
 *   <property name="cassandraParameters">
 *     <bean ref="cassandraParameters" />
 *   </property>
 * </bean>
 *
 * <bean id="dispositionProcessors" class="org.archive.modules.DispositionChain">
 *   <property name="processors">
 *     <list>
 *     <!-- write to aggregate archival files... -->
 *     <ref bean="cassandraWriterProcessor"/>
 *     <!-- other references -->
 *     </list>
 *   </property>
 * </bean>
 * }
 * </pre>
 *
 * @see org.archive.io.cassandra.CassandraParameters {@link org.archive.io.cassandra.CassandraParameters}
 *  for defining cassandraParameters
 *
 * @author greg
 */
public class CassandraWriterProcessor extends WriterPoolProcessor {

	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	private static final long serialVersionUID = 6207244931489760644L;

	private String cassandraServers;
	private int cassandraPort = 9160;

	/**
     * @see org.archive.io.cassandra.CassandraParameters
     */
    CassandraParameters cassandraParameters = null;

	/** If set to true, then only write urls that are new rowkey records.
     *  Default is false, which will write all urls to the HBase table.
     * Heritrix is good about not hitting the same url twice, so this feature
     * is to ensure that you can run multiple sessions of the same crawl
     * configuration and not write the same url more than once to the same
     * hbase table. You may just want to crawl a site to see what new urls have
     * been added over time, or continue where you left off on a terminated
     * crawl.  Heritrix itself does support this functionalty by supporting
     * "Checkpoints" during a crawl session, so this may not be a necessary
     * option.
     */
    private boolean onlyWriteNewRecords = false;

    /** If set to true, then only process urls that are new rowkey records.
     * Default is false, which will process all urls to the HBase table.
     * In this mode, Heritrix wont even fetch and parse the content served at
     * the url if it already exists as a rowkey in the HBase table.
     */
    private boolean onlyProcessNewRecords = false;


    public String getCassandraServers() {
    	return cassandraServers;
    }
    public void setCassandraServers(String cassandraServers) {
    	this.cassandraServers = cassandraServers;
    }

    public int getCassandraPort() {
    	return cassandraPort;
    }
    public void setCassandraPort(int cassandraPort) {
    	this.cassandraPort = cassandraPort;
    }

    public synchronized CassandraParameters getCassandraParameters() {
    	return cassandraParameters;
    }
    public void setCassandraParameters(CassandraParameters cassandraParameters) {
    	this.cassandraParameters = cassandraParameters;
    }

	public boolean onlyWriteNewRecords() {
        return onlyWriteNewRecords;
    }
    public void setOnlyWriteNewRecords(boolean onlyWriteNewRecords) {
        this.onlyWriteNewRecords = onlyWriteNewRecords;
    }

    public boolean onlyProcessNewRecords() {
        return onlyProcessNewRecords;
    }
    public void setOnlyProcessNewRecords(boolean onlyProcessNewRecords) {
        this.onlyProcessNewRecords = onlyProcessNewRecords;
    }


	@Override
	long getDefaultMaxFileSize() {
        return (20 * 1024 * 1024);
	}

	@Override
	List<String> getDefaultStorePaths() {
        return new ArrayList<String>();
	}

	@Override
	protected List<String> getMetadata() {
        return new ArrayList<String>();
	}

	@Override
	protected void setupPool(AtomicInteger serial) {
		setPool(new CassandraWriterPool(getCassandraServers(), getCassandraPort(), getCassandraParameters(), getPoolMaxActive(), getPoolMaxWaitMs()));
	}

	@Override
	protected ProcessResult innerProcessResult(CrawlURI uri) {
		CrawlURI curi = uri;
        long recordLength = getRecordedSize(curi);
        ReplayInputStream ris = null;
        try {
            if (shouldWrite(curi)) {
                ris = curi.getRecorder().getRecordedInput().getReplayInputStream();
                return write(curi, recordLength, ris);
            }
            LOG.info("Does not write " + curi.toString());
        } catch (IOException e) {
            curi.getNonFatalFailures().add(e);
            LOG.error("Failed write of Record: " + curi.toString(), e);
        } finally {
            ArchiveUtils.closeQuietly(ris);
        }
        return ProcessResult.PROCEED;
	}

	/**
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.ProcessorURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        // The old method is still checked, but only continue with the next
        // checks if it returns true.
        if (!super.shouldProcess(curi))
            return false;

        // If onlyProcessNewRecords is enabled and the given rowkey has cell data,
        // don't write the record.
        if (onlyProcessNewRecords()) {
            return isRecordNew(curi);
        }

        // If we make it here, then we passed all our checks and we can assume
        // we should write the record.
        return true;
    }

    /**
     * Whether the given CrawlURI should be written to archive files.
     * Annotates CrawlURI with a reason for any negative answer.
     *
     * @param curi CrawlURI
     *
     * @return true if URI should be written; false otherwise
     */
    protected boolean shouldWrite(CrawlURI curi) {
        // The old method is still checked, but only continue with the next
        // checks if it returns true.
        if (!super.shouldWrite(curi))
            return false;

        // If the content exceeds the maxContentSize, then dont write.
        if (curi.getContentSize() > getMaxFileSizeBytes()) {
            // content size is too large
            curi.getAnnotations().add(ANNOTATION_UNWRITTEN + ":size");
            LOG.warn("Content size for " + curi.getUURI() + " is too large ("
                    + curi.getContentSize() + ") - maximum content size is: "
                    + getMaxFileSizeBytes());
            return false;
        }

        // If onlyWriteNewRecords is enabled and the given rowkey has cell data,
        // don't write the record.
        if (onlyWriteNewRecords()) {
            return isRecordNew(curi);
        }

        // all tests pass, return true to write the content locally.
        return true;
    }

    /**
     * Determine if the given uri exists as a key in the configured Cassandra column family.
     *
     * @param curi the curi
     *
     * @return true if record doesn't currently exist
     */
    private boolean isRecordNew(CrawlURI curi) {
        WriterPoolMember writerPoolMember;
        try {
            writerPoolMember = getPool().borrowFile();
        } catch (IOException e1) {
            LOG.error("The following exception occurred while trying to access the writer pool ("
            			+ getPool().toString() + ")\n" + e1.getMessage());
            return false;
        }

        Cassandra.Client cassandraClient = ((CassandraWriter) writerPoolMember).getClient();

        // Generate the key for this uri
        String url = curi.toString();
        String key = UrlKey.createKey(url);

		// Check if that key exists within the column family.
    	int count = 0;
		try {
			count = cassandraClient.get_count(getCassandraParameters().getKeyspace(), key,
					new ColumnParent(getCassandraParameters().getCrawlColumnFamily()), ConsistencyLevel.ONE);
		} catch (Exception e) {
			LOG.error("An error occurred while trying to read from Cassandra - keyspace: " + getCassandraParameters().getKeyspace()
					+ " and key: " + key);
		}

		// If count was returned with a value larger than 0, then that means the key exists.
		if (count > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("The following is not a new record - Url: " + url + " has the existing key: "
                            + key + " with cell data.");
            }
            return false;
        }

        return true;
    }

    /**
     * Write to Cassandra.
     *
     * @param curi the curi
     * @param recordLength the record length
     * @param in the in
     *
     * @return the process result
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected ProcessResult write(final CrawlURI curi, long recordLength, InputStream in) throws IOException {
        WriterPoolMember writerPoolMember = getPool().borrowFile();
        long writerPoolMemberPosition = writerPoolMember.getPosition();
        CassandraWriter cassandraWriter = (CassandraWriter) writerPoolMember;
        try {
            cassandraWriter.write(curi, getHostAddress(curi), curi.getRecorder().getRecordedOutput(), curi.getRecorder().getRecordedInput());
        } finally {
            setTotalBytesWritten(getTotalBytesWritten() + (writerPoolMember.getPosition() - writerPoolMemberPosition));
            getPool().returnFile(writerPoolMember);
        }
        return checkBytesWritten();
    }
}
