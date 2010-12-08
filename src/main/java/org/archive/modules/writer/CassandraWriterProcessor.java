package org.archive.modules.writer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.archive.checkpointing.Checkpoint;
import org.archive.checkpointing.Checkpointable;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;
import org.archive.io.cassandra.CassandraParameters;
import org.archive.io.cassandra.CassandraWriter;
import org.archive.io.cassandra.CassandraWriterPool;
import org.archive.modules.CrawlMetadata;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.net.ServerCache;
import org.archive.spring.ConfigPath;
import org.archive.util.ArchiveUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.Lifecycle;

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
 *   <property name="seeds" value="localhost,127.0.0.1" />
     <property name="port" value="9160" />
 *   <property name="keyspace" value="MyApplication" />
 *   <property name="crawlColumnFamily" value="crawled_pages" />
 * </bean>
 *
 * <bean id="cassandraWriterProcessor" class="org.archive.modules.writer.CassandraWriterProcessor">
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
public class CassandraWriterProcessor extends WriterPoolProcessor implements Lifecycle, Checkpointable {

    private static final Logger logger = Logger.getLogger(WriterPoolProcessor.class.getName());
    private static final long serialVersionUID = 1L;

    /**
     * Whether to gzip-compress files when writing to disk; 
     * by default true, meaning do-compress. 
     */
    boolean compress = true; 
    public boolean getCompress() {
        return compress;
    }
    public void setCompress(boolean compress) {
        this.compress = compress;
    }
    
    /**
     * File prefix. The text supplied here will be used as a prefix naming
     * writer files. For example if the prefix is 'IAH', then file names will
     * look like IAH-20040808101010-0001-HOSTNAME.arc.gz ...if writing ARCs (The
     * prefix will be separated from the date by a hyphen).
     */
    String prefix = WriterPoolMember.DEFAULT_PREFIX; 
    public String getPrefix() {
        return prefix;
    }
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

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
    
    /**
     * Max size of each file.
     */
    long maxFileSizeBytes = getDefaultMaxFileSize();
    
    long getDefaultMaxFileSize() {
    	return (20 * 1024 * 1024);
    }
    
    public long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }
    public void setMaxFileSizeBytes(long maxFileSizeBytes) {
        this.maxFileSizeBytes = maxFileSizeBytes;
    }
    
    /**
     * Maximum active files in pool. This setting cannot be varied over the life
     * of a crawl.
     */
    int poolMaxActive = WriterPool.DEFAULT_MAX_ACTIVE;
    public int getPoolMaxActive() {
        return poolMaxActive;
    }
    public void setPoolMaxActive(int poolMaxActive) {
        this.poolMaxActive = poolMaxActive;
    }

    /**
     * Whether to skip the writing of a record when URI history information is
     * available and indicates the prior fetch had an identical content digest.
     * Note that subclass settings may provide more fine-grained control on
     * how identical digest content is handled; for those controls to have
     * effect, this setting must not be 'true' (causing content to be 
     * skipped entirely). 
     * Default is false.
     */
    boolean skipIdenticalDigests = false; 
    public boolean getSkipIdenticalDigests() {
        return skipIdenticalDigests;
    }
    public void setSkipIdenticalDigests(boolean skipIdenticalDigests) {
        this.skipIdenticalDigests = skipIdenticalDigests;
    }

    /**
     * CrawlURI annotation indicating no record was written.
     */
    protected static final String ANNOTATION_UNWRITTEN = "unwritten";

    /**
     * Total file bytes to write to disk. Once the size of all files on disk has
     * exceeded this limit, this processor will stop the crawler. A value of
     * zero means no upper limit.
     */
    long maxTotalBytesToWrite = 0L;
    public long getMaxTotalBytesToWrite() {
        return maxTotalBytesToWrite;
    }
    public void setMaxTotalBytesToWrite(long maxTotalBytesToWrite) {
        this.maxTotalBytesToWrite = maxTotalBytesToWrite;
    }

    public CrawlMetadata getMetadataProvider() {
        return (CrawlMetadata) kp.get("metadataProvider");
    }
    @Autowired
    public void setMetadataProvider(CrawlMetadata provider) {
        kp.put("metadataProvider",provider);
    }

    transient protected ServerCache serverCache;
    public ServerCache getServerCache() {
        return this.serverCache;
    }
    @Autowired
    public void setServerCache(ServerCache serverCache) {
        this.serverCache = serverCache;
    }

    protected ConfigPath directory = new ConfigPath("writer base path", ".");
    public ConfigPath getDirectory() {
        return directory;
    }
    public void setDirectory(ConfigPath directory) {
        this.directory = directory;
    }
    
    /**
     * Where to save files. Supply absolute or relative directory paths. 
     * If relative, paths will be interpreted relative to the local
     * 'directory' property. order.disk-path setting. If more than one
     * path specified, we'll round-robin dropping files to each. This 
     * setting is safe to change midcrawl (You can remove and add new 
     * dirs as the crawler progresses).
     */
    List<String> storePaths = getDefaultStorePaths();
	List<String> getDefaultStorePaths() {
        return new ArrayList<String>();
	}
    public List<String> getStorePaths() {
        return storePaths;
    }
    public void setStorePaths(List<String> paths) {
        this.storePaths = paths; 
    }
    
    /**
     * Reference to pool.
     */
    transient private WriterPool pool = null;
    
    /**
     * Total number of bytes written to disc.
     */
    private long totalBytesWritten = 0;

    private WriterPoolSettings settings;
    private AtomicInteger serial = new AtomicInteger();
    

    /**
     * @param name Name of this processor.
     * @param description Description for this processor.
     */
    public CassandraWriterProcessor() {
        super();
    }


    public synchronized void start() {
        if (isRunning()) {
            return;
        }
        super.start(); 
        this.settings = this;
        setupPool(serial);
    }
    
    public void stop() {
        if (!isRunning()) {
            return;
        }
        super.stop(); 
        this.pool.close();
        this.settings = null; 
    }
    
    
    protected AtomicInteger getSerialNo() {
        return ((WriterPool)getPool()).getSerialNo();
    }

    /**
     * Set up pool of files.
     */
    protected void setupPool(AtomicInteger serial) {
		setPool(new CassandraWriterPool(getCassandraParameters(), this, getPoolMaxActive(), getMaxWaitForIdleMs()));
	}

    
    protected ProcessResult checkBytesWritten() {
        long max = getMaxTotalBytesToWrite();
        if (max <= 0) {
            return ProcessResult.PROCEED;
        }
        if (max <= this.totalBytesWritten) {
            return ProcessResult.FINISH; // FIXME: Specify reason
//            controller.requestCrawlStop(CrawlStatus.FINISHED_WRITE_LIMIT);
        }
        return ProcessResult.PROCEED;
    }

    // TODO: add non-urgent checkpoint request, that waits for a good
    // moment (when (W)ARCs are already rolling over)? 
    public void doCheckpoint(Checkpoint checkpointInProgress) throws IOException {
        // close all ARCs on checkpoint
        this.pool.close();
        
        super.doCheckpoint(checkpointInProgress);
   
        // reopen post checkpoint
        setupPool(this.serial);
    }
    
    @Override
    protected JSONObject toCheckpointJson() throws JSONException {
        JSONObject json = super.toCheckpointJson();
        json.put("serialNumber", getSerialNo().get());
        return json;
    }
    
    @Override
    protected void fromCheckpointJson(JSONObject json) throws JSONException {
        super.fromCheckpointJson(json);
        serial.set(json.getInt("serialNumber"));
    }
    
    protected WriterPool getPool() {
        return pool;
    }

    protected void setPool(WriterPool pool) {
        this.pool = pool;
    }

    protected long getTotalBytesWritten() {
        return totalBytesWritten;
    }

    protected void setTotalBytesWritten(long totalBytesWritten) {
        this.totalBytesWritten = totalBytesWritten;
    }
	
    public List<String> getMetadata() {
        return new ArrayList<String>();
	}
    
    public List<File> getOutputDirs() {
        List<String> list = getStorePaths();
        ArrayList<File> results = new ArrayList<File>();
        for (String path: list) {
            File f = new File(
                    path.startsWith("/") ? null : getDirectory().getFile(), 
                    path);
            if (!f.exists()) {
                try {
                    f.mkdirs();
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
            results.add(f);
        }
        return results;        
    }
    
    
    protected WriterPoolSettings getWriterPoolSettings() {
        return settings;
    }

    @Override
    protected void innerProcess(CrawlURI puri) {
        throw new AssertionError();
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
            logger.info("Does not write " + curi.toString());
        } catch (IOException e) {
            curi.getNonFatalFailures().add(e);
            logger.error("Failed write of Record: " + curi.toString(), e);
        } catch (InterruptedException e) {
		} finally {
            ArchiveUtils.closeQuietly(ris);
        }
        return ProcessResult.PROCEED;
	}

    protected boolean shouldProcess(CrawlURI uri) {
        if (!(uri instanceof CrawlURI)) {
            return false;
        }
        
        CrawlURI curi = (CrawlURI)uri;
        // If failure, or we haven't fetched the resource yet, return
        if (curi.getFetchStatus() <= 0) {
            return false;
        }
        
        // If no recorded content at all, don't write record.
        long recordLength = curi.getContentSize();
        if (recordLength <= 0) {
            // getContentSize() should be > 0 if any material (even just
            // HTTP headers with zero-length body is available.
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
     * @throws InterruptedException 
     */
    protected ProcessResult write(final CrawlURI curi, long recordLength, InputStream in) throws IOException, InterruptedException {
        WriterPoolMember writerPoolMember = getPool().borrowFile();
        long writerPoolMemberPosition = writerPoolMember.getPosition();
        CassandraWriter cassandraWriter = (CassandraWriter) writerPoolMember;
        try {
            cassandraWriter.write(curi, getHostAddress(curi), curi.getRecorder().getRecordedOutput(),
            		curi.getRecorder().getRecordedInput());
        } finally {
            setTotalBytesWritten(getTotalBytesWritten() + (writerPoolMember.getPosition() - writerPoolMemberPosition));
            getPool().returnFile(writerPoolMember);
        }
        return checkBytesWritten();
    }
}
