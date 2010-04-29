package org.archive.io.cassandra;

/**
 * Configures the values of the column family, super/sub columns used
 * for the crawl. Also contains a full set of default values.
 *
 * Meant to be configured within the Spring framework either inline
 * of CassandraWriterProcessor or as a named bean and referenced later on.
 *
 * <pre>
 * {@code
 * <bean id="cassandraParameters" class="org.archive.io.cassandra.CassandraParameters">
 *   <!-- The 'keyspace' parameter is the minimum required -->
 *   <property name="keyspace" value="MyApplication" />
 *   <!-- Changing the default column family from "crawl" to "crawl_table" -->
 *   <property name="crawlColumnFamily" value="crawl_table" />
 *   <!-- Overwrite more options here -->
 * </bean>
 * }
 * </pre>
 *
 * @see org.archive.modules.writer.CassandraWriterProcessor
 *  {@link org.archive.modules.writer.CassandraWriterProcessor} for a full example
 *
 * @author greglu
 */
public class CassandraParameters {

	/** DEFAULT OPTIONS **/

	// Defaults to writing to the "crawl" table
	public static final String CRAWL_COLUMN_FAMILY = "crawl";
	public static final String ENCODING_SCHEME = "UTF-8";

	// "content" column family and qualifiers
    public static final String CONTENT_SUPER_COLUMN = "content";
    public static final String CONTENT_SUB_COLUMN = "raw_data";

    // "curi" column family and qualifiers
    public static final String CURI_SUPER_COLUMN = "curi";
    public static final String IP_SUB_COLUMN = "ip";
    public static final String PATH_FROM_SEED_SUB_COLUMN = "path-from-seed";
    public static final String IS_SEED_SUB_COLUMN = "is-seed";
    public static final String VIA_SUB_COLUMN = "via";
    public static final String URL_SUB_COLUMN = "url";
    public static final String REQUEST_SUB_COLUMN = "request";


	/** ACTUAL OPTIONS INITIALIZED TO DEFAULTS **/
    private String keyspace = "";

	private String crawlColumnFamily = CRAWL_COLUMN_FAMILY;
    private String encodingScheme = ENCODING_SCHEME;

	private String contentSuperColumn = CONTENT_SUPER_COLUMN;
    private String contentSubColumn = CONTENT_SUB_COLUMN;

    private String curiSuperColumn = CURI_SUPER_COLUMN;
    private String ipSubColumn = IP_SUB_COLUMN;
    private String pathFromSeedSubColumn = PATH_FROM_SEED_SUB_COLUMN;
    private String isSeedSubColumn = IS_SEED_SUB_COLUMN;
    private String viaSubColumn = VIA_SUB_COLUMN;
    private String urlSubColumn = URL_SUB_COLUMN;
    private String requestSubColumn = REQUEST_SUB_COLUMN;


    public String getKeyspace() {
    	if (keyspace.isEmpty())
    		throw new RuntimeException("A keyspace was never set for this object. Define one before trying to access it.");

		return keyspace;
	}
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
    public String getCrawlColumnFamily() {
        return crawlColumnFamily;
    }
    public void setCrawlColumnFamily(String crawlColumnFamily) {
        this.crawlColumnFamily = crawlColumnFamily;
    }
    public String getEncodingScheme() {
		return encodingScheme;
	}
	public void setEncodingScheme(String encodingScheme) {
		this.encodingScheme = encodingScheme;
	}
	public String getContentSuperColumn() {
        return contentSuperColumn;
    }
    public void setContentSuperColumn(String contentSuperColumn) {
        this.contentSuperColumn = contentSuperColumn;
    }
    public String getContentSubColumn() {
        return contentSubColumn;
    }
    public void setContentSubColumn(String contentSubColumn) {
        this.contentSubColumn = contentSubColumn;
    }
    public String getCuriSuperColumn() {
        return curiSuperColumn;
    }
    public void setCuriSuperColumn(String curiSuperColumn) {
        this.curiSuperColumn = curiSuperColumn;
    }
    public String getIpSubColumn() {
        return ipSubColumn;
    }
    public void setIpSubColumn(String ipSubColumn) {
        this.ipSubColumn = ipSubColumn;
    }
    public String getPathFromSeedSubColumn() {
        return pathFromSeedSubColumn;
    }
    public void setPathFromSeedSubColumn(String pathFromSeedSubColumn) {
        this.pathFromSeedSubColumn = pathFromSeedSubColumn;
    }
    public String getIsSeedSubColumn() {
        return isSeedSubColumn;
    }
    public void setIsSeedSubColumn(String isSeedSubColumn) {
        this.isSeedSubColumn = isSeedSubColumn;
    }
    public String getViaSubColumn() {
        return viaSubColumn;
    }
    public void setViaSubColumn(String viaSubColumn) {
        this.viaSubColumn = viaSubColumn;
    }
    public String getUrlSubColumn() {
        return urlSubColumn;
    }
    public void setUrlSubColumn(String urlSubColumn) {
        this.urlSubColumn = urlSubColumn;
    }
    public String getRequestSubColumn() {
        return requestSubColumn;
    }
    public void setRequestSubColumn(String requestSubColumn) {
        this.requestSubColumn = requestSubColumn;
    }
}
