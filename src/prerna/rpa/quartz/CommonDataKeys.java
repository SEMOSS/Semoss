package prerna.rpa.quartz;

/**
 * Class containing data map keys that are common to several jobs. Please
 * specify the intended data type of the key's value in each key's Javadoc.
 * There are two use cases for defining a key here, 1) if you commonly pass data
 * between jobs using these keys, or 2) if they are shared by several jobs in a
 * job chain.
 */
public class CommonDataKeys {

	private CommonDataKeys() {
		throw new IllegalStateException("Constants class");
	}
	
	/** {@code Map<String, BatchedJobInput>} - (identifier, ({@code JobDataMap}, {@code Class<? extends InterruptableJob>})) */
	public static final String BATCH_INPUT_MAP = "jobsInBatch";

	/** {@code Map<String, BatchedJobOutput>} - (identifier, ({@code JobDataMap}, success)) */
	public static final String BATCH_OUTPUT_MAP = "batchOutput";

	/** {@code List<Class<? extends InterruptableJob>>} - a sequence of job classes */
	public static final String CHAIN_SEQUENCE = "jobsInChain";

	/** {@code boolean} */
	public static final String BOOLEAN = "boolean";
	
	/** {@code String} */
	public static final String JDBC_DRIVER = "jdbcDriver";
	
	/** {@code String} */
	public static final String JDBC_CONNECTION_URL = "jdbcConnectionURL";
	
	/** {@code String} */
	public static final String JDBC_USERNAME = "jdbcUsername";
	
	/** {@code String} */
	public static final String JDBC_PASSWORD = "jdbcPassword";

	/** {@code javax.mail.Session} */
	public static final String EMAIL_SESSION = "emailSession";
	
	/** {@code String} */
	public static final String KICKOUT_PREFIX = "kickoutPrefix";

	/** {@code String} */
	public static final String JEDIS_HASH = "jedisKey";
	
	/** {@code Set<IHeadersDataRow>} */
	public static final String ROWS = "rows";
	
	/** {@code ITableDataFrame} */
	public static final String FRAME = "frame";
	
	/** {@code String} */
	public static final String INSIGHT_ID = "insightId";
	
}