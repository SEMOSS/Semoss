package prerna.rpa.quartz;

/**
 * Class containing data map keys that are common to several jobs. Please
 * specify the intended data type of the key's value in each key's Javadoc.
 */
public class CommonDataKeys {

	/** {@code Map<String, BatchedJobInput>} - (identifier, (JobDataMap, Class<? extends InterruptableJob>)) */
	public static final String BATCH_INPUT_MAP = "jobsInBatch";

	/** {@code Map<String, BatchedJobOutput>} - (identifier, (JobDataMap, success)) */
	public static final String BATCH_OUTPUT_MAP = "batchOutput";

	/** {@code List<Class<? extends InterruptableJob>>} - a sequence of job classes */
	public static final String CHAIN_SEQUENCE = "jobsInChain";

	/** {@code boolean} */
	public static final String BOOLEAN = "boolean";

	/** {@code JobDetail} */
	public static final String IF_TRUE_JOB = "ifTrueJob";
	
	/** {@code String} */
	public static final String JDBC_DRIVER = "jdbcDriver";
	
	/** {@code String} */
	public static final String JDBC_CONNECTION_URL = "jdbcConnectionURL";
	
	/** {@code String} */
	public static final String JDBC_USERNAME = "jdbcUsername";
	
	/** {@code String} */
	public static final String JDBC_PASSWORD = "jdbcPassword";
	
	/** {@code String} */
	public static final String ENGINE_NAME = "engineName";
	
	/** {@code String} */
	public static final String INSIGHT_ID = "insightId";
	
	/** {@code javax.mail.Session} */
	public static final String EMAIL_SESSION = "emailSession";
	
}