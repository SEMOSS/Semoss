package prerna.rpa.config;

public class JobConfigKeys {
	
	private JobConfigKeys() {
		throw new IllegalStateException("Constants class");
	}
	
	// Job metadata
	// All jobs may have these keys
	// However, they are are not always required
	public static final String JOB_NAME = "-jobName";
	public static final String JOB_GROUP = "-jobGroup";
	public static final String JOB_CLASS_NAME = "-jobClass";
	public static final String JOB_CRON_EXPRESSION = "-jobCronExpression";

}
