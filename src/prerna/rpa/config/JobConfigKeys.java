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
	public static final String TRIGGER_ON_LOAD = "-jobTriggerOnLoad";
	public static final String ACTIVE = "-active";
	public static final String USER_ACCESS = "-userAccess";
	public static final String PARAMETERS = "parameters"; // Cannot have the dash due to FE

}
