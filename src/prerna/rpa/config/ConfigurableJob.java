package prerna.rpa.config;

import java.util.HashMap;
import java.util.Map;

import org.quartz.InterruptableJob;

import prerna.rpa.quartz.JobBatch;
import prerna.rpa.quartz.JobChain;
import prerna.rpa.quartz.jobs.IfJob;
import prerna.rpa.quartz.jobs.IsolatedJob;
import prerna.rpa.quartz.jobs.db.JedisToJDBCJob;
import prerna.rpa.quartz.jobs.db.jdbc.ETLJob;
import prerna.rpa.quartz.jobs.db.jdbc.ExecuteSQLJob;
import prerna.rpa.quartz.jobs.db.jdbc.maria.GenerateHashedPrimkeyJob;
import prerna.rpa.quartz.jobs.insight.ConditionalJob;
import prerna.rpa.quartz.jobs.insight.CreateInsightJob;
import prerna.rpa.quartz.jobs.insight.GetFrameFromInsightJob;
import prerna.rpa.quartz.jobs.insight.InsightsRerunCronJob;
import prerna.rpa.quartz.jobs.insight.OneColConditionJob;
import prerna.rpa.quartz.jobs.insight.RunPixelJobFromDB;
import prerna.rpa.quartz.jobs.mail.GetSMTPSessionJob;
import prerna.rpa.quartz.jobs.mail.SendEmailJob;
import prerna.rpa.quartz.jobs.reporting.kickout.RunKickoutAlgorithmJob;
import prerna.rpa.quartz.jobs.reporting.kickout.specific.anthem.ProcessWGSPReportsJob;

public enum ConfigurableJob {
	
	RUN_PIXEL_JOB("RunPixelJob", RunPixelJobFromDB.class),

	JOB_BATCH("JobBatch", JobBatch.class),
	JOB_CHAIN("JobChain", JobChain.class),
	IF_JOB("IfJob", IfJob.class),
	ISOLATED_JOB("IsolatedJob", IsolatedJob.class),
	ETL_JOB("ETLJob", ETLJob.class),
	GENERATE_HASHED_PRIMKEY_JOB("GenerateHashedPrimkeyJob", GenerateHashedPrimkeyJob.class),
	EXECUTE_SQL_JOB("ExecuteSQLJob", ExecuteSQLJob.class),
	CONDITIONAL_JOB("ConditionalJob", ConditionalJob.class),
	ONE_COL_CONDITION_JOB("OneColConditionJob", OneColConditionJob.class),
	CREATE_INSIGHT_JOB("CreateInsightJob", CreateInsightJob.class),
	GET_FRAME_FROM_INSIGHT_JOB("GetFrameFromInsightJob", GetFrameFromInsightJob.class),
	INSIGHT_RERUN_JOB("InsightsRerunCronJob", InsightsRerunCronJob.class),
	GET_SMTP_SESSION_JOB("GetSMTPSessionJob", GetSMTPSessionJob.class),
	SEND_EMAIL_JOB("SendEmailJob", SendEmailJob.class),
	JEDIS_TO_JDBC_JOB("JedisToJDBCJob", JedisToJDBCJob.class),
	PROCESS_WGSP_REPORTS_JOB("ProcessWGSPReportsJob", ProcessWGSPReportsJob.class),
	RUN_KICKOUT_ALGORITHM_JOB("RunKickoutAlgorithmJob", RunKickoutAlgorithmJob.class);
	
	
	private final String jobClassName;
	private final Class<? extends InterruptableJob> jobClass;
	
	private static final Map<String, ConfigurableJob> JOB_CLASS_NAME_TO_CONFIGURABLE_JOB = new HashMap<>();
	
	// Map the job class name to the ConfigurableJob
	static {
		for (ConfigurableJob configurableJob : ConfigurableJob.values()) {
			JOB_CLASS_NAME_TO_CONFIGURABLE_JOB.put(configurableJob.getJobClassName(), configurableJob);
		}
	}
	
	ConfigurableJob(final String jobClassName, Class<? extends InterruptableJob> jobClass) {
		this.jobClassName = jobClassName;
		this.jobClass = jobClass;
	}
	
	public static ConfigurableJob getConfigurableJobFromJobClassName(String jobClassName) {
		return JOB_CLASS_NAME_TO_CONFIGURABLE_JOB.get(jobClassName);
	}
	
	public String getJobClassName() {
		return jobClassName;
	}
	
	public Class<? extends InterruptableJob> getJobClass() {
		return jobClass;
	}
}
