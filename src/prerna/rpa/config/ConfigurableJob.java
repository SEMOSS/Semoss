package prerna.rpa.config;

import java.util.HashMap;
import java.util.Map;

import org.quartz.InterruptableJob;
import prerna.rpa.quartz.JobBatch;
import prerna.rpa.quartz.JobChain;
import prerna.rpa.quartz.jobs.IfJob;
import prerna.rpa.quartz.jobs.example.BakePiesJob;
import prerna.rpa.quartz.jobs.example.JudgePiesJob;
import prerna.rpa.quartz.jobs.jdbc.ExecuteSQLJob;
import prerna.rpa.quartz.jobs.jdbc.maria.ETLJob;
import prerna.rpa.quartz.jobs.jdbc.maria.GenerateHashedPrimkeyJob;
import prerna.rpa.quartz.jobs.insight.InsightsRerunCronJob;
import prerna.rpa.quartz.jobs.insight.ConditionalJob;
import prerna.rpa.quartz.jobs.insight.OneColConditionJob;
import prerna.rpa.quartz.jobs.insight.RunPixelJob;
import prerna.rpa.quartz.jobs.insight.CreateInsightJob;
import prerna.rpa.quartz.jobs.insight.GetFrameFromInsightJob;
import prerna.rpa.quartz.jobs.mail.GetSMTPSessionJob;
import prerna.rpa.quartz.jobs.mail.SendEmailJob;

public enum ConfigurableJob {
	JOB_BATCH("JobBatch", JobBatch.class),
	JOB_CHAIN("JobChain", JobChain.class),
	IF_JOB("IfJob", IfJob.class),
	CONDITIONAL_JOB("ConditionalJob", ConditionalJob.class),
	ONE_COL_CONDITION_JOB("OneColConditionJob", OneColConditionJob.class),
	ETL_JOB("ETLJob", ETLJob.class),
	GENERATE_HASHED_PRIMKEY_JOB("GenerateHashedPrimkeyJob", GenerateHashedPrimkeyJob.class),
	EXECUTE_SQL_JOB("ExecuteSQLJob", ExecuteSQLJob.class),
	BAKE_PIES_JOB("BakePiesJob", BakePiesJob.class),
	JUDGE_PIES_JOB("JudgePiesJob", JudgePiesJob.class),
	CREATE_INSIGHT_JOB("CreateInsightJob", CreateInsightJob.class),
	GET_FRAME_FROM_INSIGHT_JOB("GetFrameFromInsightJob", GetFrameFromInsightJob.class),
	INSIGHT_RERUN_JOB("InsightsRerunCronJob", InsightsRerunCronJob.class),
	GET_SMTP_SESSION_JOB("GetSMTPSessionJob", GetSMTPSessionJob.class),
	SEND_EMAIL_JOB("SendEmailJob", SendEmailJob.class),
	RUN_PIXEL_JOB("RunPixelJob", RunPixelJob.class);
	
	private final String jobClassName;
	private final Class<? extends InterruptableJob> jobClass;
	
	private static Map<String, ConfigurableJob> jobClassNameToConfigurableJob = new HashMap<>();
	
	// Map the tag name to the ConfigurableJob
	static {
		for (ConfigurableJob configurableJob : ConfigurableJob.values()) {
			jobClassNameToConfigurableJob.put(configurableJob.getJobClassName(), configurableJob);
		}
	}
	
	ConfigurableJob(final String jobClassName, Class<? extends InterruptableJob> jobClass) {
		this.jobClassName = jobClassName;
		this.jobClass = jobClass;
	}
	
	public static ConfigurableJob getConfigurableJobFromJobClassName(String jobClassName) {
		return jobClassNameToConfigurableJob.get(jobClassName);
	}
	
	public String getJobClassName() {
		return jobClassName;
	}
	
	public Class<? extends InterruptableJob> getJobClass() {
		return jobClass;
	}
}
