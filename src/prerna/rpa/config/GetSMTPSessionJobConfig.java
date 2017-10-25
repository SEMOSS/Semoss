package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.mail.GetSMTPSessionJob;

public class GetSMTPSessionJobConfig extends JobConfig {

	private JsonObject jobDefinition;
	
	public GetSMTPSessionJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		String smtpHost = getString(jobDefinition, GetSMTPSessionJob.IN_SMTP_HOST_KEY);
		jobDataMap.put(GetSMTPSessionJob.IN_SMTP_HOST_KEY, smtpHost);
		int smtpPort = getInt(jobDefinition, GetSMTPSessionJob.IN_SMTP_PORT_KEY);
		jobDataMap.put(GetSMTPSessionJob.IN_SMTP_PORT_KEY, smtpPort);
		
		return jobDataMap;
	}

}
