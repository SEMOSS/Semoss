package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.mail.SendEmailJob;

public class SendEmailJobConfig extends JobConfig {

	private JsonObject jobDefinition;
	
	public SendEmailJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		String from = getString(jobDefinition, SendEmailJob.IN_FROM_KEY);
		jobDataMap.put(SendEmailJob.IN_FROM_KEY, from);
		String[] to = getString(jobDefinition, SendEmailJob.IN_TO_KEY).split(";");
		jobDataMap.put(SendEmailJob.IN_TO_KEY, to);
		String subject = getString(jobDefinition, SendEmailJob.IN_SUBJECT_KEY);
		jobDataMap.put(SendEmailJob.IN_SUBJECT_KEY, subject);
		String body = getString(jobDefinition, SendEmailJob.IN_BODY_KEY);
		jobDataMap.put(SendEmailJob.IN_BODY_KEY, body);
		boolean bodyIsHTML = getBoolean(jobDefinition, SendEmailJob.IN_BODY_IS_HTML_KEY);
		jobDataMap.put(SendEmailJob.IN_BODY_IS_HTML_KEY, bodyIsHTML);
		
		// The input for the email session will be added to the context by another job
		
		return jobDataMap;
	}

}
