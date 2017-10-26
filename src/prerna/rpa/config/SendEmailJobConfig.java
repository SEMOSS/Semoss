package prerna.rpa.config;

import java.util.Map;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.mail.SendEmailJob;

public class SendEmailJobConfig extends ContextualJobConfig {

	private JsonObject jobDefinition;
	private Map<String, Object> contextualData;
	
	public SendEmailJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
	// TODO add in logic to send to particular people based on the contextual data
	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		String from = getString(jobDefinition, SendEmailJob.IN_FROM_KEY);
		jobDataMap.put(SendEmailJob.IN_FROM_KEY, from);
		
		String[] to = getString(jobDefinition, SendEmailJob.IN_TO_KEY).split(";");
		jobDataMap.put(SendEmailJob.IN_TO_KEY, to);
		
		// For the subject and the body, replace with context (if given)
		String subject = getString(jobDefinition, SendEmailJob.IN_SUBJECT_KEY);
		subject = ConfigUtil.replaceWithContext(subject, contextualData);
		jobDataMap.put(SendEmailJob.IN_SUBJECT_KEY, subject);
		
		String body = getString(jobDefinition, SendEmailJob.IN_BODY_KEY);
		body = ConfigUtil.replaceWithContext(body, contextualData);
		jobDataMap.put(SendEmailJob.IN_BODY_KEY, body);
		
		boolean bodyIsHTML = getBoolean(jobDefinition, SendEmailJob.IN_BODY_IS_HTML_KEY);
		jobDataMap.put(SendEmailJob.IN_BODY_IS_HTML_KEY, bodyIsHTML);
		
		// The input for the email session will be added to the context by another job
		
		return jobDataMap;
	}

	@Override
	public void accept(Map<String, Object> contextualData) {
		this.contextualData = contextualData;
	}
	
}
