package prerna.rpa.config;

import java.util.Map;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.mail.SendEmailJob;

public class SendEmailJobConfig extends ContextualJobConfig {

	private Map<String, Object> contextualData;
	
	public SendEmailJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	protected void populateJobDataMap() throws ParseConfigException {
		putString(SendEmailJob.IN_FROM_KEY);
		putStringArray(SendEmailJob.IN_TO_KEY);
		
		// For the subject and the body, replace with context (if given)
		String subject = getString(SendEmailJob.IN_SUBJECT_KEY);
		subject = ConfigUtil.replaceWithContext(subject, contextualData);
		jobDataMap.put(SendEmailJob.IN_SUBJECT_KEY, subject);
		
		String body = getString(SendEmailJob.IN_BODY_KEY);
		body = ConfigUtil.replaceWithContext(body, contextualData);
		jobDataMap.put(SendEmailJob.IN_BODY_KEY, body);
		
		putBoolean(SendEmailJob.IN_BODY_IS_HTML_KEY);
		
		// The input for the email session will be added to the context by another job
	}

	@Override
	public void accept(Map<String, Object> contextualData) {
		this.contextualData = contextualData;
	}
	
}
