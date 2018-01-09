package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.mail.GetSMTPSessionJob;

public class GetSMTPSessionJobConfig extends JobConfig {
	
	public GetSMTPSessionJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	protected void populateJobDataMap() throws ParseConfigException {	
		putString(GetSMTPSessionJob.IN_SMTP_HOST_KEY);
		putInt(GetSMTPSessionJob.IN_SMTP_PORT_KEY);
	}

}
