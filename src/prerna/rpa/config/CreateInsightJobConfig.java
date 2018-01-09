package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.CreateInsightJob;

public class CreateInsightJobConfig extends JobConfig {
	
	public CreateInsightJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	public void populateJobDataMap() throws ParseConfigException {
		putString(CreateInsightJob.IN_PIXEL_KEY);
	}

}
