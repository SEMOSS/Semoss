package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.RunPixelJobFromDB;

public class RunPixelJobConfig  extends JobConfig {
	
	public RunPixelJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	public void populateJobDataMap() throws ParseConfigException {
		putString(RunPixelJobFromDB.IN_PIXEL_KEY);
		putString(JobConfigKeys.USER_ACCESS);
	}

}