package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.RunPixelJob;

public class RunPixelJobConfig  extends JobConfig {
	
	public RunPixelJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	public void populateJobDataMap() throws ParseConfigException {
		putString(RunPixelJob.IN_PIXEL_KEY);
		putString(JobConfigKeys.USER_ACCESS);
	}

}