package prerna.rpa.config;

import com.google.gson.JsonObject;

public class RunPixelJobConfig  extends JobConfig {
	
	public RunPixelJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	public void populateJobDataMap() throws ParseConfigException {
		putString(JobConfigKeys.PIXEL);
		putString(JobConfigKeys.USER_ACCESS);
	}

}