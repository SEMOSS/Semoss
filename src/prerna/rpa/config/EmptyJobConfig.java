package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

/**
 * Use this class as a convenience if you need to include a job which gets all
 * its input from the other jobs via the context.
 */
public class EmptyJobConfig extends JobConfig {
	
	public EmptyJobConfig(JsonObject jobDefinition) {
		// Do nothing
	}

	@Override
	public JobDataMap getJobDataMap() throws Exception {
		return new JobDataMap();
	}
	
}
