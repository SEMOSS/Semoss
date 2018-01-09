package prerna.rpa.config;

import com.google.gson.JsonObject;

/**
 * Use this class as a convenience if you need to include a job which gets all
 * its input from the other jobs via the context.
 */
public class EmptyJobConfig extends JobConfig {
	
	public EmptyJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	protected void populateJobDataMap() {
		// Do nothing
	}
	
}
