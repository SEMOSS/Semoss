package prerna.rpa.config;

import com.google.gson.JsonObject;

public class JudgePiesJobConfig extends JobConfig {
	
	public JudgePiesJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	public void populateJobDataMap() {
		// Nothing to do here
	}
	
}
