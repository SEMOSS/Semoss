package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.example.BakePieJob;

public class BakePieJobConfig extends JobConfig {
	
	public BakePieJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	protected void populateJobDataMap() {
		putLong(BakePieJob.IN_DURATION_KEY);
	}

}
