package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

public class JudgePiesJobConfig extends JobConfig {
	
	public JudgePiesJobConfig(JsonObject jobDefinition) {
		// Nothing to do here, the input for this job must come from a JobBatch
	}

	@Override
	public JobDataMap getJobDataMap() {
		JobDataMap jobDataMap = new JobDataMap();
		return jobDataMap;
	}
	
}
