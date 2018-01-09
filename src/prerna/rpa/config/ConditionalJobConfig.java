package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.ConditionalJob;

public class ConditionalJobConfig extends JobConfig {
	
	public ConditionalJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	public void populateJobDataMap() {

		JsonObject conditionalJobDefinition = jobDefinition.get(ConfigUtil.getJSONKey(ConditionalJob.IN_JOB_DEFINITION_KEY)).getAsJsonObject();
		
		// Make sure that the conditional job is also contextual now
		// This will throw an illegal argument exception if not
		ContextualJobConfig.initialize(conditionalJobDefinition);
		jobDataMap.put(ConditionalJob.IN_JOB_DEFINITION_KEY, conditionalJobDefinition);
		
		// The input for the rows satisfying condition will be added to the context by another job
	}

}
