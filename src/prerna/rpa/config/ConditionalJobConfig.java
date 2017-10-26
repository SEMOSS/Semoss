package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.ConditionalJob;

public class ConditionalJobConfig extends JobConfig {

	private JsonObject jobDefinition;
	
	public ConditionalJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();

		JsonObject conditionalJobDefinition = jobDefinition.get(ConfigUtil.getJSONKey(ConditionalJob.IN_JOB_DEFINITION_KEY)).getAsJsonObject();
		
		// Make sure that the conditional job is also contextual now
		// This will throw an illegal argument exception if not
		ContextualJobConfig.initialize(conditionalJobDefinition);
		jobDataMap.put(ConditionalJob.IN_JOB_DEFINITION_KEY, conditionalJobDefinition);
		
		// The input for the rows satisfying condition will be added to the context by another job

		return jobDataMap;
	}

}
