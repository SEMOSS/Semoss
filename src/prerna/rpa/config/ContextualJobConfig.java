package prerna.rpa.config;

import java.util.Map;

import com.google.gson.JsonObject;

public abstract class ContextualJobConfig extends JobConfig {

	public ContextualJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	// Use JobConfig's initialization method
	// But throw an illegal argument exception if the job is not contextual
	public static ContextualJobConfig initialize(JsonObject jobDefinition) {
		JobConfig jobConfig = JobConfig.initialize(jobDefinition);
		String jobClassName = jobDefinition.get(JobConfigKeys.JOB_CLASS_NAME).getAsString();
		if (!(jobConfig instanceof ContextualJobConfig)) {
			throw new IllegalArgumentException(jobClassName + " is not contextual.");
		} else {
			return (ContextualJobConfig) jobConfig;
		}
	}
	
	public abstract void accept(Map<String, Object> contextualData);
	
}
