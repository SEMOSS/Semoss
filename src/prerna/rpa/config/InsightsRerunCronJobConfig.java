package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.InsightsRerunCronJob;

public class InsightsRerunCronJobConfig extends JobConfig {
	
	private JsonObject jobDefinition;
	
	public InsightsRerunCronJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}

	@Override
	public JobDataMap getJobDataMap() {
		JobDataMap jobDataMap = new JobDataMap();

		// Add the engines to the map
		String engines = jobDefinition.get(ConfigUtil.getJSONKey(InsightsRerunCronJob.ENGINES_KEY)).getAsString();
		jobDataMap.put(InsightsRerunCronJob.ENGINES_KEY, engines);
					
		return jobDataMap;
	}

}
