package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.CreateInsightJob;

public class CreateInsightJobConfig extends JobConfig {

	private JsonObject jobDefinition;
	
	public CreateInsightJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		String pixel = getString(jobDefinition, CreateInsightJob.IN_PIXEL_KEY);
		jobDataMap.put(CreateInsightJob.IN_PIXEL_KEY, pixel);
		return jobDataMap;
	}

}
