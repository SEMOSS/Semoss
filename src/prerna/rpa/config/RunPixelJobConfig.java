package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.RunPixelJob;

public class RunPixelJobConfig  extends JobConfig {

	private JsonObject jobDefinition;
	
	public RunPixelJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		String pixel = getString(jobDefinition, RunPixelJob.IN_PIXEL_KEY);
		jobDataMap.put(RunPixelJob.IN_PIXEL_KEY, pixel);
		return jobDataMap;
	}

}