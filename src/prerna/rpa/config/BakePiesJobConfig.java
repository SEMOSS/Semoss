package prerna.rpa.config;

import org.quartz.JobDataMap;
import prerna.rpa.quartz.jobs.example.BakePiesJob;

import com.google.gson.JsonObject;

public class BakePiesJobConfig extends JobConfig {

	private JsonObject jobDefinition;
	
	public BakePiesJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}

	@Override
	public JobDataMap getJobDataMap() {
		JobDataMap jobDataMap = new JobDataMap();
		long duration = getLong(jobDefinition, BakePiesJob.IN_DURATION_KEY);
		jobDataMap.put(BakePiesJob.IN_DURATION_KEY, duration);
		return jobDataMap;
	}

}
