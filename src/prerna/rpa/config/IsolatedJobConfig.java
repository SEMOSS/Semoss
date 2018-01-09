package prerna.rpa.config;

import static org.quartz.JobBuilder.newJob;

import org.quartz.JobDetail;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.IsolatedJob;

public class IsolatedJobConfig extends JobConfig {

	public IsolatedJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	protected void populateJobDataMap() throws ParseConfigException, IllegalConfigException {
		
		// Create the job detail for the isolated job
		JsonObject isolatedJobDefinition = jobDefinition.get(ConfigUtil.getJSONKey(IsolatedJob.IN_ISOLATED_JOB_KEY)).getAsJsonObject();
		JobConfig isolatedJobConfig = JobConfig.initialize(isolatedJobDefinition);
		
		JobDetail isolatedJobDetail = newJob(isolatedJobConfig.getJobClass())
				.withIdentity(isolatedJobConfig.getJobName(), isolatedJobConfig.getJobGroup())
				.usingJobData(isolatedJobConfig.getJobDataMap())
				.build();
		jobDataMap.put(IsolatedJob.IN_ISOLATED_JOB_KEY, isolatedJobDetail);
	}

}
