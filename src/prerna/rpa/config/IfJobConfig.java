package prerna.rpa.config;

import static org.quartz.JobBuilder.newJob;

import org.quartz.JobDetail;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.IfJob;

public class IfJobConfig extends JobConfig {
	
	public IfJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	protected void populateJobDataMap() throws ParseConfigException, IllegalConfigException {
		
		// TODO docs: note which are purely from the context in javadocs for each input
		
		// Create the job detail for the if true job
		JsonObject ifTrueJobDefinition = jobDefinition.get(ConfigUtil.getJSONKey(IfJob.IN_IF_TRUE_JOB_KEY)).getAsJsonObject();
		JobConfig ifTrueJobConfig = JobConfig.initialize(ifTrueJobDefinition);
		
		JobDetail ifTrueJobDetail = newJob(ifTrueJobConfig.getJobClass())
				.withIdentity(ifTrueJobConfig.getJobName(), ifTrueJobConfig.getJobGroup())
				.usingJobData(ifTrueJobConfig.getJobDataMap())
				.build();
		jobDataMap.put(IfJob.IN_IF_TRUE_JOB_KEY, ifTrueJobDetail);
	}

}
