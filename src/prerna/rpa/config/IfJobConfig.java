package prerna.rpa.config;

import static org.quartz.JobBuilder.newJob;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import prerna.rpa.quartz.jobs.IfJob;

import com.google.gson.JsonObject;

public class IfJobConfig extends JobConfig {

	private JsonObject jobDefinition;
	
	public IfJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		// The input for the boolean will be added to the context by another job
		
		// Create the job detail for the if true job
		JsonObject ifTrueJobDefinition = jobDefinition.get(ConfigUtil.getJSONKey(IfJob.IN_IF_TRUE_JOB_KEY)).getAsJsonObject();
		JobConfig ifTrueJobConfig = JobConfig.initialize(ifTrueJobDefinition);
		
		JobDetail ifTrueJobDetail = newJob(JobConfig.getJobClass(ifTrueJobDefinition))
				.withIdentity(JobConfig.getJobName(ifTrueJobDefinition), JobConfig.getJobGroup(ifTrueJobDefinition))
				.usingJobData(ifTrueJobConfig.getJobDataMap())
				.build();
		jobDataMap.put(IfJob.IN_IF_TRUE_JOB_KEY, ifTrueJobDetail);
		
		return jobDataMap;
	}

}
