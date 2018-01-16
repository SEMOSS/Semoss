package prerna.rpa.config;

import static org.quartz.JobBuilder.newJob;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.rpa.quartz.SchedulerUtil;

public class JobConfigParser {

	private static final Logger LOGGER = LogManager.getLogger(JobConfigParser.class.getName());
	
	private JobConfigParser() {
		throw new IllegalStateException("All class members are static.");
	}
	
	public static JobKey parse(String jsonFileName, boolean unitTest) throws ParseConfigException, IllegalConfigException, SchedulerException {

		// Read in the json file
		String jsonString = ConfigUtil.readStringFromJSONFile(jsonFileName);
		JsonParser parser = new JsonParser();
		JsonObject jobDefinition = parser.parse(jsonString).getAsJsonObject();

		// Get the job's properties
		JobConfig jobConfig = JobConfig.initialize(jobDefinition);
		Class<? extends Job> jobClass = jobConfig.getJobClass();
		String jobName = jobConfig.getJobName();
		String jobGroup = jobConfig.getJobGroup();
		String jobCronExpression = jobConfig.getCronExpression();
		String triggerOnLoad = jobConfig.getTriggerOnLoad();

		// Get the job's data map
		JobDataMap jobDataMap;
		try {
			jobDataMap = jobConfig.getJobDataMap();
		} catch (ParseConfigException | IllegalConfigException e) {
			LOGGER.error("Failed to parse job data map for " + jobName + ".");
			throw e;
		}

		// Schedule the job
		JobDetail job = newJob(jobClass).withIdentity(jobName, jobGroup).usingJobData(jobDataMap).build();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName + "Trigger", jobName + "TriggerGroup")
				.withSchedule(CronScheduleBuilder.cronSchedule(jobCronExpression)).build();
		Scheduler scheduler = SchedulerUtil.getScheduler();
		if (unitTest) {
			scheduler.addJob(job, true, true);
			scheduler.triggerJob(job.getKey());
		} else {
			scheduler.scheduleJob(job, trigger);
		}
		LOGGER.info("Scheduled " + jobName + " to run on the following schedule: " + jobCronExpression + ".");
		
		// if triggerOnLoad is true run
		if (triggerOnLoad != null && triggerOnLoad.equalsIgnoreCase("true")){
			scheduler.triggerJob(job.getKey());
		}
		
		// Return the job key
		return job.getKey();
	}
}
