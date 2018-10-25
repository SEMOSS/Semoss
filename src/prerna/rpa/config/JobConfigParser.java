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
		boolean triggerOnLoad = jobConfig.getTriggerOnLoad();
		boolean status = jobConfig.getStatus();

		// If the job already exists, then return
		JobKey key = new JobKey(jobName, jobGroup);
		Scheduler scheduler = SchedulerUtil.getScheduler();
		if (scheduler.checkExists(key)) {
			LOGGER.info("The job " + jobName + " already exists.");
			return key;
		}
		
		// Get the job's data map
		JobDataMap jobDataMap;
		try {
			jobDataMap = jobConfig.getJobDataMap();
		} catch (ParseConfigException | IllegalConfigException e) {
			LOGGER.error("Failed to parse job data map for " + jobName + ".");
			throw e;
		}

		// schedule job if its in active status
		if (status) {
			// Schedule the job
			JobDetail job = newJob(jobClass).withIdentity(jobName, jobGroup).usingJobData(jobDataMap).build();
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName + "Trigger", jobName + "TriggerGroup")
					.withSchedule(CronScheduleBuilder.cronSchedule(jobCronExpression)).build();
			if (unitTest) {
				scheduler.addJob(job, true, true);
				scheduler.triggerJob(job.getKey());
			} else {
				scheduler.scheduleJob(job, trigger);
			}
			LOGGER.info("Scheduled " + jobName + " to run on the following schedule: " + jobCronExpression + ".");

			// if triggerOnLoad is true run
			if (triggerOnLoad) {
				scheduler.triggerJob(job.getKey());
			}
			// Return the job key
			return job.getKey();
		} else {
			return null;
		}
	}
}
