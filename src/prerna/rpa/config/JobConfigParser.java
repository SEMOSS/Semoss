package prerna.rpa.config;

import static org.quartz.JobBuilder.newJob;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import prerna.rpa.quartz.QuartzUtility;
import prerna.rpa.quartz.SchedulerUtil;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JobConfigParser {

	private static final Logger LOGGER = LogManager.getLogger(JobConfigParser.class.getName());
	
	public static JobKey parse(String jsonFileName, boolean unitTest) throws Exception {

		// Read in the json file
		String jsonString = ConfigUtil.readStringFromJSONFile(jsonFileName);
		JsonParser parser = new JsonParser();
		JsonObject jobDefinition = parser.parse(jsonString).getAsJsonObject();

		// Get the job's properties
		Class<? extends Job> jobClass = JobConfig.getJobClass(jobDefinition);
		String jobName = JobConfig.getJobName(jobDefinition);
		String jobGroup = JobConfig.getJobGroup(jobDefinition);
		String jobCronExpression = JobConfig.getCronExpression(jobDefinition);

		// For testing
		if (unitTest) {
			jobCronExpression = QuartzUtility.composeCronForNowPlus(5);
		}

		// Get the job's data map
		JobConfig jobConfig = JobConfig.initialize(jobDefinition);
		JobDataMap jobDataMap;
		try {
			jobDataMap = jobConfig.getJobDataMap();
		} catch (Exception e) {
			LOGGER.error("Failed to parse job data map for " + jobName + ".");
			throw e;
		}

		// Schedule the job
		JobDetail job = newJob(jobClass).withIdentity(jobName, jobGroup).usingJobData(jobDataMap).build();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName + "Trigger", jobName + "TriggerGroup")
				.withSchedule(CronScheduleBuilder.cronSchedule(jobCronExpression)).build();
		SchedulerUtil.getScheduler().scheduleJob(job, trigger);
		LOGGER.info("Scheduled " + jobName + " to run on the following shedule: " + jobCronExpression + ".");
		
		// Return the job key
		return job.getKey();
	}
}
