package prerna.sablecc2.reactor.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class RescheduleExistingJobReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(RescheduleExistingJobReactor.class);

	public RescheduleExistingJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(), 
				ReactorKeysEnum.CRON_EXPRESSION.getKey() };
	}

	@Override
	public NounMetadata execute() {
		/**
		 * RescheduleJobFromDB(jobName = ["sample_job_name"], jobGroup=["sample_job_group"]);
		 * 
		 * This reactor will reschedule existing unscheduled jobs in Quartz.
		 */

		organizeKeys();
		// Get inputs
		String jobName = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		String cronExpression = this.keyValue.get(this.keysToGet[2]);
		SchedulerH2DatabaseUtility.validateInput(jobName, jobGroup, cronExpression);

		// resume the job in quartz
		// later grab cron expression and add functionality to resume specific trigger under job
		try {
			JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
			String triggerName = jobName.concat("Trigger");
			String triggerGroup = jobGroup.concat("TriggerGroup");
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, triggerGroup)
					.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
					.forJob(jobKey).build();

			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerH2DatabaseUtility.startScheduler(scheduler);

			// reschedule job
			if (scheduler.checkExists(jobKey)) {
				scheduler.scheduleJob(trigger);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}

		// Save metadata into a map and return
		Map<String, String> quartzJobMetadata = new HashMap<>();
		quartzJobMetadata.put("jobName", jobName);
		quartzJobMetadata.put("jobGroup", jobGroup);
		quartzJobMetadata.put("cronExpression", cronExpression);

		return new NounMetadata(quartzJobMetadata, PixelDataType.MAP, PixelOperationType.RESCHEDULE_JOB);
	}
}
