package prerna.sablecc2.reactor.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class ResumeJobTriggerReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ResumeJobTriggerReactor.class);

	public ResumeJobTriggerReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if(Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
		
		/**
		 * RescheduleJobFromDB(jobName = ["sample_job_name"], jobGroup=["sample_job_group"]);
		 * 
		 * This reactor will reschedule existing unscheduled jobs in Quartz.
		 */

		organizeKeys();
		// Get inputs
		String jobId = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);

		// the job group is the app the user is in
		// user must be an admin or editor of the app
		// to add a scheduled job
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
			throw new IllegalArgumentException("User does not have proper permissions to schedule jobs");
		}
		
		// resume the job in quartz
		// later grab cron expression and add functionality to resume specific trigger under job
		try {
			JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
			String triggerName = jobId.concat("Trigger");
			String triggerGroup = jobGroup.concat("TriggerGroup");
			TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroup);

			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerDatabaseUtility.startScheduler(scheduler);

			// reschedule job
			if (scheduler.checkExists(jobKey)) {
				scheduler.resumeTrigger(triggerKey);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}

		// Save metadata into a map and return
		Map<String, String> quartzJobMetadata = new HashMap<>();
		quartzJobMetadata.put("jobId", jobId);
		quartzJobMetadata.put("jobGroup", jobGroup);

		return new NounMetadata(quartzJobMetadata, PixelDataType.MAP, PixelOperationType.RESCHEDULE_JOB);
	}
}
