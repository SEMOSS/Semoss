package prerna.sablecc2.reactor.scheduler;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class UnscheduleJobReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(UnscheduleJobReactor.class);

	public UnscheduleJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		/**
		 * UnscheduleJobFromDB(jobName = ["sample_job_name"], jobGroup=["sample_job_group"]);
		 * 
		 * This reactor will unschedule the job in Quartz but keep the job stored in the database.
		 * The jobs that're unscheduled can be rescheduled in the future.
		 */

		organizeKeys();
		// Get inputs
		String jobName = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);

		try {
			String triggerName = jobName.concat("Trigger");
			String triggerGroup = jobGroup.concat("TriggerGroup");
			TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroup);
			JobKey jobKey = JobKey.jobKey(jobName, jobGroup);

			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerH2DatabaseUtility.startScheduler(scheduler);

			if (scheduler.checkExists(jobKey)) {
				scheduler.unscheduleJob(triggerKey);
				return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.UNSCHEDULE_JOB);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}

		return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.UNSCHEDULE_JOB);
	}
}
