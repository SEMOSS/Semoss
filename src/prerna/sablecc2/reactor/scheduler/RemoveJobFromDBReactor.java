package prerna.sablecc2.reactor.scheduler;

import java.sql.Connection;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class RemoveJobFromDBReactor extends AbstractReactor {
	
	private static final Logger logger = LogManager.getLogger(RemoveJobFromDBReactor.class);

	public RemoveJobFromDBReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		/**
		 * RemoveJobFromDB(jobName = ["sample_job_name"], jobGroup=["sample_job_group"]);
		 * This reactor will delete the job in Quartz and the database.
		 */

		organizeKeys();
		// Get inputs
		String jobName = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);

		// delete job from quartz
		try {
			JobKey job = JobKey.jobKey(jobName, jobGroup);

			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerH2DatabaseUtility.startScheduler(scheduler);

			if (scheduler.checkExists(job)) {
				scheduler.deleteJob(job);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}

		// Delete record from SMSS_JOB_RECIPES table in H2
		Connection connection = SchedulerH2DatabaseUtility.connectToSchedulerH2();
		boolean recordExists = SchedulerH2DatabaseUtility.existsInJobRecipesTable(connection, jobName, jobGroup);
		if (recordExists) {
			SchedulerH2DatabaseUtility.removeFromJobRecipesTable(connection, jobName, jobGroup);
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.UNSCHEDULE_JOB);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.UNSCHEDULE_JOB);
	}
}
