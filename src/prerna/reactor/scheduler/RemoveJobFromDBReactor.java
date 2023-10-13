package prerna.reactor.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class RemoveJobFromDBReactor extends AbstractReactor {
	
	private static final Logger logger = LogManager.getLogger(RemoveJobFromDBReactor.class);

	public RemoveJobFromDBReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if(Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
		
		/**
		 * RemoveJobFromDB(jobName = ["sample_job_name"], jobGroup=["sample_job_group"]);
		 * This reactor will delete the job in Quartz and the database.
		 */
		organizeKeys();
		// Get inputs
		String jobId = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		boolean deleteJob = false;
		
		// the job group is the app the user is in
		// user must be an admin or editor of the app
		// to add a scheduled job
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
			throw new IllegalArgumentException("User does not have proper permissions to schedule jobs");
		}
		
		// delete job from quartz
		try {
			JobKey job = JobKey.jobKey(jobId, jobGroup);
			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
			
			// start up scheduler
			SchedulerDatabaseUtility.startScheduler(scheduler);

			if (scheduler.checkExists(job)) {
				deleteJob = scheduler.deleteJob(job);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}

		// delete record from SMSS_JOB_RECIPES table in H2
		boolean recordExists = SchedulerDatabaseUtility.existsInJobRecipesTable(jobId, jobGroup);
		if (recordExists) {
			SchedulerDatabaseUtility.removeFromJobRecipesTable(jobId, jobGroup);
		}

		return new NounMetadata(deleteJob, PixelDataType.BOOLEAN, PixelOperationType.UNSCHEDULE_JOB);
	}
}
