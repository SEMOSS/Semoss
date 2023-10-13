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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ExecuteScheduledJobReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ExecuteScheduledJobReactor.class);

	public ExecuteScheduledJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		if(Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
		
		organizeKeys();

		// Get inputs
		String jobId= this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		
		// the job group is the app the user is in
		// user must be an admin or editor of the app
		// to add a scheduled job
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
			throw new IllegalArgumentException("User does not have proper permissions to schedule jobs");
		}
		
		JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		try {
			if (scheduler.checkExists(jobKey)) {
				scheduler.triggerJob(jobKey);
			} else {
				throw new IllegalArgumentException("Could not find job with name = " + jobId+ " and group = " + jobGroup);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
