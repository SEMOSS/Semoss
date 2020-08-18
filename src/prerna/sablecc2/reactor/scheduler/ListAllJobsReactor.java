package prerna.sablecc2.reactor.scheduler;

import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ListAllJobsReactor extends AbstractReactor {

	// inputs
	private static final String MY_JOBS = "myJobs";

	public ListAllJobsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.USERNAME.getKey(), MY_JOBS };
	}

	@Override
	public NounMetadata execute() {
		/**
		 * 4 POSSIBLE CASES
		 * ListAllJobs();
		 * ListAllJobs(app=["sample_app_id"]);
		 * ListAllJobs(username=["sample_username"]);
		 * ListAllJobs(app=["sample_app_id"], username=["sample_username"]);
		 * 
		 * This reactor will return all jobs based on app and user, if no parameters are
		 * passed it will check if user has admin permissions, if so it will return all
		 * jobs, if not it will throw error.
		 * 
		 */
		Map<String, Map<String, String>> jobMap = null;
		organizeKeys();

		String appId = this.keyValue.get(this.keysToGet[0]);
		String userId = this.keyValue.get(this.keysToGet[1]);

		if (appId == null && userId == null) {
			// TODO: check if admin if not admin throw permissions error
			// security utils. isAdmin() to check if user is admin *******
			// return all jobs
			jobMap = SchedulerH2DatabaseUtility.retrieveAllJobs();
		} else if (appId != null && userId == null) {
			jobMap = SchedulerH2DatabaseUtility.retrieveJobsForApp(appId);
		} else if (appId == null) {
			jobMap = SchedulerH2DatabaseUtility.retrieveUsersJobs(userId);
		} else {
			jobMap = SchedulerH2DatabaseUtility.retrieveUsersJobsForApp(appId, userId);
		}

		return new NounMetadata(jobMap, PixelDataType.MAP, PixelOperationType.LIST_JOB);
	}
}
