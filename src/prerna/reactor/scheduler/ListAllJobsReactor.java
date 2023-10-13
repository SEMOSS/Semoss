package prerna.reactor.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ListAllJobsReactor extends AbstractReactor {

	// inputs
	private static final String MY_JOBS = "myJobs";

	public ListAllJobsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.USERNAME.getKey(), MY_JOBS, 
				ReactorKeysEnum.JOB_TAGS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if(Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
		
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
		 */
		
		Map<String, Map<String, String>> jobMap = null;
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String userId = this.keyValue.get(this.keysToGet[1]);
		List<String> jobTags  = getJobTags();

		if (projectId == null && userId == null) {
			// TODO: check if admin if not admin throw permissions error
			// security utils. isAdmin() to check if user is admin *******
			// return all jobs
			jobMap = SchedulerDatabaseUtility.retrieveAllJobs(jobTags);
		} else if (projectId != null && userId == null) {
			jobMap = SchedulerDatabaseUtility.retrieveJobsForProject(projectId, jobTags);
		} else if (projectId == null) {
			jobMap = SchedulerDatabaseUtility.retrieveUsersJobs(userId, jobTags);
		} else {
			jobMap = SchedulerDatabaseUtility.retrieveUsersJobsForProject(projectId, userId, jobTags);
		}

		return new NounMetadata(jobMap, PixelDataType.MAP, PixelOperationType.LIST_JOB);
	}
	
	private List<String> getJobTags() {
		List<String> jobTags = null;
		GenRowStruct grs= this.store.getNoun(ReactorKeysEnum.JOB_TAGS.getKey());
		if(grs != null && !grs.isEmpty()) {
			jobTags = new ArrayList<>();
			int size = grs.size();
			for(int i = 0; i < size; i++) {
				jobTags.add( grs.get(i)+"" );
			}
		}
		return jobTags;
	}
}
