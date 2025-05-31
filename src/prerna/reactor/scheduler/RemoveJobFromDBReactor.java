package prerna.reactor.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
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
	    // If scheduler is force disabled, abort execution
	    if (Utility.schedulerForceDisable()) {
	        throw new IllegalArgumentException("Scheduler is not enabled");
	    }

	    /**
	     * This reactor deletes jobs from both Quartz and the Job DB table.
	     * It verifies permissions before deletion.
	     */

	    organizeKeys();

	    // Fetch job IDs and groups
	    List<String> jobIdsList = getJobIds();
	    List<String> jobGroupList = getJobGroups();

	    // Ensure job IDs and groups are paired up
	    if (jobIdsList.size() != jobGroupList.size()) {
	        throw new IllegalArgumentException("Number of job Ids and job groups must match");
	    }

	    boolean allJobsDeleted = true;
	    User user = this.insight.getUser();

	    // Get the Scheduler instance and start only once
	    Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		SchedulerDatabaseUtility.startScheduler(scheduler);

	    // Iterate through each job ID/group pair
	    for (int i = 0; i < jobIdsList.size(); i++) {
	        String jobId = jobIdsList.get(i).trim();
	        String jobGroup = jobGroupList.get(i).trim();
	        boolean deleteJob = false;

	        // Permission check: must be admin or app editor
	        if (!SecurityAdminUtils.userIsAdmin(user)
	            && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
	            throw new IllegalArgumentException(
	                "User lacks permission to delete job: " + jobId
	            );
	        }

	        // Quartz job deletion
	        try {
	            JobKey job = JobKey.jobKey(jobId, jobGroup);
	            if (scheduler.checkExists(job)) {
	                deleteJob = scheduler.deleteJob(job);
	            }
	        } catch (SchedulerException se) {
	            logger.error(Constants.STACKTRACE, se);
	            allJobsDeleted = false;
	        }

	        // Remove from SMSS_JOB_RECIPES table if it exists
	        if (SchedulerDatabaseUtility.existsInJobRecipesTable(jobId, jobGroup)) {
	            SchedulerDatabaseUtility.removeFromJobRecipesTable(jobId, jobGroup);
	        }

	        // Update overall success indicator
	        allJobsDeleted &= deleteJob;
	    }

	    // Report whether ALL requested jobs were deleted (in both Quartz and DB)
	    return new NounMetadata(
	        allJobsDeleted,
	        PixelDataType.BOOLEAN,
	        PixelOperationType.UNSCHEDULE_JOB
	    );
	}
	
	/**
	 * Get inputs
	 * @return list of jobIds to remove
	 */
	public List<String> getJobIds() {
		List<String> engineIds = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				engineIds.add(grs.get(i).toString());
			}
			return engineIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			engineIds.add(this.curRow.get(i).toString());
		}
		return engineIds;
	}
	
	/**
	 * Get inputs
	 * @return list of job groups
	 */
	public List<String> getJobGroups() {
		List<String> engineIds = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				engineIds.add(grs.get(i).toString());
			}
			return engineIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			engineIds.add(this.curRow.get(i).toString());
		}
		return engineIds;
	}


}
