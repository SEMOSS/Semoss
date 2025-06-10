package prerna.reactor.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	    
	    // Map of jobIds of successful and failed jobs
	    Map<String, List<String>> jobDeletionResult = new HashMap<>();
	    jobDeletionResult.put("success", new ArrayList<>());
	    jobDeletionResult.put("failed", new ArrayList<>());
	    
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
	            deleteJob = false;
	        }

	        // Remove from SMSS_JOB_RECIPES table if it exists
	        if (SchedulerDatabaseUtility.existsInJobRecipesTable(jobId, jobGroup)) {
	            SchedulerDatabaseUtility.removeFromJobRecipesTable(jobId, jobGroup);
	        }
	        
	        // Add jobId in the map based on deletion outcome
	        if(deleteJob) {
	        	jobDeletionResult.get("success").add(jobId);
	        }else {
	        	jobDeletionResult.get("failed").add(jobId);
	        }
	    }

	    // Return a map containing jobIds of successfully and unsuccessfully deleted (in both Quartz and DB) jobs
	    return new NounMetadata(
	        jobDeletionResult,
	        PixelDataType.MAP,
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

	@Override
	public String getReactorDescription() {
	    return "This reactor deletes multiple jobs from both Quartz Scheduler and SMSS_JOB_RECIPES table, ensuring proper permission before deletion.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.JOB_ID.getKey())) {
	    	return "Job IDs to delete";
	    }else if(key.equals(ReactorKeysEnum.JOB_GROUP.getKey())) {
	    	return "Job Groups corresponding to each job ID";
	    }
	    return super.getDescriptionForKey(key);
	}

}
