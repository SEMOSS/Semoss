package prerna.reactor.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
	
	private static final Logger classLogger = LogManager.getLogger(RemoveJobFromDBReactor.class);

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
	    
	    if (jobIdsList.size() == 0) {
	        throw new IllegalArgumentException("Must pass in at least one jobId to remove");
	    }
	    // Ensure job IDs and groups are paired up
	    if (jobIdsList.size() != jobGroupList.size()) {
	        throw new IllegalArgumentException("Number of job Ids and job groups must match");
	    }
	    
	    // Map of jobIds of successful and failed jobs
	    Map<String, List<String>> jobDeletionResult = new HashMap<>();
	    jobDeletionResult.put("success", new ArrayList<>());
	    jobDeletionResult.put("failed", new ArrayList<>());
	    
	    // Return a map containing jobIds of successfully and unsuccessfully deleted (in both Quartz and DB) jobs
	    NounMetadata retNoun = new NounMetadata(jobDeletionResult, PixelDataType.MAP, PixelOperationType.UNSCHEDULE_JOB);
        Set<String> permissionErrorIds = new HashSet<>();
	    User user = this.insight.getUser();
	    
	    // Get the Scheduler instance and start only once
	    Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		SchedulerDatabaseUtility.startScheduler(scheduler);

	    // Iterate through each job ID/group pair
	    for (int i = 0; i < jobIdsList.size(); i++) {
	        String jobId = jobIdsList.get(i).trim();
	        String jobGroup = jobGroupList.get(i).trim();
	        boolean jobDeleted = false;

	        // Permission check: must be admin or app editor
	        if (!SecurityAdminUtils.userIsAdmin(user)
	            && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
	             jobDeletionResult.get("failed").add(jobId);
	             permissionErrorIds.add(jobId);
	             continue;
	        }

	        // Quartz job deletion
	        try {
	            JobKey job = JobKey.jobKey(jobId, jobGroup);
	            if (scheduler.checkExists(job)) {
	                jobDeleted = scheduler.deleteJob(job);
	            }
	        } catch (SchedulerException se) {
	            classLogger.error(Constants.STACKTRACE, se);
	            jobDeleted = false;
	        }

	        // Remove from SMSS_JOB_RECIPES table if it exists
	        if (SchedulerDatabaseUtility.existsInJobRecipesTable(jobId, jobGroup)) {
	            SchedulerDatabaseUtility.removeFromJobRecipesTable(jobId, jobGroup);
	        }
	        
	        // Add jobId in the map based on deletion outcome
	        if(jobDeleted) {
	        	jobDeletionResult.get("success").add(jobId);
	        } else {
	        	jobDeletionResult.get("failed").add(jobId);
	        }
	    }
	    
	    // Add any permission errors so user can investigate
	    for(String error : permissionErrorIds) {
	        retNoun.addAdditionalReturn(NounMetadata.getWarningNounMessage("User does not have the necesssary permission to remove jobs from group " + error));
	    }

	    return retNoun;
	}
	
	/**
	 * Get inputs
	 * @return list of jobIds to remove
	 */
	public List<String> getJobIds() {
		List<String> jobIds = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				jobIds.add(grs.get(i).toString());
			}
			return jobIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			jobIds.add(this.curRow.get(i).toString());
		}
		return jobIds;
	}
	
	/**
	 * Get inputs
	 * @return list of job groups
	 */
	public List<String> getJobGroups() {
		List<String> jobGroups = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				jobGroups.add(grs.get(i).toString());
			}
			return jobGroups;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			jobGroups.add(this.curRow.get(i).toString());
		}
		return jobGroups;
	}

	@Override
	public String getReactorDescription() {
	    return "This reactor deletes multiple jobs from both Quartz Scheduler and SMSS_JOB_RECIPES table, ensuring proper permission before deletion.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.JOB_ID.getKey())) {
	    	return "Job IDs to delete. This list should have a corresponding entry in " + ReactorKeysEnum.JOB_GROUP.getKey();
	    } else if(key.equals(ReactorKeysEnum.JOB_GROUP.getKey())) {
	    	return "Job Groups where each entry matches the corresponding index in " + ReactorKeysEnum.JOB_ID.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}
