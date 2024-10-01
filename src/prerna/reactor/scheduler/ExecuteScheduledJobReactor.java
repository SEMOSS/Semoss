package prerna.reactor.scheduler;

import java.util.ArrayList;
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
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.scheduler.SchedulerFactorySingleton;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ExecuteScheduledJobReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ExecuteScheduledJobReactor.class);

	public ExecuteScheduledJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.JOB_TAGS.getKey() };
		this.keyRequired = new int[] { 0, 1, 0 };
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
		List<String> jobTags = getJobTags();
		
		// the job group is the app the user is in
		// user must be an admin or editor of the app
		// to add a scheduled job
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
			throw new IllegalArgumentException("User does not have proper permissions to schedule jobs");
		}
		
		if ((jobId == null || jobId.isEmpty()) && (jobTags == null || jobTags.isEmpty())) {
			throw new IllegalArgumentException("Must pass in jobId or jobTags");
		}
		
		if (jobId != null) {
			triggerJob(jobId, jobGroup);
		}
		
		if (jobTags != null && !jobTags.isEmpty()) {
			Map<String, Map<String, String>> jobIdsForTags = SchedulerDatabaseUtility.retrieveAllJobs(jobTags);

			for (String outerJob : jobIdsForTags.keySet()) {
				String jId = jobIdsForTags.get(outerJob).get(ReactorKeysEnum.JOB_ID.getKey());
				if (jId.equals(jobId)) {
					continue;
				}
				String jGroup = jobIdsForTags.get(outerJob).get(ReactorKeysEnum.JOB_GROUP.getKey());
				triggerJob(jId, jGroup);
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	private List<String> getJobTags() {
		List<String> jobTags = null;
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.JOB_TAGS.getKey());
		if (grs != null && !grs.isEmpty()) {
			jobTags = new ArrayList<>();
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				jobTags.add(grs.get(i) + "");
			}
		}
		return jobTags;
	}
	
	public void triggerJob(String jobId, String jobGroup) {
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
	}

}
