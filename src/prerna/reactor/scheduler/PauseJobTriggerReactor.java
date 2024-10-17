package prerna.reactor.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class PauseJobTriggerReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(PauseJobTriggerReactor.class);

	public PauseJobTriggerReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.JOB_TAGS.getKey() };
		this.keyRequired = new int[] { 0, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		if (Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}

		/**
		 * PauseJobTrigger(jobId = ["sample_job_id"], jobGroup=["sample_job_group"]);
		 * 
		 * This reactor will pause the job in Quartz but keep the job stored in the
		 * database. The jobs that are paused can be resumed in the future.
		 * 
		 * PauseJobTrigger(jobId = ["sample_job_id"], jobGroup=["sample_job_group"],
		 * jobTags = ["sample_job_tag"]); This will pause the jobId passed in along with
		 * any jobs attributed to the tag passed in
		 * 
		 * PauseJobTrigger(jobTags = ["sample_job_tag"], jobGroup=["sample_job_group"]);
		 * This will pause all jobs attributed to the tag passed in
		 */

		organizeKeys();
		// Get inputs
		String jobId = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		List<String> jobTags = getJobTags();

		// the job group is the app the user is in
		// user must be an admin or editor of the app
		// to add a scheduled job
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
			throw new IllegalArgumentException("User does not have proper permissions to schedule jobs");
		}

		// Check that user provides at least one input jobId or jobTags
		if ((jobId == null || jobId.isEmpty()) && (jobTags == null || jobTags.isEmpty())) {
			throw new IllegalArgumentException("Must pass in jobId or jobTags");
		}

		boolean returnStatus = false;
		List<String> affectedJobs = new ArrayList<>();

		if (jobId != null) {
			returnStatus = pauseJob(jobId, jobGroup);
			if (returnStatus == false) {
				affectedJobs.add(jobId);
			}
		}

		// Check if jobTags were supplied, pause all jobs attributed to job tag if
		// passed in
		if (jobTags != null && !jobTags.isEmpty()) {
			Map<String, Map<String, String>> jobIdsForTags = SchedulerDatabaseUtility.retrieveAllJobs(jobTags);

			for (String outerJob : jobIdsForTags.keySet()) {
				String jId = jobIdsForTags.get(outerJob).get(ReactorKeysEnum.JOB_ID.getKey());
				if (jId.equals(jobId)) {
					continue;
				}
				String jGroup = jobIdsForTags.get(outerJob).get(ReactorKeysEnum.JOB_GROUP.getKey());
				returnStatus = pauseJob(jId, jGroup);
				if (returnStatus == false) {
					affectedJobs.add(jId);
				}
			}
		}
		return new NounMetadata(affectedJobs, PixelDataType.VECTOR);
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

	private boolean pauseJob(String jobId, String jobGroup) {
		try {
			String triggerName = jobId.concat("Trigger");
			String triggerGroup = jobGroup.concat("TriggerGroup");
			TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroup);
			JobKey jobKey = JobKey.jobKey(jobId, jobGroup);

			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			if (scheduler.checkExists(jobKey)) {
				scheduler.pauseTrigger(triggerKey);
				return false;
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
		return false;
	}
}
