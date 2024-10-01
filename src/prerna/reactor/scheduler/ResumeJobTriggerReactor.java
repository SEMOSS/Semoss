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
import org.quartz.TriggerKey;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.scheduler.SchedulerFactorySingleton;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ResumeJobTriggerReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ResumeJobTriggerReactor.class);

	public ResumeJobTriggerReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.JOB_TAGS.getKey() };
		this.keyRequired = new int[] { 0, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		if(Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
		
		/**
		 * RescheduleJobFromDB(jobName = ["sample_job_name"], jobGroup=["sample_job_group"]);
		 * 
		 * This reactor will reschedule existing unscheduled jobs in Quartz.
		 */

		organizeKeys();
		// Get inputs
		String jobId = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		List<String> jobTags = getJobTags();
		List<String> pausedJobIds = new ArrayList<>();

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
		
		// resume the job in quartz
		// later grab cron expression and add functionality to resume specific trigger under job
		
		Map<String, String> quartzJobMetadata = new HashMap<>();
		if(jobId != null && !jobId.isEmpty()) {
			String pausedJob = resumeJob(jobId, jobGroup);
			pausedJobIds.add(pausedJob);
			
			quartzJobMetadata.put("jobId", jobId);
			quartzJobMetadata.put("jobGroup", jobGroup);
		}
		
		if (jobTags != null && !jobTags.isEmpty()) {
			Map<String, Map<String, String>> jobIdsForTags = SchedulerDatabaseUtility.retrieveAllJobs(jobTags);

			for (String outerJob : jobIdsForTags.keySet()) {
				String jId = jobIdsForTags.get(outerJob).get(ReactorKeysEnum.JOB_ID.getKey());
				if (jId.equals(jobId)) {
					continue;
				}
				String jGroup = jobIdsForTags.get(outerJob).get(ReactorKeysEnum.JOB_GROUP.getKey());
				resumeJob(jId, jGroup);
				pausedJobIds.add(jId);
			}
			
			// Covers scenario where job id is not passed in but job tags is and it finds multiple jobs to resume
			// Also covers scenario where job id is sent in & job tags is also sent in, but no jobs are returned
			// that fall under the job tag, therefore only the job id passed in was resumed, and we do not want
			// to return a list in that scenario
			if(pausedJobIds.size() > 1 || jobId.isEmpty()) {
				return new NounMetadata(pausedJobIds, PixelDataType.VECTOR);
			}
		}
		
		return new NounMetadata(quartzJobMetadata, PixelDataType.MAP, PixelOperationType.RESCHEDULE_JOB);
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
	
	private String resumeJob(String jobId, String jobGroup){
		
		try {
			JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
			String triggerName = jobId.concat("Trigger");
			String triggerGroup = jobGroup.concat("TriggerGroup");
			TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroup);

			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerDatabaseUtility.startScheduler(scheduler);

			// reschedule job
			if (scheduler.checkExists(jobKey)) {
				scheduler.resumeTrigger(triggerKey);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
		
		return jobId;
	}
}