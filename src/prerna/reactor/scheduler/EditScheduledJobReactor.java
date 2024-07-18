package prerna.reactor.scheduler;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class EditScheduledJobReactor extends ScheduleJobReactor {

	private static final Logger classLogger = LogManager.getLogger(EditScheduledJobReactor.class);

	// Inputs
	private static final String CURRENT_JOB_NAME = "curJobName";
	private static final String CURRENT_JOB_GROUP = "curJobGroup";

	public EditScheduledJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.CRON_EXPRESSION.getKey(), ReactorKeysEnum.CRON_TZ.getKey(), 
				ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.RECIPE_PARAMETERS.getKey(), 
				TRIGGER_ON_LOAD, TRIGGER_NOW, UI_STATE, CURRENT_JOB_NAME, CURRENT_JOB_GROUP, 
				ReactorKeysEnum.JOB_TAGS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		if(Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
		organizeKeys();

		String userId = null;
		// Get inputs
		String jobId = this.keyValue.get(ReactorKeysEnum.JOB_ID.getKey());
		String jobName = this.keyValue.get(ReactorKeysEnum.JOB_NAME.getKey());
		String jobGroup = this.keyValue.get(ReactorKeysEnum.JOB_GROUP.getKey());
		String cronExpression = this.keyValue.get(ReactorKeysEnum.CRON_EXPRESSION.getKey());
		TimeZone cronTimeZone = null;
		String cronTz = this.keyValue.get(ReactorKeysEnum.CRON_TZ.getKey());
		if(cronTz == null || (cronTz=cronTz.trim()).isEmpty()) {
			cronTz = Utility.getApplicationTimeZoneId();
		}
		try {
			cronTimeZone = TimeZone.getTimeZone(cronTz);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Invalid Time Zone = " + cronTz);
		}
		
		List<String> jobTags = getJobTags();

		SchedulerDatabaseUtility.validateInput(jobName, jobGroup, cronExpression);

		// the job group is the app the user is in
		// user must be an admin or editor of the app
		// to add a scheduled job
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
			throw new IllegalArgumentException("User does not have proper permissions to schedule jobs");
		}

		String recipe = this.keyValue.get(ReactorKeysEnum.RECIPE.getKey());
		recipe = SchedulerDatabaseUtility.validateAndDecodeRecipe(recipe);

		String recipeParameters = this.keyValue.get(ReactorKeysEnum.RECIPE_PARAMETERS.getKey());
		recipeParameters = SchedulerDatabaseUtility.validateAndDecodeRecipeParameters(recipeParameters);
		if(recipeParameters == null) {
			recipeParameters = "";
		}

		// get triggers
		boolean triggerOnLoad = getTriggerOnLoad();
		boolean triggerNow = getTriggerNow();

		String uiState = this.keyValue.get(UI_STATE);
		if(uiState == null) {
			throw new NullPointerException("UI State is null and needs to be passed");
		}

		// existing name/group
		String curJobName = this.keyValue.get(CURRENT_JOB_NAME);
		if(curJobName == null) {
			curJobName = jobName;
		}
		String curJobGroup = this.keyValue.get(CURRENT_JOB_GROUP);
		if(curJobGroup == null) {
			curJobGroup = jobGroup;
		}
		try {
			scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerDatabaseUtility.startScheduler(scheduler);

			// get user access information
			List<AuthProvider> authProviders = user.getLogins();
			StringBuilder providerInfo = new StringBuilder(); 
			for (int i = 0; i < authProviders.size(); i++) {
				AuthProvider authProvider = authProviders.get(i); 
				AccessToken token = user.getAccessToken(authProvider);
				// save user id for later insertion
				userId = token.getId();
				providerInfo.append(authProvider.name()).append(":").append(token.getId());
				if (i != authProviders.size() - 1) {
					providerInfo.append(",");
				}
			}

			
			// the id does not change
			// but technically the group does change at the moment
			JobKey jobKey = JobKey.jobKey(jobId, curJobGroup);
			// if job does not exist throw error
			if (!scheduler.checkExists(jobKey)) {
				classLogger.error("job " + Utility.cleanLogString(jobKey.toString()) + " could not be found to edit");
				throw new IllegalArgumentException("job " + Utility.cleanLogString(jobKey.toString()) + " could not be found to edit");
			}
			
			try {
				JobDetail currentJobDetail = scheduler.getJobDetail(jobKey);
				JobDataMap currentJobDataMap = currentJobDetail.getJobDataMap();
				currentJobDataMap.clear();
				JobDataMap newJobDataMap = getJobDataMap(jobId, jobName, jobGroup, 
						cronExpression, cronTimeZone, recipe, recipeParameters, 
						triggerOnLoad, uiState, providerInfo.toString());
				// add the new job data map into the job detail
				currentJobDataMap.putAll(newJobDataMap);
				// add back the updated job detail
				scheduler.addJob(currentJobDetail, true);
				
				// edit the current recipe
				SchedulerDatabaseUtility.updateJobRecipesTable(userId, jobId, 
						jobName, jobGroup, 
						cronExpression, cronTimeZone,
						recipe, recipeParameters, 
						"Default", triggerOnLoad, uiState, 
						curJobName, curJobGroup, jobTags);

				// update the trigger
				String triggerName = jobId.concat("Trigger");
				String triggerGroup = jobGroup.concat("TriggerGroup");
				TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroup);
				Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, triggerGroup)
						.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)
								.inTimeZone(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()))).build();
				// reschedule job
				if (scheduler.checkExists(jobKey)) {
					scheduler.rescheduleJob(triggerKey, trigger);
				}
			} catch (SchedulerException e) {
				throw new RuntimeException("Failed to schedule the job", e);
			}

			// do we trigger now?
			if (triggerNow) {
				triggerJobNow(jobKey);
			}

			Map<String, Object> retMap = createRetMap(jobId, jobName, jobGroup, 
					cronExpression, cronTimeZone, 
					recipe, recipeParameters,
					triggerOnLoad, uiState, providerInfo.toString());
						
			return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.SCHEDULE_JOB);
		} catch (SchedulerException se) {
			classLogger.error(Constants.STACKTRACE, se);
			throw new IllegalArgumentException("Unable to schedule the job. Error message = " + se.getMessage());
		}
	}

}
