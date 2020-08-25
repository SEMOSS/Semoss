package prerna.sablecc2.reactor.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.rpa.config.IllegalConfigException;
import prerna.rpa.config.ParseConfigException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class EditScheduledJobReactor extends ScheduleJobReactor {

	private static final Logger logger = LogManager.getLogger(EditScheduledJobReactor.class);

	// Inputs
	private static final String CURRENT_JOB_NAME = "curJobName";
	private static final String CURRENT_JOB_GROUP = "curJobGroup";

	public EditScheduledJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.CRON_EXPRESSION.getKey(), ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.RECIPE_PARAMETERS.getKey(), 
				TRIGGER_ON_LOAD, TRIGGER_NOW, PARAMETERS, CURRENT_JOB_NAME, CURRENT_JOB_GROUP};
	}

	@Override
	public NounMetadata execute() {
		Map<String, String> quartzJobMetadata = null;
		String userId = null;

		organizeKeys();

		// Get inputs
		String jobName = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		String cronExpression = this.keyValue.get(this.keysToGet[2]);
		SchedulerH2DatabaseUtility.validateInput(jobName, jobGroup, cronExpression);

		String recipe = this.keyValue.get(this.keysToGet[3]);
		recipe = SchedulerH2DatabaseUtility.validateAndDecodeRecipe(recipe);

		String recipeParameters = this.keyValue.get(this.keysToGet[4]);
		recipeParameters = SchedulerH2DatabaseUtility.validateAndDecodeRecipeParameters(recipeParameters);
		
		// get triggers
		boolean triggerOnLoad = getTriggerOnLoad();
		boolean triggerNow = getTriggerNow();

		String parameters = this.keyValue.get(this.keysToGet[7]);
		if(parameters == null) {
			parameters = "";
		}
		
		// existing name/group
		String curJobName = this.keyValue.get(this.keysToGet[8]);
		String curJobGroup = this.keyValue.get(this.keysToGet[9]);
		
		try {
			scheduler = SchedulerFactorySingleton.getInstance().getScheduler();

			// start up scheduler
			SchedulerH2DatabaseUtility.startScheduler(scheduler);

			// get user access information
			User user = this.insight.getUser();
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

			// create json object for later use
			JsonObject jsonObject = createJsonObject(jobName, jobGroup, cronExpression, recipe, triggerOnLoad, parameters, providerInfo.toString());

			JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
			JobKey oldJobKey = JobKey.jobKey(curJobName, curJobGroup);

			// if job exists throw error, job already exists
			if (!scheduler.checkExists(oldJobKey)) {
				logger.error("job " + Utility.cleanLogString(oldJobKey.toString()) + " could not be found to edit");
				throw new IllegalArgumentException("job " + Utility.cleanLogString(oldJobKey.toString()) + " could not be found to edit");
			}

			try {
				if(jobKey.equals(oldJobKey)) {
					// edit the current recipe
					SchedulerH2DatabaseUtility.updateJobRecipesTable(userId, jobName, jobGroup, cronExpression, recipe, recipeParameters, "Default", 
							triggerOnLoad, parameters, curJobName, curJobGroup);
					
					// update the trigger
					String triggerName = jobName.concat("Trigger");
					String triggerGroup = jobGroup.concat("TriggerGroup");
					TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroup);
					Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, triggerGroup)
							.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
					// reschedule job
					if (scheduler.checkExists(jobKey)) {
						scheduler.rescheduleJob(triggerKey, trigger);
					}
				} else {
					// make sure the new name you are using is also valid
					if (scheduler.checkExists(jobKey)) {
						logger.error("job " + Utility.cleanLogString(jobKey.toString()) + " already exists");
						throw new IllegalArgumentException("job " + Utility.cleanLogString(jobKey.toString()) + " already exists. Must edit to a unique job name");
					}
					
					// edit the current recipe
					SchedulerH2DatabaseUtility.updateJobRecipesTable(userId, jobName, jobGroup, cronExpression, recipe, recipeParameters, "Default", 
							triggerOnLoad, parameters, curJobName, curJobGroup);
					
					// delete the current job
					if (scheduler.checkExists(oldJobKey)) {
						scheduler.deleteJob(oldJobKey);
					}
					// add the new job
	 				scheduleJob(jsonObject);
				}
			} catch (ParseConfigException | IllegalConfigException | SchedulerException e) {
				throw new RuntimeException("Failed to schedule the job", e);
			}

			// do we trigger now?
			if (triggerNow) {
				triggerJobNow(jobKey);
			}
			
			// Pretty-print version of the json
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			String jsonConfig = gson.toJson(jsonObject);

			// Save metadata into a map and return
			quartzJobMetadata = new HashMap<>();
			quartzJobMetadata.put(JSON_CONFIG, jsonConfig);
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}

		return new NounMetadata(quartzJobMetadata, PixelDataType.MAP, PixelOperationType.SCHEDULE_JOB);
	}

}
