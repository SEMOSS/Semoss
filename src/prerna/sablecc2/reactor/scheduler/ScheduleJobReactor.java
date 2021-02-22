package prerna.sablecc2.reactor.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.rpa.config.ConfigurableJob;
import prerna.rpa.config.IllegalConfigException;
import prerna.rpa.config.JobConfig;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.config.ParseConfigException;
import prerna.rpa.quartz.jobs.insight.RunPixelJobFromDB;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class ScheduleJobReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ScheduleJobReactor.class);

	// Inputs
	public static final String TRIGGER_NOW = "triggerNow";
	public static final String TRIGGER_ON_LOAD = "triggerOnLoad";
	public static final String UI_STATE = "uiState";

	// Outputs
	public static final String JSON_CONFIG = "jsonConfig";

	protected Scheduler scheduler = null;

	public ScheduleJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.CRON_EXPRESSION.getKey(), ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.RECIPE_PARAMETERS.getKey(),
				TRIGGER_ON_LOAD, TRIGGER_NOW, UI_STATE, ReactorKeysEnum.JOB_TAGS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Map<String, String> quartzJobMetadata = null;
		String userId = null;

		organizeKeys();

		// Get inputs
        String jobId = UUID.randomUUID().toString();
		String jobName = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		String cronExpression = this.keyValue.get(this.keysToGet[2]);

		List<String> jobTags = getJobTags();

		SchedulerDatabaseUtility.validateInput(jobName, jobGroup, cronExpression);

		// the job group is the app the user is in
		// user must be an admin or editor of the app
		// to add a scheduled job
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user) && !SecurityAppUtils.userCanEditEngine(user, jobGroup)) {
			throw new IllegalArgumentException("User does not have proper permissions to schedule jobs");
		}
		
		String recipe = this.keyValue.get(this.keysToGet[3]);
		recipe = SchedulerDatabaseUtility.validateAndDecodeRecipe(recipe);
		recipe = recipe.trim();
		
		String recipeParameters = this.keyValue.get(this.keysToGet[4]);
		recipeParameters = SchedulerDatabaseUtility.validateAndDecodeRecipeParameters(recipeParameters);
		if(recipeParameters == null) {
			recipeParameters = "";
		} else {
			recipeParameters = recipeParameters.trim();
		}
		
		// get triggers
		boolean triggerOnLoad = getTriggerOnLoad();
		boolean triggerNow = getTriggerNow();

		String uiState = this.keyValue.get(this.keysToGet[7]);
		if(uiState == null) {
			uiState = "";
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

			// create json object for later use
			JsonObject jsonObject = createJsonObject(jobId, jobName, jobGroup, cronExpression, recipe, recipeParameters,
					triggerOnLoad, uiState, providerInfo.toString());

			JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
			// if job exists throw error, job already exists
			if (scheduler.checkExists(jobKey)) {
				logger.error("job " + Utility.cleanLogString(jobKey.toString()) + " already exists");
				throw new IllegalArgumentException("job " + Utility.cleanLogString(jobKey.toString()) + " already exists");
			}

			try {
				scheduleJob(jsonObject);
			} catch (ParseConfigException | IllegalConfigException | SchedulerException e) {
				throw new RuntimeException("Failed to schedule the job", e);
			}
			
			if (triggerNow) {
				triggerJobNow(jobKey);
			}

			// insert into SMOSS_JOB_RECIPES table
			logger.info("Saving JobId to database: "+jobId);
			SchedulerDatabaseUtility.insertIntoJobRecipesTable(userId, jobId, jobName, jobGroup, cronExpression, recipe, recipeParameters, "Default", triggerOnLoad, uiState, jobTags);

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

	////////////////// Helper Methods ////////////////////////

	protected void triggerJobNow(JobKey jobKey) {
		try {
			if (scheduler.checkExists(jobKey)) {
				scheduler.triggerJob(jobKey);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
	}

	protected JobKey scheduleJob(JsonObject jsonObject) throws ParseConfigException, IllegalConfigException, SchedulerException {
		// Get the job's properties
		JobConfig jobConfig = JobConfig.initialize(jsonObject);
		Class<? extends Job> jobClass = RunPixelJobFromDB.class;
		String jobId = jobConfig.getJobId();
		String jobName = jobConfig.getJobName();
		String jobGroup = jobConfig.getJobGroup();
		String cronExpression = jobConfig.getCronExpression();

		// Get the job's data map
		JobDataMap jobDataMap;
		try {
			jobDataMap = jobConfig.getJobDataMap();
		} catch (ParseConfigException | IllegalConfigException e) {
			logger.error("Failed to parse job data map for " + jobName + ".");
			throw e;
		}

		// Schedule the job
		JobDetail job = JobBuilder.newJob(jobClass).withIdentity(jobId, jobGroup).usingJobData(jobDataMap).storeDurably().build();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(jobId+ "Trigger", jobGroup + "TriggerGroup")
				.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)
						.inTimeZone(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()))).build();

		scheduler.scheduleJob(job, trigger);

		logger.info("Scheduled " + jobId+ " to run on the following schedule: " + cronExpression + ".");

		// Return the job key
		return job.getKey();
	}

	public static JsonObject createJsonObject(String jobId, String jobName, String jobGroup, String cronExpression, String recipe,
			String recipeParameters, boolean triggerOnLoad, String uiState, String providerInfo) {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty(JobConfigKeys.JOB_ID, jobId);
		jsonObject.addProperty(JobConfigKeys.JOB_NAME, jobName);
		jsonObject.addProperty(JobConfigKeys.JOB_GROUP, jobGroup);
		jsonObject.addProperty(JobConfigKeys.JOB_CRON_EXPRESSION, cronExpression);
		jsonObject.addProperty(JobConfigKeys.TRIGGER_ON_LOAD, triggerOnLoad);
		jsonObject.addProperty(JobConfigKeys.UI_STATE, uiState);
		jsonObject.addProperty(JobConfigKeys.USER_ACCESS, providerInfo);

		// need this for the job config
		jsonObject.addProperty(JobConfigKeys.JOB_CLASS_NAME, ConfigurableJob.RUN_PIXEL_JOB.getJobClassName());
		jsonObject.addProperty(JobConfigKeys.PIXEL, recipe);
		jsonObject.addProperty(JobConfigKeys.PIXEL_PARAMETERS, recipeParameters);
		return jsonObject;
	}

	protected boolean getTriggerOnLoad() {
		GenRowStruct boolGrs = this.store.getNoun(TRIGGER_ON_LOAD);
		if (boolGrs != null && !boolGrs.isEmpty()) {
			List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
			if(val != null && !val.isEmpty()) {
				return (boolean) val.get(0);
			}
		}

		return false;
	}

	protected boolean getTriggerNow() {
		GenRowStruct boolGrs = this.store.getNoun(TRIGGER_NOW);
		if (boolGrs != null && !boolGrs.isEmpty()) {
			List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
			if(val != null && !val.isEmpty()) {
				return (boolean) val.get(0);
			}
		}

		return false;
	}
	
	protected List<String> getJobTags() {
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
