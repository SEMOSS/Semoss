package prerna.sablecc2.reactor.scheduler;

import static org.quartz.JobBuilder.newJob;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
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
import prerna.rpa.RPAProps;
import prerna.rpa.config.ConfigUtil;
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
	private static final String TRIGGER_NOW = "triggerNow";
	private static final String TRIGGER_ON_LOAD = "triggerOnLoad";
	public static final String PARAMETERS = "parameters";

	// Outputs
	private static final String JSON_CONFIG = "jsonConfig";

	public Scheduler scheduler = null;

	public ScheduleJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.CRON_EXPRESSION.getKey(), ReactorKeysEnum.RECIPE.getKey(), TRIGGER_ON_LOAD, TRIGGER_NOW,
				PARAMETERS };
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

		// get triggers
		boolean triggerOnLoad = getTriggerOnLoad();
		boolean triggerNow = getTriggerNow();

		String parameters = this.keyValue.get(this.keysToGet[6]);
		if(parameters == null) {
			parameters = "";
		}
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

			// if job exists throw error, job already exists
			if (scheduler.checkExists(jobKey)) {
				logger.error("job" + Utility.cleanLogString(jobKey.toString()) + "already exists");
				throw new IllegalArgumentException("job " + Utility.cleanLogString(jobKey.toString()) + " already exists");
			}

			scheduleJob(jsonObject);

			if (triggerNow) {
				triggerJobNow(jobKey);
			}

			// insert into SMOSS_JOB_RECIPES table
			Connection connection = SchedulerH2DatabaseUtility.connectToSchedulerH2();
			SchedulerH2DatabaseUtility.insertIntoJobRecipesTable(connection, userId, jobName, jobGroup, cronExpression, recipe, "Default", triggerOnLoad, parameters);

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

	private void triggerJobNow(JobKey jobKey) {
		try {
			if (scheduler.checkExists(jobKey)) {
				scheduler.triggerJob(jobKey);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
	}

	public void scheduleJob(JsonObject jsonObject) {
		try {
			schedule(jsonObject);
		} catch (ParseConfigException | IllegalConfigException | SchedulerException e) {
			throw new RuntimeException("Failed to schedule the job", e);
		}
	}

	public JobKey schedule(JsonObject jsonObject) throws ParseConfigException, IllegalConfigException, SchedulerException {
		// Get the job's properties
		JobConfig jobConfig = JobConfig.initialize(jsonObject);
		Class<? extends Job> jobClass = RunPixelJobFromDB.class;
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
		JobDetail job = newJob(jobClass).withIdentity(jobName, jobGroup).usingJobData(jobDataMap).storeDurably().build();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName + "Trigger", jobGroup + "TriggerGroup")
				.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();

		scheduler.scheduleJob(job, trigger);

		logger.info("Scheduled " + jobName + " to run on the following schedule: " + cronExpression + ".");

		// Return the job key
		return job.getKey();
	}

	public static JsonObject createJsonObject(String jobName, String jobGroup, String cronExpression, String recipe,
			boolean triggerOnLoad, String parameters, String providerInfo) {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty(JobConfigKeys.JOB_NAME, jobName);
		jsonObject.addProperty(JobConfigKeys.JOB_GROUP, jobGroup);
		jsonObject.addProperty(JobConfigKeys.JOB_CRON_EXPRESSION, cronExpression);
		jsonObject.addProperty(JobConfigKeys.TRIGGER_ON_LOAD, triggerOnLoad);
		jsonObject.addProperty(JobConfigKeys.PARAMETERS, parameters);
		jsonObject.addProperty(JobConfigKeys.USER_ACCESS, RPAProps.getInstance().encrypt(providerInfo));

		// need this for the job config
		jsonObject.addProperty(JobConfigKeys.JOB_CLASS_NAME, ConfigurableJob.RUN_PIXEL_JOB.getJobClassName());
		jsonObject.addProperty(ConfigUtil.getJSONKey(RunPixelJobFromDB.IN_PIXEL_KEY), recipe);
		
		return jsonObject;
	}

	private boolean getTriggerOnLoad() {
		GenRowStruct boolGrs = this.store.getNoun(TRIGGER_ON_LOAD);
		if (boolGrs != null && !boolGrs.isEmpty()) {
			List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);

			return (boolean) val.get(0);
		}

		return false;
	}

	private boolean getTriggerNow() {
		GenRowStruct boolGrs = this.store.getNoun(TRIGGER_NOW);
		if (boolGrs != null && !boolGrs.isEmpty()) {
			List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);

			return (boolean) val.get(0);
		}

		return false;
	}
}
