package prerna.sablecc2.reactor.scheduler;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.quartz.CronExpression;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

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
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.config.JobConfigParser;
import prerna.rpa.config.ParseConfigException;
import prerna.rpa.quartz.SchedulerUtil;
import prerna.rpa.quartz.jobs.insight.RunPixelJob;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ScheduleJobReactor extends AbstractReactor {

	// Inputs
	private static final String TRIGGER_NOW = "triggerNow";
	private static final String TRIGGER_ON_LOAD = "triggerOnLoad";
	private static final String PARAMETERS = "parameters";
	
	// Outputs
	private static final String JSON_CONFIG = "jsonConfig";
	
	public ScheduleJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.CRON_EXPRESSION.getKey(), ReactorKeysEnum.RECIPE.getKey(), TRIGGER_ON_LOAD,
				TRIGGER_NOW, PARAMETERS};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// Get inputs
		String jobName = this.keyValue.get(this.keysToGet[0]);
		if(jobName == null || jobName.length() <= 0) {
			throw new IllegalArgumentException("Must provide job name");
		}
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		if (jobGroup == null || jobGroup.length() <= 0){
			throw new IllegalArgumentException("Must provide job group");
		}
		String cronExpression = this.keyValue.get(this.keysToGet[2]);
		// validate cron expression	
		if (!CronExpression.isValidExpression(cronExpression)){
			throw new IllegalArgumentException("Must provide a valid cron expression!");
		}
		String recipe = this.keyValue.get(this.keysToGet[3]);
		if(recipe == null || recipe.length() <= 0) {
			throw new IllegalArgumentException("Must provide a recipe");
		}
		
		try {
			recipe = URLDecoder.decode(recipe,"UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new IllegalArgumentException("Must be able to decode recipe");
		}
		
		boolean triggerOnLoad = getTriggerOnLoad();
		boolean triggerNow = getTriggerNow();
		String parameters = this.keyValue.get(this.keysToGet[6]);
		
		User user = this.insight.getUser();
		List<AuthProvider> authProviders = user.getLogins();
		StringBuilder providerInfo = new StringBuilder(); 
		for (int i = 0; i < authProviders.size(); i++) {
			AuthProvider authProvider = authProviders.get(i); 
			AccessToken token = user.getAccessToken(authProvider);
			providerInfo.append(authProvider.name()).append(":").append(token.getId());
			if (i != authProviders.size() - 1) {
				providerInfo.append(",");
			}
		}
		
		// Define the json; this is used to persist the job to disk
		// (Quartz is entirely in-memory)
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty(JobConfigKeys.JOB_NAME, jobName);
		jsonObject.addProperty(JobConfigKeys.JOB_GROUP, jobGroup);
		jsonObject.addProperty(JobConfigKeys.JOB_CRON_EXPRESSION, cronExpression);
		jsonObject.addProperty(JobConfigKeys.JOB_CLASS_NAME, ConfigurableJob.RUN_PIXEL_JOB.getJobClassName());
		jsonObject.addProperty(ConfigUtil.getJSONKey(RunPixelJob.IN_PIXEL_KEY), recipe);
		jsonObject.addProperty(JobConfigKeys.TRIGGER_ON_LOAD, triggerOnLoad);
		jsonObject.addProperty(JobConfigKeys.PARAMETERS, parameters);
		jsonObject.addProperty(JobConfigKeys.ACTIVE, true);
		jsonObject.addProperty(JobConfigKeys.USER_ACCESS, RPAProps.getInstance().encrypt(providerInfo.toString()));
		
		// Json file to persist job data
		String jsonFileName = jobGroup + "__" + jobName + ".json";
		String filePath = RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY) + jsonFileName;
		File jsonFile = new File(filePath);
		
		// throw error if job exists
		try {
			Scheduler scheduler = SchedulerUtil.getScheduler();
			JobKey job = JobKey.jobKey(jobName, jobGroup);
			if (scheduler.checkExists(job)) {
				throw new IllegalArgumentException ("Job already exists!");
			}
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
		
		// Pretty-print version of the json
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonConfig = gson.toJson(jsonObject);
		try {
			FileUtils.writeStringToFile(jsonFile, jsonConfig, Charset.forName("UTF-8"));
		} catch (IOException e) {
			throw new RuntimeException("Failed to save job config to " + filePath, e);
		}
		
		// Schedule the job
		try {
			JobConfigParser.parse(jsonFile.getName(), false);
		} catch (ParseConfigException | IllegalConfigException | SchedulerException e) {
			throw new RuntimeException("Failed to schedule the job", e);
		}
		
		// Trigger the job if needed
		JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
		if (triggerNow) {
			try {
				boolean exists = false;
				while(!exists) {
					exists = SchedulerUtil.getScheduler().checkExists(jobKey);
				}
				SchedulerUtil.getScheduler().triggerJob(jobKey);
			} catch (SchedulerException e) {
				e.printStackTrace();
			}
		}
		
		// Save metadata into a map and return
		Map<String, String> quartzJobMetadata = new HashMap<>();
		quartzJobMetadata.put(JSON_CONFIG, jsonConfig);
		return new NounMetadata(quartzJobMetadata, PixelDataType.MAP, PixelOperationType.SCHEDULE_JOB);
	}

	private boolean getTriggerOnLoad() {
		GenRowStruct boolGrs = this.store.getNoun(TRIGGER_ON_LOAD);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}

	
	private boolean getTriggerNow() {
		GenRowStruct boolGrs = this.store.getNoun(TRIGGER_NOW);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TRIGGER_NOW)) {
			return "Schedule the job immediately and then use cronExpression";
		} else if (key.equals(TRIGGER_ON_LOAD)) {
			return "Schedule the job when server starts";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
	
}
