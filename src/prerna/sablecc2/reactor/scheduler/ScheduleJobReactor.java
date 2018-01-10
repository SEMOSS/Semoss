package prerna.sablecc2.reactor.scheduler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.quartz.CronExpression;
import org.quartz.JobKey;
import org.quartz.SchedulerException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

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
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class ScheduleJobReactor extends AbstractReactor {

	// Inputs
	private static final String JOB_NAME = "jobName";
	private static final String JOB_GROUP = "jobGroup";
	private static final String CRON_EXPRESSION = "cronExpression";
	private static final String TRIGGER_NOW = "triggerNow";
	private static final String RECIPE = "recipe";
	
	// Outputs
	private static final String JSON_CONFIG = "jsonConfig";
	
	@Override
	public NounMetadata execute() {
		
		// Get inputs
		String jobName = getJobName();
		String jobGroup = getJobGroup();
		String cronExpression = getCronExpression();
		boolean triggerNow = getTriggerNow();
		String recipe = getRecipe();
		
		// Define the json; this is used to persist the job to disk
		// (Quartz is entirely in-memory)
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty(JobConfigKeys.JOB_NAME, jobName);
		jsonObject.addProperty(JobConfigKeys.JOB_GROUP, jobGroup);
		jsonObject.addProperty(JobConfigKeys.JOB_CRON_EXPRESSION, cronExpression);
		jsonObject.addProperty(JobConfigKeys.JOB_CLASS_NAME, ConfigurableJob.RUN_PIXEL_JOB.getJobClassName());
		jsonObject.addProperty(ConfigUtil.getJSONKey(RunPixelJob.IN_PIXEL_KEY), recipe);
		
		// Pretty-print version of the json
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonConfig = gson.toJson(jsonObject);
		
		// Save the json as a file
		String jsonFileName = jobName + "_" + jobGroup + ".json";
		String filePath = RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY) + jobName + "_" + jobGroup + ".json";
		try {
			FileUtils.writeStringToFile(new File(filePath), jsonConfig, Charset.forName("UTF-8"));
		} catch (IOException e) {
			throw new RuntimeException("Failed to save job config to " + filePath, e);
		}
		
		// Schedule the job
		try {
			JobKey jobKey = JobConfigParser.parse(jsonFileName, false);
			if (triggerNow) {
				SchedulerUtil.getScheduler().triggerJob(jobKey);
			}
		} catch (ParseConfigException | IllegalConfigException | SchedulerException e) {
			throw new RuntimeException(e.toString());
		}
		

		
		// Save metadata into a map and return
		Map<String, String> quartzJobMetadata = new HashMap<>();
		quartzJobMetadata.put(JOB_NAME, jobName);
		quartzJobMetadata.put(JOB_GROUP, jobGroup);
		quartzJobMetadata.put(CRON_EXPRESSION, cronExpression);
		quartzJobMetadata.put(RECIPE, recipe);
		quartzJobMetadata.put(JSON_CONFIG, jsonConfig);
		return new NounMetadata(quartzJobMetadata, PixelDataType.MAP, PixelOperationType.SCHEDULE_JOB);
	}

	private String getJobName() {
		GenRowStruct grs = this.store.getNoun(JOB_NAME);
		if (grs == null) throw new IllegalArgumentException("Need to define " + JOB_NAME);
		String input = grs.getNoun(0).getValue().toString();
		return input;
	}
	
	private String getJobGroup() {
		GenRowStruct grs = this.store.getNoun(JOB_GROUP);
		if (grs == null) return getJobName() + "Group"; 
		String input = grs.getNoun(0).getValue().toString();
		return input;
	}
	
	private String getCronExpression() {
		GenRowStruct grs = this.store.getNoun(CRON_EXPRESSION);
		if (grs == null) throw new IllegalArgumentException("Need to define " + CRON_EXPRESSION);
		String input = grs.getNoun(0).getValue().toString();
		boolean valid = CronExpression.isValidExpression(input);
		if (!valid) throw new IllegalArgumentException(input + " is not a valid CRON expression");
		return input;
	}
	
	private boolean getTriggerNow() {
		GenRowStruct grs = this.store.getNoun(TRIGGER_NOW);
		if (grs == null) return false;
		boolean input = Boolean.parseBoolean(grs.getNoun(0).getValue().toString());
		return input;
	}
	
	private String getRecipe() {
		GenRowStruct grs = this.store.getNoun(RECIPE);
		if (grs == null) throw new IllegalArgumentException("Need to define " + RECIPE);
		String input = grs.getNoun(0).getValue().toString();
		return input;
	}
	
}
