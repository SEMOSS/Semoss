package prerna.sablecc2.reactor.scheduler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.rpa.RPAProps;
import prerna.rpa.config.ConfigUtil;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.config.ParseConfigException;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class RescheduleExistingJobReactor extends AbstractReactor {

	public RescheduleExistingJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				ReactorKeysEnum.CRON_EXPRESSION.getKey() };
	}

	@Override
	public NounMetadata execute() {
		/**
		 * RescheduleExistingJob(jobName = ["sample_job_name"],
		 * jobGroup=["sample_job_group"], (optional) cronExpression = [""]);
		 * 
		 * This reactor will reschedule a job that is listed as inactive.
		 * Optionally you can update the cron schedule when rescheduling.
		 * 
		 */

		organizeKeys();

		// Get inputs
		String jobName = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		// optional
		String cronExpression = this.keyValue.get(this.keysToGet[2]);

		String filePath = RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY) + jobName + "_" + jobGroup
				+ ".json";
		File file = new File(filePath);

		// read current json contents
		String jsonString;
		try {
			jsonString = ConfigUtil.readStringFromJSONFile(file.getName());
		} catch (ParseConfigException e) {
			throw new IllegalArgumentException("Job or Group doesnt exist!");
		}

		// update current json to active and update cron schedule
		JsonParser parser = new JsonParser();
		JsonObject jobDefinition = parser.parse(jsonString).getAsJsonObject();

		// cant reschedule an active job
		boolean status = jobDefinition.get(JobConfigKeys.ACTIVE).getAsBoolean();
		if (status) {
			throw new IllegalArgumentException("Job is already active! Must unschedule it first");
		}

		// continue to update json
		jobDefinition.addProperty(JobConfigKeys.ACTIVE, true);
		if (cronExpression != null) {
			jobDefinition.addProperty(JobConfigKeys.JOB_CRON_EXPRESSION, cronExpression);
		}

		// Pretty-print version of the json
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonConfig = gson.toJson(jobDefinition);

		// delete current json
		if (file.exists()) {
			file.delete();
		}

		// write new json to file, which will trigger the watcher to schedule it to Quartz
		try {
			FileUtils.writeStringToFile(file, jsonConfig, Charset.forName("UTF-8"));
		} catch (IOException e) {
			throw new RuntimeException("Failed to save job config update to " + file.toString(), e);
		}

		// Save metadata into a map and return
		Map<String, String> quartzJobMetadata = new HashMap<>();
		quartzJobMetadata.put("jobName", jobName);
		quartzJobMetadata.put("jobGroup", jobGroup);
		quartzJobMetadata.put("cronExpression", cronExpression);
		quartzJobMetadata.put("status", "active");

		return new NounMetadata(quartzJobMetadata, PixelDataType.MAP, PixelOperationType.RESCHEDULE_JOB);
	}
}
