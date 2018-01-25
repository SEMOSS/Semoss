package prerna.sablecc2.reactor.scheduler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.rpa.RPAProps;
import prerna.rpa.config.ConfigUtil;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.config.ParseConfigException;
import prerna.rpa.quartz.SchedulerUtil;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class UnscheduleJobReactor extends AbstractReactor {

	// Inputs
	private static final String DELETE_FLAG = "delete";

	public UnscheduleJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(), DELETE_FLAG };
	}

	@Override
	public NounMetadata execute() {
		/**
		 * UnscheduleJob(jobName = ["sample_job_name"],
		 * jobGroup=["sample_job_group"], delete = ["true"]);
		 * 
		 * This reactor will unschedule the job in Quartz and can optionally
		 * delete the json config file from the rpa directory. If the json is
		 * not deleted then it can be re-scheduled in the future, otherwise a
		 * new job will need to be created.
		 */

		organizeKeys();

		// Get inputs
		String jobName = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		boolean deleteFlag = getDeleteFlag();

		String filePath = RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY) + jobName + "_" + jobGroup
				+ ".json";
		File file = new File(filePath);

		// delete job from quartz
		Scheduler scheduler;
		try {
			scheduler = SchedulerUtil.getScheduler();
			JobKey job = JobKey.jobKey(jobName, jobGroup);
			if (scheduler.checkExists(job)) {
				scheduler.deleteJob(job);
			} else {
				throw new IllegalArgumentException("Job doesnt exist");
			}
		} catch (SchedulerException e) {
			e.printStackTrace();
		}

		// delete json or update it to inactive
		if (deleteFlag) {
			// Delete the json file
			if (file.exists()) {
				file.delete();
			}
		} else {
			// update json file with inactive flag
			String jsonString;
			try {
				jsonString = ConfigUtil.readStringFromJSONFile(file.getName());
			} catch (ParseConfigException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException("Failed to update job config." + e);
			}

			// update current json to inactive or add it
			JsonParser parser = new JsonParser();
			JsonObject jobDefinition = parser.parse(jsonString).getAsJsonObject();
			jobDefinition.addProperty(JobConfigKeys.ACTIVE, false);

			// Pretty-print version of the json
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			String jsonConfig = gson.toJson(jobDefinition);

			// write new json back to file
			try {
				FileUtils.writeStringToFile(file, jsonConfig, Charset.forName("UTF-8"));
			} catch (IOException e) {
				throw new RuntimeException("Failed to save job config update to " + file.toString(), e);
			}
		}

		// Save metadata into a map and return
		Map<String, String> quartzJobMetadata = new HashMap<>();
		quartzJobMetadata.put(ReactorKeysEnum.JOB_NAME.getKey(), jobName);
		quartzJobMetadata.put(ReactorKeysEnum.JOB_GROUP.getKey(), jobGroup);
		quartzJobMetadata.put(DELETE_FLAG, deleteFlag + "");
		quartzJobMetadata.put("status", "inactive");

		return new NounMetadata(quartzJobMetadata, PixelDataType.MAP, PixelOperationType.UNSCHEDULE_JOB);
	}

	private boolean getDeleteFlag() {
		GenRowStruct boolGrs = this.store.getNoun(keysToGet[2]);
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(DELETE_FLAG)) {
			return "Delete the job permanently (true or false)";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
