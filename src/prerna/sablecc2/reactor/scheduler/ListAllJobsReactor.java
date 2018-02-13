package prerna.sablecc2.reactor.scheduler;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.rpa.RPAProps;
import prerna.rpa.config.ConfigUtil;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.config.ParseConfigException;
import prerna.rpa.quartz.jobs.insight.RunPixelJob;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ListAllJobsReactor extends AbstractReactor {

	public ListAllJobsReactor() {
		// no inputs
	}

	@Override
	public NounMetadata execute() {
		/**
		 * ListAllJobs();
		 * 
		 * This reactor will return all active jobs based on the json config
		 * file. If the status is not defined in the config then it is assumed
		 * to be active.
		 * 
		 */

		Map master = new HashMap<String, Map<String, Object>>();


		// get all json files
		String jsonFilePath = RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY);
		File jsonDirectory = new File(jsonFilePath);
		List<File> jsonList = Arrays.asList(jsonDirectory.listFiles());

		for (File file : jsonList) {
			// read json and parse out status element
			String jsonString;
			String name = file.getName();
			try {
				jsonString = ConfigUtil.readStringFromJSONFile(name);
			} catch (ParseConfigException e) {
				throw new RuntimeException("Failed to update job config." + e);
			}

			// log it as active or inactive
			JsonParser parser = new JsonParser();
			JsonObject jobDefinition = parser.parse(jsonString).getAsJsonObject();
			boolean active = jobDefinition.get(JobConfigKeys.ACTIVE).getAsBoolean();
			String jobName = jobDefinition.get(JobConfigKeys.JOB_NAME).toString().replaceAll("\"", "");
			String group = jobDefinition.get(JobConfigKeys.JOB_GROUP).toString().replaceAll("\"", "");
			// ignore the file if hidden
			if (jobDefinition.get("hidden") != null) {
				boolean hidden = jobDefinition.get("hidden").getAsBoolean();
				if (hidden) {
					continue;
				}
			}
			String parameters = null;
			Object paramMap = null;
			if (jobDefinition.get(JobConfigKeys.PARAMETERS) != null) {
				// get as string
				parameters = jobDefinition.get(JobConfigKeys.PARAMETERS).toString();
				try {
					paramMap = new ObjectMapper().readValue(parameters, Object.class);
				} catch (IOException e2) {
					e2.printStackTrace();
				}
			}
			String cron = jobDefinition.get(JobConfigKeys.JOB_CRON_EXPRESSION).toString().replaceAll("\"", "");
			String recipe = null; 
			if (jobDefinition.get(ConfigUtil.getJSONKey(RunPixelJob.IN_PIXEL_KEY)) != null) {
				recipe = jobDefinition.get(ConfigUtil.getJSONKey(RunPixelJob.IN_PIXEL_KEY)).toString().replaceAll("\"","");
			}
			
			// display all
			Vector<HashMap<String, Object>> jobList = new Vector<HashMap<String, Object>>();
			if(master.containsKey(group)) {
				jobList = (Vector<HashMap<String, Object>> ) master.get(group);
			}
			HashMap<String, Object> jobMap = new HashMap<String, Object>();
			jobMap.put(ReactorKeysEnum.JOB_NAME.getKey(), jobName);
			jobMap.put(ReactorKeysEnum.CRON_EXPRESSION.getKey(), cron);
			jobMap.put(ReactorKeysEnum.RECIPE.getKey(), recipe);
			if (paramMap != null) {
				jobMap.put(JobConfigKeys.PARAMETERS, paramMap);
			}
			jobList.add(jobMap);
			master.put(group, jobList);
		}


		return new NounMetadata(master, PixelDataType.MAP, PixelOperationType.LIST_JOB);

	}
}
