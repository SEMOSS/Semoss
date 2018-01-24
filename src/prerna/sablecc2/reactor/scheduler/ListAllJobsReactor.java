package prerna.sablecc2.reactor.scheduler;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.rpa.RPAProps;
import prerna.rpa.config.ConfigUtil;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.config.ParseConfigException;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
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
			JsonElement status = jobDefinition.get(JobConfigKeys.JOB_STATUS);
			String jobName = jobDefinition.get(JobConfigKeys.JOB_NAME).toString().replaceAll("\"", "");
			String group = jobDefinition.get(JobConfigKeys.JOB_GROUP).toString().replaceAll("\"", "");

			// group by active/inactive
			// if (status != null && status.toString().equals("\"inactive\"")) {
			// ArrayList<String> jobList = new ArrayList<>();
			// if(activeGroup.containsKey(group)) {
			// jobList = (ArrayList<String>) activeGroup.get(group);
			// }
			// jobList.add(job_name);
			// activeGroup.put(group, jobList);
			// } else {
			// ArrayList<String> jobList = new ArrayList<>();
			// if(activeGroup.containsKey(group)) {
			// jobList = (ArrayList<String>) activeGroup.get(group);
			// }
			// jobList.add(job_name);
			// activeGroup.put(group, jobList);
			// }
			
			// display all
			HashMap<String, Object> jobMap = new HashMap<String, Object>();
			if(master.containsKey(group)) {
				jobMap = (HashMap<String, Object>) master.get(group);
			}
			if (status != null && status.toString().equals("\"inactive\"")) {
				jobMap.put(jobName, false);
			} else {
				jobMap.put(jobName, true);
			}
			master.put(group, jobMap);
		}


		return new NounMetadata(master, PixelDataType.MAP, PixelOperationType.LIST_JOB);

	}
}
