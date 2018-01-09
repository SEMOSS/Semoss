package prerna.rpa.config;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import prerna.rpa.quartz.BatchedJobInput;
import prerna.rpa.quartz.JobBatch;

public class JobBatchConfig extends JobConfig{
	
	public JobBatchConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	protected void populateJobDataMap() throws ParseConfigException, IllegalConfigException {
		Map<String, BatchedJobInput> batchMap = new HashMap<>();
		
		// Add the timeout to the map
		putLong(JobBatch.IN_TIMEOUT_KEY);
		
		// For each job definition in the job batch,
		// a) Add JobDataMap and job class into a BatchedJobInput
		// b) Add (job name, BatchedJobInput) into the batch map
		JsonArray batchArray = jobDefinition.get(ConfigUtil.getJSONKey(JobBatch.IN_BATCH_INPUT_MAP_KEY)).getAsJsonArray();
		for (JsonElement batchJobElement : batchArray) {
			JsonObject batchJobDefinition = batchJobElement.getAsJsonObject();
			JobConfig batchJobConfig = JobConfig.initialize(batchJobDefinition);
			BatchedJobInput batchJobInput = new BatchedJobInput(batchJobConfig.getJobDataMap(), batchJobConfig.getJobClass());
			batchMap.put(batchJobConfig.getJobName(), batchJobInput);
		}
		
		// Add batch map into the data map
		jobDataMap.put(JobBatch.IN_BATCH_INPUT_MAP_KEY, batchMap);
	}
	
}
