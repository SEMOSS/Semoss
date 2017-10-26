package prerna.rpa.config;

import java.util.HashMap;
import java.util.Map;

import org.quartz.JobDataMap;
import prerna.rpa.quartz.BatchedJobInput;
import prerna.rpa.quartz.JobBatch;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class JobBatchConfig extends JobConfig{
	
	private JsonObject jobDefinition;
	
	public JobBatchConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}

	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		Map<String, BatchedJobInput> batchMap = new HashMap<String, BatchedJobInput>();
		
		// Add the timeout to the map
		long timeout = getLong(jobDefinition, JobBatch.IN_TIMEOUT_KEY);
		jobDataMap.put(JobBatch.IN_TIMEOUT_KEY, timeout);
		
		// For each job definition in the job batch,
		// a) Add JobDataMap and job class into a BatchedJobInput
		// b) Add (job name, BatchedJobInput) into the batch map
		JsonArray batchArray = jobDefinition.get(ConfigUtil.getJSONKey(JobBatch.IN_BATCH_INPUT_MAP_KEY)).getAsJsonArray();
		for (JsonElement batchJobElement : batchArray) {
			JsonObject batchJobDefinition = batchJobElement.getAsJsonObject();
			JobConfig batchJobConfig = JobConfig.initialize(batchJobDefinition);
			BatchedJobInput batchJobInput = new BatchedJobInput(batchJobConfig.getJobDataMap(), JobConfig.getJobClass(batchJobDefinition));
			batchMap.put(JobConfig.getJobName(batchJobDefinition), batchJobInput);
		}
		
		// Add batch map into the data map
		jobDataMap.put(JobBatch.IN_BATCH_INPUT_MAP_KEY, batchMap);
		
		return jobDataMap;
	}
	
}
