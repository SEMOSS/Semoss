package prerna.rpa.config;

import java.util.ArrayList;
import java.util.List;

import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import prerna.rpa.quartz.JobChain;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class JobChainConfig extends JobConfig {

	private JsonObject jobDefinition;
	
	public JobChainConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}

	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		// For each job definition in the chain sequence,
		// add the job's class and data map entries
		List<Class<? extends InterruptableJob>> chainSequence = new ArrayList<Class<? extends InterruptableJob>>();
		JsonArray chainArray = jobDefinition.get(ConfigUtil.getJSONKey(JobChain.IN_CHAIN_SEQUENCE_KEY)).getAsJsonArray();
		for (JsonElement chainJobElement : chainArray) {
			JsonObject chainJobDefinition = chainJobElement.getAsJsonObject();
			JobConfig chainJobConfig = JobConfig.initialize(chainJobDefinition);
			jobDataMap.putAll(chainJobConfig.getJobDataMap());
			chainSequence.add(JobConfig.getJobClass(chainJobDefinition));
		}
		jobDataMap.put(JobChain.IN_CHAIN_SEQUENCE_KEY, chainSequence);
				
		return jobDataMap;
	}
	
}
