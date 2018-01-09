package prerna.rpa.config;

import java.util.ArrayList;
import java.util.List;

import org.quartz.InterruptableJob;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import prerna.rpa.quartz.JobChain;

public class JobChainConfig extends JobConfig {
	
	public JobChainConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	protected void populateJobDataMap() throws ParseConfigException, IllegalConfigException {
		
		// For each job definition in the chain sequence,
		// add the job's class and data map entries
		List<Class<? extends InterruptableJob>> chainSequence = new ArrayList<>();
		JsonArray chainArray = jobDefinition.get(ConfigUtil.getJSONKey(JobChain.IN_CHAIN_SEQUENCE_KEY)).getAsJsonArray();
		for (JsonElement chainJobElement : chainArray) {
			JsonObject chainJobDefinition = chainJobElement.getAsJsonObject();
			JobConfig chainJobConfig = JobConfig.initialize(chainJobDefinition);
			jobDataMap.putAll(chainJobConfig.getJobDataMap());			
			Class<? extends InterruptableJob> chainJobClass = chainJobConfig.getJobClass();
			if (chainJobClass == JobChain.class) {
				throw new IllegalConfigException("Cannot add a job chain directly within another job chain. Try JobChain(IsolatedJob(JobChain())) (non-blocking) or JobChain(JobBatch(JobChain())) (blocking).");
			} else if (chainSequence.contains(chainJobConfig.getJobClass())) {
				throw new IllegalConfigException("Cannot add two jobs of the same class into a single job chain, as the context of the second job will override that of the first. Try JobChain(IsolatedJob(Job()), Job()) (non-blocking) or JobChain(JobBatch(Job()), Job()) (blocking).");
			} else {
				chainSequence.add(chainJobConfig.getJobClass());
			}
		}
		jobDataMap.put(JobChain.IN_CHAIN_SEQUENCE_KEY, chainSequence);			
	}
	
}
