package prerna.rpa.config.specific.anthem;

import com.google.gson.JsonObject;

import prerna.rpa.config.JobConfig;
import prerna.rpa.config.ParseConfigException;
import prerna.rpa.quartz.jobs.reporting.kickout.RunKickoutAlgorithmJob;
import prerna.rpa.reporting.kickout.KickoutAlgorithm;

public class RunKickoutAlgorithmJobConfig extends JobConfig {
	
	public RunKickoutAlgorithmJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	protected void populateJobDataMap() throws ParseConfigException {
		
		// The type of algorithm
		KickoutAlgorithm algorithm = KickoutAlgorithm.getKickoutAlgorithmFromAlgorithmName(getString(RunKickoutAlgorithmJob.IN_KICKOUT_ALGORITHM));
		jobDataMap.put(RunKickoutAlgorithmJob.IN_KICKOUT_ALGORITHM, algorithm);
		
		// Jedis prefix
		putString(RunKickoutAlgorithmJob.IN_PREFIX_KEY);
		
		// Window of time before a record is considered new
		putInt(RunKickoutAlgorithmJob.IN_D_KEY);
	}

}
