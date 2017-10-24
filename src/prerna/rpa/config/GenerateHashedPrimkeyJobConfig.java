package prerna.rpa.config;

import org.quartz.JobDataMap;
import prerna.rpa.quartz.jobs.jdbc.maria.GenerateHashedPrimkeyJob;

import com.google.gson.JsonObject;

public class GenerateHashedPrimkeyJobConfig extends JobConfig {
	
	private JsonObject jobDefinition;
	
	public GenerateHashedPrimkeyJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}

	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		// Connection details
		String driver = getString(jobDefinition, GenerateHashedPrimkeyJob.DRIVER_KEY);
		jobDataMap.put(GenerateHashedPrimkeyJob.DRIVER_KEY, driver);
		String connectionUrl = getString(jobDefinition, GenerateHashedPrimkeyJob.CONNECTION_URL_KEY);
		jobDataMap.put(GenerateHashedPrimkeyJob.CONNECTION_URL_KEY, connectionUrl);
		String username = getString(jobDefinition, GenerateHashedPrimkeyJob.USERNAME_KEY);
		jobDataMap.put(GenerateHashedPrimkeyJob.USERNAME_KEY, username);
		String password = getString(jobDefinition, GenerateHashedPrimkeyJob.PASSWORD_KEY);
		jobDataMap.put(GenerateHashedPrimkeyJob.PASSWORD_KEY, password);
		
		// Primkey details
		String tableName = getString(jobDefinition, GenerateHashedPrimkeyJob.TABLE_NAME_KEY);
		jobDataMap.put(GenerateHashedPrimkeyJob.TABLE_NAME_KEY, tableName);
		String hashColumns = getString(jobDefinition, GenerateHashedPrimkeyJob.HASH_COLUMNS_KEY);
		jobDataMap.put(GenerateHashedPrimkeyJob.HASH_COLUMNS_KEY, hashColumns);
		String primkeyName = getString(jobDefinition, GenerateHashedPrimkeyJob.PRIMKEY_NAME_KEY);
		jobDataMap.put(GenerateHashedPrimkeyJob.PRIMKEY_NAME_KEY, primkeyName);
		String primkeyLength = getString(jobDefinition, GenerateHashedPrimkeyJob.PRIMKEY_LENGTH_KEY);
		jobDataMap.put(GenerateHashedPrimkeyJob.PRIMKEY_LENGTH_KEY, primkeyLength);
		
		return jobDataMap;
	}
	
}
