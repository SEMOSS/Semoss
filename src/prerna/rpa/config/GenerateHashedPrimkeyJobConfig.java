package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.db.jdbc.maria.GenerateHashedPrimkeyJob;

public class GenerateHashedPrimkeyJobConfig extends JobConfig {
	
	public GenerateHashedPrimkeyJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	protected void populateJobDataMap() throws ParseConfigException {
		
		// Connection details
		putString(GenerateHashedPrimkeyJob.IN_DRIVER_KEY);
		putString(GenerateHashedPrimkeyJob.IN_CONNECTION_URL_KEY);
		putString(GenerateHashedPrimkeyJob.IN_USERNAME_KEY);
		putString(GenerateHashedPrimkeyJob.IN_PASSWORD_KEY);
		
		// Primkey details
		putString(GenerateHashedPrimkeyJob.IN_TABLE_NAME_KEY);
		putString(GenerateHashedPrimkeyJob.IN_HASH_COLUMNS_KEY);
		putString(GenerateHashedPrimkeyJob.IN_PRIMKEY_NAME_KEY);
		putString(GenerateHashedPrimkeyJob.IN_PRIMKEY_LENGTH_KEY);
	}
	
}
