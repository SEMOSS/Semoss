package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.db.jdbc.ExecuteSQLJob;

public class ExecuteSQLJobConfig extends JobConfig {
		
	public ExecuteSQLJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	protected void populateJobDataMap() throws ParseConfigException {
		
		// Connection details
		putString(ExecuteSQLJob.IN_DRIVER_KEY);
		putString(ExecuteSQLJob.IN_CONNECTION_URL_KEY);
		putString(ExecuteSQLJob.IN_USERNAME_KEY);
		putString(ExecuteSQLJob.IN_PASSWORD_KEY);
		
		// SQL
		putString(ExecuteSQLJob.IN_SQL_KEY);		
	}
	
}
