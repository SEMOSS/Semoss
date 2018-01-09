package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.db.jdbc.ETLJob;

public class ETLJobConfig extends JobConfig {
	
	public ETLJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}

	@Override
	protected void populateJobDataMap() throws ParseConfigException {
		
		// "From" connection details
		putString(ETLJob.IN_FROM_DRIVER_KEY);
		putString(ETLJob.IN_FROM_CONNECTION_URL_KEY);
		putString(ETLJob.IN_FROM_USERNAME_KEY);
		putString(ETLJob.IN_FROM_PASSWORD_KEY);
		
		// "From" sqls
		putString(ETLJob.IN_FROM_SQL_EXECUTE_KEY);		
		putString(ETLJob.IN_FROM_SQL_QUERY_KEY);
				
		// "To" connection details
		putString(ETLJob.IN_TO_DRIVER_KEY);
		putString(ETLJob.IN_TO_CONNECTION_URL_KEY);
		putString(ETLJob.IN_TO_USERNAME_KEY);
		putString(ETLJob.IN_TO_PASSWORD_KEY);
		
		// "To" table name
		putString(ETLJob.IN_TO_TABLE_NAME_KEY);
	}
	
}
