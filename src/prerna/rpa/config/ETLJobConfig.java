package prerna.rpa.config;

import org.quartz.JobDataMap;
import prerna.rpa.quartz.jobs.jdbc.maria.ETLJob;

import com.google.gson.JsonObject;

public class ETLJobConfig extends JobConfig {
	
	private JsonObject jobDefinition;
	
	public ETLJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}

	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		// "From" connection details
		String fromDriver = getString(jobDefinition, ETLJob.IN_FROM_DRIVER_KEY);
		jobDataMap.put(ETLJob.IN_FROM_DRIVER_KEY, fromDriver);
		String fromConnectionUrl = getString(jobDefinition, ETLJob.IN_FROM_CONNECTION_URL_KEY);
		jobDataMap.put(ETLJob.IN_FROM_CONNECTION_URL_KEY, fromConnectionUrl);
		String fromUsername = getString(jobDefinition, ETLJob.IN_FROM_USERNAME_KEY);
		jobDataMap.put(ETLJob.IN_FROM_USERNAME_KEY, fromUsername);
		String fromPassword = getString(jobDefinition, ETLJob.IN_FROM_PASSWORD_KEY);
		jobDataMap.put(ETLJob.IN_FROM_PASSWORD_KEY, fromPassword);
		
		// "From" sqls
		String fromSQLExecute = getString(jobDefinition, ETLJob.IN_FROM_SQL_EXECUTE_KEY);		
		jobDataMap.put(ETLJob.IN_FROM_SQL_EXECUTE_KEY, fromSQLExecute);
		String fromSQLQuery = getString(jobDefinition, ETLJob.IN_FROM_SQL_QUERY_KEY);
		jobDataMap.put(ETLJob.IN_FROM_SQL_QUERY_KEY, fromSQLQuery);
		
		// "To" connection details
		String toDriver = getString(jobDefinition, ETLJob.IN_TO_DRIVER_KEY);
		jobDataMap.put(ETLJob.IN_TO_DRIVER_KEY, toDriver);
		String toConnectionUrl = getString(jobDefinition, ETLJob.IN_TO_CONNECTION_URL_KEY);
		jobDataMap.put(ETLJob.IN_TO_CONNECTION_URL_KEY, toConnectionUrl);
		String toUsername = getString(jobDefinition, ETLJob.IN_TO_USERNAME_KEY);
		jobDataMap.put(ETLJob.IN_TO_USERNAME_KEY, toUsername);
		String toPassword = getString(jobDefinition, ETLJob.IN_TO_PASSWORD_KEY);
		jobDataMap.put(ETLJob.IN_TO_PASSWORD_KEY, toPassword);
		
		// "To" table name
		String toTableName = getString(jobDefinition, ETLJob.IN_TO_TABLE_NAME_KEY);
		jobDataMap.put(ETLJob.IN_TO_TABLE_NAME_KEY, toTableName);
		
		return jobDataMap;
	}
	
}
