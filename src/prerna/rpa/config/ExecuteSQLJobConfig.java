package prerna.rpa.config;

import org.quartz.JobDataMap;
import prerna.rpa.quartz.jobs.jdbc.ExecuteSQLJob;

import com.google.gson.JsonObject;

public class ExecuteSQLJobConfig extends JobConfig {
	
	private JsonObject jobDefinition;
	
	public ExecuteSQLJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}

	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		// Connection details
		String driver = getString(jobDefinition, ExecuteSQLJob.DRIVER_KEY);
		jobDataMap.put(ExecuteSQLJob.DRIVER_KEY, driver);
		String connectionUrl = getString(jobDefinition, ExecuteSQLJob.CONNECTION_URL_KEY);
		jobDataMap.put(ExecuteSQLJob.CONNECTION_URL_KEY, connectionUrl);
		String username = getString(jobDefinition, ExecuteSQLJob.USERNAME_KEY);
		jobDataMap.put(ExecuteSQLJob.USERNAME_KEY, username);
		String password = getString(jobDefinition, ExecuteSQLJob.PASSWORD_KEY);
		jobDataMap.put(ExecuteSQLJob.PASSWORD_KEY, password);
		
		// SQL
		String sql = getString(jobDefinition, ExecuteSQLJob.SQL_KEY);		
		jobDataMap.put(ExecuteSQLJob.SQL_KEY, sql);
		
		return jobDataMap;
	}
	
}
