package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.db.JedisToJDBCJob;

public class JedisToJDBCJobConfig extends JobConfig {
	
	public JedisToJDBCJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	protected void populateJobDataMap() throws ParseConfigException {
		putString(JedisToJDBCJob.IN_JEDIS_HASH_KEY);
		putString(JedisToJDBCJob.IN_TABLE_NAME_KEY);
		putStringArray(JedisToJDBCJob.IN_COLUMN_HEADERS_KEY);
		putStringArray(JedisToJDBCJob.IN_COLUMN_TYPES_KEY);
		putString(JedisToJDBCJob.IN_JDBC_DRIVER_KEY);
		putString(JedisToJDBCJob.IN_JDBC_CONNECTION_URL_KEY);
		putString(JedisToJDBCJob.IN_JDBC_USERNAME_KEY);
		putString(JedisToJDBCJob.IN_JDBC_PASSWORD_KEY);
	}

}
