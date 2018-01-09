package prerna.rpa.db.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class CustomJedisPoolConfig extends GenericObjectPoolConfig {
	
	public CustomJedisPoolConfig() {
		setTestWhileIdle(true);
		setMinEvictableIdleTimeMillis(60000);
		setTimeBetweenEvictionRunsMillis(30000);
		setNumTestsPerEvictionRun(-1);
	}

}
