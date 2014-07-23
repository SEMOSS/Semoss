package prerna.util;

import java.util.Hashtable;

import redis.clients.jedis.Jedis;

public class JedisHash{

	public static JedisHash instance = null;
	private Jedis jedis = null;
	boolean connected = false;
	
	protected JedisHash()
	{
		try
		{
			String redisHost = DIHelper.getInstance().getProperty(Constants.REDIS_HOST);
			if(redisHost != null)
			{
				int redisPort = Integer.parseInt(DIHelper.getInstance().getProperty(Constants.REDIS_PORT));
				jedis = new Jedis (redisHost, redisPort); //pool.getResource();
				connected = true;
			}
		}catch(Exception ignored)
		{
			ignored.printStackTrace();
		}
	}
	
	public static JedisHash getInstance()
	{
		if(instance == null)
			instance = new JedisHash();
		return instance;
	}
	
	public String get(String key)
	{
		if(connected)
			return jedis.get(key); 
		return null;
	}
	
	public void put(String key, String object)
	{
		if(connected)
			jedis.set(key,object);
		// else 
		// really nothing much to do
	}
}
