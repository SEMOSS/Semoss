/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
		}catch(RuntimeException ignored)
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
