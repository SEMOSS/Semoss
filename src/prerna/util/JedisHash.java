/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

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
