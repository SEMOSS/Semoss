/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.redis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Shared Redis pool factory for SEMOSS features that need Redis coordination.
 */
public final class RedisConnectionFactory {

	private static final ConcurrentMap<String, JedisPool> POOLS = new ConcurrentHashMap<>();

	private RedisConnectionFactory() {
	}

	public static JedisPool getPool() {
		RedisConnectionConfig config = RedisConnectionConfig.fromDIHelper();
		if (config == null) {
			return null;
		}
		return getPool(config);
	}

	public static JedisPool getPool(RedisConnectionConfig config) {
		return POOLS.computeIfAbsent(config.cacheKey(), ignored -> createPool(config));
	}

	private static JedisPool createPool(RedisConnectionConfig config) {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(config.getPoolMaxTotal());
		poolConfig.setMaxIdle(config.getPoolMaxIdle());
		poolConfig.setMinIdle(config.getPoolMinIdle());
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestWhileIdle(true);
		String password = config.getPassword();
		if (password != null && !password.trim().isEmpty()) {
			return new JedisPool(poolConfig, config.getHost(), config.getPort(), config.getTimeoutMs(), password);
		}
		return new JedisPool(poolConfig, config.getHost(), config.getPort(), config.getTimeoutMs());
	}
}
