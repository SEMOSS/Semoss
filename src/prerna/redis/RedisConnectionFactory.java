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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.util.Pool;

/**
 * Shared Redis pool factory for SEMOSS features that need Redis coordination.
 *
 * <p>
 * Returns a {@link Pool}&lt;{@link Jedis}&gt; so callers work uniformly against
 * either a direct {@link JedisPool} (single node) or a
 * {@link JedisSentinelPool} (high-availability with automatic master failover).
 * Both extend {@code Pool<Jedis>} and hand out {@code Jedis} from
 * {@code getResource()}, so no consumer code changes when Sentinel is toggled
 * on.
 * </p>
 */
public final class RedisConnectionFactory {

	private static final Logger classLogger = LogManager.getLogger(RedisConnectionFactory.class);

	private static final ConcurrentMap<String, Pool<Jedis>> POOLS = new ConcurrentHashMap<>();

	private RedisConnectionFactory() {
	}

	public static Pool<Jedis> getPool() {
		return getPool(RedisConnectionConfig.fromDIHelper());
	}

	public static Pool<Jedis> getPool(RedisConnectionConfig config) {
		return POOLS.computeIfAbsent(config.cacheKey(), ignored -> createPool(config));
	}

	private static Pool<Jedis> createPool(RedisConnectionConfig config) {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(config.getPoolMaxTotal());
		poolConfig.setMaxIdle(config.getPoolMaxIdle());
		poolConfig.setMinIdle(config.getPoolMinIdle());
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestWhileIdle(true);

		if (config.isSentinelEnabled()) {
			return createSentinelPool(config, poolConfig);
		}
		if (config.isSentinelMisconfigured()) {
			classLogger.warn("REDIS_SENTINEL_ENABLED is true but REDIS_MASTER_NAME and/or REDIS_SENTINEL_NODES "
					+ "are not set; falling back to the direct REDIS_HOST/REDIS_PORT connection.");
		}

		String password = config.getPassword();
		if (password != null && !password.trim().isEmpty()) {
			return new JedisPool(poolConfig, config.getHost(), config.getPort(), config.getTimeoutMs(), password);
		}
		return new JedisPool(poolConfig, config.getHost(), config.getPort(), config.getTimeoutMs());
	}

	private static Pool<Jedis> createSentinelPool(RedisConnectionConfig config, JedisPoolConfig poolConfig) {
		// Data-node (master/replica) client config: what our commands actually talk to.
		JedisClientConfig masterClientConfig = clientConfig(config.getTimeoutMs(), config.getPassword());
		// Sentinel client config: sentinel auth is independent of the data-node
		// password.
		JedisClientConfig sentinelClientConfig = clientConfig(config.getTimeoutMs(), config.getSentinelPassword());
		classLogger.info("Connecting to Redis via Sentinel: master=" + config.getMasterName() + ", sentinels="
				+ config.getSentinelNodes());
		return new JedisSentinelPool(config.getMasterName(), config.getSentinelNodes(), poolConfig, masterClientConfig,
				sentinelClientConfig);
	}

	private static JedisClientConfig clientConfig(int timeoutMs, String password) {
		DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder().connectionTimeoutMillis(timeoutMs)
				.socketTimeoutMillis(timeoutMs);
		if (password != null && !password.trim().isEmpty()) {
			builder.password(password);
		}
		return builder.build();
	}
}
