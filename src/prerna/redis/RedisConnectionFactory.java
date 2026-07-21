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

import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.RedisSentinelClient;
import redis.clients.jedis.UnifiedJedis;

/**
 * Shared Redis client factory for SEMOSS features that need Redis coordination.
 *
 * <p>
 * Returns a {@link UnifiedJedis}, the common base of Jedis 7's client family,
 * so callers work uniformly against standalone ({@link RedisClient}), Sentinel
 * high-availability ({@link RedisSentinelClient}), or Cluster
 * ({@link RedisClusterClient}) deployments. Every variant exposes the same
 * command surface and is thread-safe and internally pooled, so a single shared
 * instance is reused per configuration and callers invoke commands directly (no
 * per-call {@code getResource()}/close).
 * </p>
 *
 * <p>
 * Selection precedence: Cluster (if enabled and nodes configured), then
 * Sentinel (if enabled with a master name and nodes), otherwise the direct
 * {@code REDIS_HOST}/{@code REDIS_PORT} standalone connection.
 * </p>
 */
public final class RedisConnectionFactory {

	private static final Logger classLogger = LogManager.getLogger(RedisConnectionFactory.class);

	private static final ConcurrentMap<String, UnifiedJedis> CLIENTS = new ConcurrentHashMap<>();

	private RedisConnectionFactory() {
	}

	public static UnifiedJedis getClient() {
		return getClient(RedisConnectionConfig.fromDIHelper());
	}

	public static UnifiedJedis getClient(RedisConnectionConfig config) {
		return CLIENTS.computeIfAbsent(config.cacheKey(), ignored -> createClient(config));
	}

	private static UnifiedJedis createClient(RedisConnectionConfig config) {
		ConnectionPoolConfig poolConfig = buildPoolConfig(config);
		// Data-node client config: what our commands actually authenticate/talk to.
		JedisClientConfig dataClientConfig = clientConfig(config.getTimeoutMs(), config.getPassword());

		if (config.isClusterEnabled()) {
			classLogger.info("Connecting to Redis via Cluster: nodes=" + config.getClusterNodes());
			return RedisClusterClient.builder().nodes(config.getClusterNodes())
					.maxAttempts(config.getClusterMaxAttempts()).clientConfig(dataClientConfig).poolConfig(poolConfig)
					.build();
		}

		if (config.isSentinelEnabled()) {
			// Sentinel auth is independent of the data-node password.
			JedisClientConfig sentinelClientConfig = clientConfig(config.getTimeoutMs(), config.getSentinelPassword());
			classLogger.info("Connecting to Redis via Sentinel: master=" + config.getMasterName() + ", sentinels="
					+ config.getSentinelNodes());
			return RedisSentinelClient.builder().masterName(config.getMasterName()).sentinels(config.getSentinelNodes())
					.sentinelClientConfig(sentinelClientConfig).clientConfig(dataClientConfig).poolConfig(poolConfig)
					.build();
		}

		if (config.isClusterMisconfigured()) {
			classLogger.warn("REDIS_CLUSTER_ENABLED is true but REDIS_CLUSTER_NODES is not set; "
					+ "falling back to the direct REDIS_HOST/REDIS_PORT connection.");
		}
		if (config.isSentinelMisconfigured()) {
			classLogger.warn("REDIS_SENTINEL_ENABLED is true but REDIS_MASTER_NAME and/or REDIS_SENTINEL_NODES "
					+ "are not set; falling back to the direct REDIS_HOST/REDIS_PORT connection.");
		}

		classLogger.info("Connecting to Redis standalone: " + config.getHost() + ":" + config.getPort());
		return RedisClient.builder().hostAndPort(config.getHost(), config.getPort()).clientConfig(dataClientConfig)
				.poolConfig(poolConfig).build();
	}

	private static ConnectionPoolConfig buildPoolConfig(RedisConnectionConfig config) {
		ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
		poolConfig.setMaxTotal(config.getPoolMaxTotal());
		poolConfig.setMaxIdle(config.getPoolMaxIdle());
		poolConfig.setMinIdle(config.getPoolMinIdle());
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestWhileIdle(true);
		return poolConfig;
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
