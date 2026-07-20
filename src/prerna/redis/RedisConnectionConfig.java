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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import prerna.cluster.util.clients.AppCloudClientProperties;
import redis.clients.jedis.HostAndPort;

/**
 * Shared Redis connection settings resolved from DI/RDF properties.
 */
public final class RedisConnectionConfig {

	public static final String REDIS_ENABLED = "REDIS_ENABLED";
	public static final String REDIS_HOST = "REDIS_HOST";
	public static final String REDIS_PORT = "REDIS_PORT";
	public static final String REDIS_PASSWORD = "REDIS_PASSWORD";
	public static final String REDIS_TIMEOUT_MS = "REDIS_TIMEOUT_MS";
	public static final String REDIS_POOL_MAX_TOTAL = "REDIS_POOL_MAX_TOTAL";
	public static final String REDIS_POOL_MAX_IDLE = "REDIS_POOL_MAX_IDLE";
	public static final String REDIS_POOL_MIN_IDLE = "REDIS_POOL_MIN_IDLE";
	// High-availability via Redis Sentinel. When enabled the direct host/port is
	// ignored and the current master is discovered through the sentinel nodes.
	public static final String REDIS_SENTINEL_ENABLED = "REDIS_SENTINEL_ENABLED";
	public static final String REDIS_MASTER_NAME = "REDIS_MASTER_NAME";
	public static final String REDIS_SENTINEL_NODES = "REDIS_SENTINEL_NODES";
	public static final String REDIS_SENTINEL_PASSWORD = "REDIS_SENTINEL_PASSWORD";
	// Horizontal scaling via Redis Cluster. When enabled the direct host/port is
	// ignored and keys are routed across the cluster nodes by hash slot. Cluster
	// takes precedence over Sentinel if both are enabled.
	public static final String REDIS_CLUSTER_ENABLED = "REDIS_CLUSTER_ENABLED";
	public static final String REDIS_CLUSTER_NODES = "REDIS_CLUSTER_NODES";
	public static final String REDIS_CLUSTER_MAX_ATTEMPTS = "REDIS_CLUSTER_MAX_ATTEMPTS";

	private static final int DEFAULT_SENTINEL_PORT = 26379;
	private static final int DEFAULT_REDIS_PORT = 6379;

	private static final AppCloudClientProperties PROPS = AppCloudClientProperties.build();

	private final String host;
	private final int port;
	private final String password;
	private final int timeoutMs;
	private final int poolMaxTotal;
	private final int poolMaxIdle;
	private final int poolMinIdle;
	private final boolean sentinelEnabled;
	private final String masterName;
	private final Set<HostAndPort> sentinelNodes;
	private final String sentinelPassword;
	private final boolean clusterEnabled;
	private final Set<HostAndPort> clusterNodes;
	private final int clusterMaxAttempts;

	private RedisConnectionConfig(String host, int port, String password, int timeoutMs, int poolMaxTotal,
			int poolMaxIdle, int poolMinIdle, boolean sentinelEnabled, String masterName,
			Set<HostAndPort> sentinelNodes, String sentinelPassword, boolean clusterEnabled,
			Set<HostAndPort> clusterNodes, int clusterMaxAttempts) {
		this.host = host;
		this.port = port;
		this.password = password;
		this.timeoutMs = timeoutMs;
		this.poolMaxTotal = poolMaxTotal;
		this.poolMaxIdle = poolMaxIdle;
		this.poolMinIdle = poolMinIdle;
		this.sentinelEnabled = sentinelEnabled;
		this.masterName = masterName;
		this.sentinelNodes = sentinelNodes;
		this.sentinelPassword = sentinelPassword;
		this.clusterEnabled = clusterEnabled;
		this.clusterNodes = clusterNodes;
		this.clusterMaxAttempts = clusterMaxAttempts;
	}

	public static RedisConnectionConfig fromDIHelper() {
		String host = trimToNull(property(REDIS_HOST));
		if (host == null) {
			host = "localhost";
		}
		int port = (int) getLongProperty(REDIS_PORT, 6379L);
		int timeoutMs = (int) getLongProperty(REDIS_TIMEOUT_MS, 2000L);
		String password = trimToNull(property(REDIS_PASSWORD));
		int poolMaxTotal = Math.max(1, (int) getLongProperty(REDIS_POOL_MAX_TOTAL, 64L));
		int poolMaxIdle = Math.max(1, (int) getLongProperty(REDIS_POOL_MAX_IDLE, 16L));
		poolMaxIdle = Math.min(poolMaxIdle, poolMaxTotal);
		int poolMinIdle = Math.max(0, (int) getLongProperty(REDIS_POOL_MIN_IDLE, 1L));
		poolMinIdle = Math.min(poolMinIdle, poolMaxIdle);
		boolean sentinelEnabled = Boolean.parseBoolean(nullToEmpty(trimToNull(property(REDIS_SENTINEL_ENABLED))));
		String masterName = trimToNull(property(REDIS_MASTER_NAME));
		Set<HostAndPort> sentinelNodes = parseNodes(property(REDIS_SENTINEL_NODES), DEFAULT_SENTINEL_PORT);
		String sentinelPassword = trimToNull(property(REDIS_SENTINEL_PASSWORD));
		boolean clusterEnabled = Boolean.parseBoolean(nullToEmpty(trimToNull(property(REDIS_CLUSTER_ENABLED))));
		Set<HostAndPort> clusterNodes = parseNodes(property(REDIS_CLUSTER_NODES), DEFAULT_REDIS_PORT);
		int clusterMaxAttempts = Math.max(1, (int) getLongProperty(REDIS_CLUSTER_MAX_ATTEMPTS, 5L));
		return new RedisConnectionConfig(host.trim(), port, password, timeoutMs, poolMaxTotal, poolMaxIdle, poolMinIdle,
				sentinelEnabled, masterName, sentinelNodes, sentinelPassword, clusterEnabled, clusterNodes,
				clusterMaxAttempts);
	}

	/**
	 * Parses a comma-separated {@code host:port} list of nodes. Entries without an
	 * explicit port fall back to {@code defaultPort}.
	 *
	 * @param raw         the raw property value (nullable)
	 * @param defaultPort the port to use for entries that omit one
	 * @return an ordered, unmodifiable set of endpoints (never null)
	 */
	private static Set<HostAndPort> parseNodes(String raw, int defaultPort) {
		String value = trimToNull(raw);
		if (value == null) {
			return Collections.emptySet();
		}
		Set<HostAndPort> nodes = new LinkedHashSet<>();
		for (String entry : value.split(",")) {
			String node = entry.trim();
			if (node.isEmpty()) {
				continue;
			}
			int lastColon = node.lastIndexOf(':');
			String nodeHost;
			int nodePort;
			if (lastColon > 0 && lastColon < node.length() - 1) {
				nodeHost = node.substring(0, lastColon).trim();
				try {
					nodePort = Integer.parseInt(node.substring(lastColon + 1).trim());
				} catch (NumberFormatException e) {
					nodePort = defaultPort;
				}
			} else {
				nodeHost = node;
				nodePort = defaultPort;
			}
			if (!nodeHost.isEmpty()) {
				nodes.add(new HostAndPort(nodeHost, nodePort));
			}
		}
		return Collections.unmodifiableSet(nodes);
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getPassword() {
		return password;
	}

	public int getTimeoutMs() {
		return timeoutMs;
	}

	public int getPoolMaxTotal() {
		return poolMaxTotal;
	}

	public int getPoolMaxIdle() {
		return poolMaxIdle;
	}

	public int getPoolMinIdle() {
		return poolMinIdle;
	}

	/**
	 * @return true when Cluster is enabled AND at least one cluster node is
	 *         configured. When true the client is built against the cluster nodes
	 *         rather than the direct host/port, and this takes precedence over
	 *         Sentinel.
	 */
	public boolean isClusterEnabled() {
		return clusterEnabled && !clusterNodes.isEmpty();
	}

	/**
	 * @return true when Cluster was requested via {@link #REDIS_CLUSTER_ENABLED}
	 *         but no cluster nodes are configured.
	 */
	public boolean isClusterMisconfigured() {
		return clusterEnabled && clusterNodes.isEmpty();
	}

	/**
	 * @return true when Sentinel is enabled AND a master name and at least one
	 *         sentinel node are configured. Ignored when Cluster is enabled.
	 */
	public boolean isSentinelEnabled() {
		return sentinelEnabled && masterName != null && !sentinelNodes.isEmpty();
	}

	/**
	 * @return true when Sentinel was requested via {@link #REDIS_SENTINEL_ENABLED}
	 *         but the master name or sentinel nodes are missing/invalid.
	 */
	public boolean isSentinelMisconfigured() {
		return sentinelEnabled && (masterName == null || sentinelNodes.isEmpty());
	}

	public String getMasterName() {
		return masterName;
	}

	public Set<HostAndPort> getSentinelNodes() {
		return sentinelNodes;
	}

	public String getSentinelPassword() {
		return sentinelPassword;
	}

	public Set<HostAndPort> getClusterNodes() {
		return clusterNodes;
	}

	public int getClusterMaxAttempts() {
		return clusterMaxAttempts;
	}

	public String cacheKey() {
		return host + "|" + port + "|" + timeoutMs + "|" + nullToEmpty(password) + "|" + poolMaxTotal + "|"
				+ poolMaxIdle + "|" + poolMinIdle + "|sentinel:" + sentinelEnabled + "|" + nullToEmpty(masterName) + "|"
				+ sentinelNodes + "|" + nullToEmpty(sentinelPassword) + "|cluster:" + clusterEnabled + "|"
				+ clusterNodes + "|" + clusterMaxAttempts;
	}

	private static long getLongProperty(String key, long defaultValue) {
		String value = trimToNull(property(key));
		if (value == null) {
			return defaultValue;
		}
		try {
			return Long.parseLong(value.trim());
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	private static String property(String key) {
		if (key == null || key.trim().isEmpty()) {
			return null;
		}
		// checks DIHelper (RDF_Map) first, then falls back to environment variables
		return PROPS.get(key);
	}

	private static String trimToNull(String value) {
		if (value == null) {
			return null;
		}
		String trimmed = value.trim();
		return trimmed.isEmpty() ? null : trimmed;
	}

	private static String nullToEmpty(String value) {
		return value == null ? "" : value;
	}
}
