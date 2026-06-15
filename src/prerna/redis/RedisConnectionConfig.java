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

import prerna.util.Utility;

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

	private final String host;
	private final int port;
	private final String password;
	private final int timeoutMs;
	private final int poolMaxTotal;
	private final int poolMaxIdle;
	private final int poolMinIdle;

	private RedisConnectionConfig(String host, int port, String password, int timeoutMs,
			int poolMaxTotal, int poolMaxIdle, int poolMinIdle) {
		this.host = host;
		this.port = port;
		this.password = password;
		this.timeoutMs = timeoutMs;
		this.poolMaxTotal = poolMaxTotal;
		this.poolMaxIdle = poolMaxIdle;
		this.poolMinIdle = poolMinIdle;
	}

	public static RedisConnectionConfig fromDIHelper() {
		String host = trimToNull(property(REDIS_HOST));
		if (host == null) {
			host = "localhost";
		}
		int port = (int) getLongProperty(REDIS_PORT, 6379L);
		int timeoutMs = (int) getLongProperty(REDIS_TIMEOUT_MS, 2000L);
		String password = trimToNull(property(REDIS_PASSWORD));
		int poolMaxTotal = Math.max(1,
				(int) getLongProperty(REDIS_POOL_MAX_TOTAL, 64L));
		int poolMaxIdle = Math.max(1,
				(int) getLongProperty(REDIS_POOL_MAX_IDLE, 16L));
		poolMaxIdle = Math.min(poolMaxIdle, poolMaxTotal);
		int poolMinIdle = Math.max(0,
				(int) getLongProperty(REDIS_POOL_MIN_IDLE, 1L));
		poolMinIdle = Math.min(poolMinIdle, poolMaxIdle);
		return new RedisConnectionConfig(host.trim(), port, password, timeoutMs,
				poolMaxTotal, poolMaxIdle, poolMinIdle);
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

	public String cacheKey() {
		return host + "|" + port + "|" + timeoutMs + "|" + nullToEmpty(password)
				+ "|" + poolMaxTotal + "|" + poolMaxIdle + "|" + poolMinIdle;
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
		return Utility.getDIHelperProperty(key);
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
