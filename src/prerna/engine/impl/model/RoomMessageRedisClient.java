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
package prerna.engine.impl.model;

import java.util.Collections;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

/**
 * Narrow Redis client for room-message coordination and cache keys.
 */
public final class RoomMessageRedisClient {

	private static final String UNLOCK_SCRIPT =
			"if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
	private static final String RENEW_SCRIPT =
			"if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";

	private final String host;
	private final int port;
	private final String password;
	private final int timeoutMs;

	RoomMessageRedisClient(String host, int port, String password, int timeoutMs) {
		this.host = host;
		this.port = port;
		this.password = password;
		this.timeoutMs = timeoutMs;
	}

	boolean tryAcquireLock(String roomId, String token, long ttlMs) {
		return withJedis(jedis -> "OK".equals(jedis.set(lockKey(roomId), token,
				SetParams.setParams().nx().px(ttlMs))));
	}

	void releaseLock(String roomId, String token) {
		withJedis(jedis -> jedis.eval(UNLOCK_SCRIPT, Collections.singletonList(lockKey(roomId)),
				Collections.singletonList(token)));
	}

	void renewLock(String roomId, String token, long ttlMs) {
		withJedis(jedis -> jedis.eval(RENEW_SCRIPT, Collections.singletonList(lockKey(roomId)),
				java.util.Arrays.asList(token, String.valueOf(ttlMs))));
	}

	String getMessages(String roomId) {
		return withJedis(jedis -> jedis.get(messagesKey(roomId)));
	}

	void setMessages(String roomId, String value) {
		withJedis(jedis -> jedis.set(messagesKey(roomId), value));
	}

	void incrementVersion(String roomId) {
		withJedis(jedis -> jedis.incr(versionKey(roomId)));
	}

	private <T> T withJedis(JedisCallback<T> callback) {
		try (Jedis jedis = new Jedis(host, port, timeoutMs)) {
			if (password != null && !password.trim().isEmpty()) {
				jedis.auth(password);
			}
			return callback.apply(jedis);
		} catch (Exception e) {
			throw new IllegalStateException("Redis room-message command failed: " + e.getMessage(), e);
		}
	}

	private static String messagesKey(String roomId) {
		return "room:" + roomId + ":messages";
	}

	private static String versionKey(String roomId) {
		return "room:" + roomId + ":version";
	}

	private static String lockKey(String roomId) {
		return "room:" + roomId + ":lock";
	}

	private interface JedisCallback<T> {
		T apply(Jedis jedis);
	}
}
