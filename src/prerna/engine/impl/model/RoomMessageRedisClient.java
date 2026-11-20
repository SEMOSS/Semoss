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

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

/**
 * Narrow Redis client for room-message coordination and cache keys.
 */
public final class RoomMessageRedisClient {

	private static final String UNLOCK_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
	private static final String RENEW_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";

	private final UnifiedJedis client;

	RoomMessageRedisClient(UnifiedJedis client) {
		this.client = client;
	}

	boolean tryAcquireLock(String roomId, String token, long ttlMs) {
		return withJedis(jedis -> "OK".equals(jedis.set(lockKey(roomId), token, SetParams.setParams().nx().px(ttlMs))));
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

	void deleteMessages(String roomId) {
		withJedis(jedis -> jedis.del(messagesKey(roomId)));
	}

	void incrementVersion(String roomId) {
		withJedis(jedis -> jedis.incr(versionKey(roomId)));
	}

	public boolean tryClaimActiveRun(String roomId, String runId, String token, long ttlMs) {
		return withJedis(jedis -> "OK".equals(
				jedis.set(activeRunKey(roomId), activeRunValue(runId, token), SetParams.setParams().nx().px(ttlMs))));
	}

	public void releaseActiveRun(String roomId, String runId, String token) {
		withJedis(jedis -> jedis.eval(UNLOCK_SCRIPT, Collections.singletonList(activeRunKey(roomId)),
				Collections.singletonList(activeRunValue(runId, token))));
	}

	public void renewActiveRun(String roomId, String runId, String token, long ttlMs) {
		withJedis(jedis -> jedis.eval(RENEW_SCRIPT, Collections.singletonList(activeRunKey(roomId)),
				java.util.Arrays.asList(activeRunValue(runId, token), String.valueOf(ttlMs))));
	}

	private <T> T withJedis(JedisCallback<T> callback) {
		// UnifiedJedis is thread-safe and internally pooled; it is shared and must
		// NOT be closed per call (that would tear down the whole client).
		try {
			return callback.apply(client);
		} catch (Exception e) {
			throw new IllegalStateException("Redis room-message command failed: " + e.getMessage(), e);
		}
	}

	// The {roomId} hash tag co-locates all of a room's keys on a single Redis
	// Cluster hash slot, so multi-key/EVAL operations stay within one slot and
	// never trigger CROSSSLOT. It is inert on standalone/Sentinel deployments.
	private static String messagesKey(String roomId) {
		return "room:{" + roomId + "}:messages";
	}

	private static String versionKey(String roomId) {
		return "room:{" + roomId + "}:version";
	}

	private static String lockKey(String roomId) {
		return "room:{" + roomId + "}:lock";
	}

	private static String activeRunKey(String roomId) {
		return "room:{" + roomId + "}:active_run";
	}

	private static String activeRunValue(String runId, String token) {
		return runId + "|" + token;
	}

	private interface JedisCallback<T> {
		T apply(UnifiedJedis jedis);
	}
}
