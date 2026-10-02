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
package prerna.reactor.agent.run;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.RoomMessageRedisClient;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.redis.RedisConnectionConfig;
import prerna.util.Utility;

/**
 * The cluster-wide half of the one-agent-per-room rule: a room's turn is held
 * by at most one node at a time.
 *
 * <p>
 * {@link AgentRunRegistry} enforces the same rule inside a single JVM. This
 * lock is what keeps two nodes from starting agents in the same room, and the
 * Redis commands behind it live in {@code RoomMessageRedisClient}. What is
 * decided here is the policy around them: whether the lock is used at all, the
 * fencing token that makes release and renewal safe, the lease TTL, and the
 * heartbeat that keeps a long run's claim alive.
 *
 * <p>
 * When the queue is disabled the claim always succeeds and returns
 * {@link RoomTurnLease#NO_OP}, so callers take one code path and the in-JVM
 * registry is the only gate.
 */
final class ClusterRoomTurnLock {

	private static final Logger logger = LogManager.getLogger(ClusterRoomTurnLock.class);

	private static final String QUEUE_ENABLED = "AGENT_RUN_QUEUE_ENABLED";
	private static final String ACTIVE_TTL_MS = "AGENT_RUN_ACTIVE_TTL_MS";
	private static final long DEFAULT_ACTIVE_TTL_MS = 300000L;

	private ClusterRoomTurnLock() {

	}

	static boolean isQueueEnabled() {
		String configured = Utility.getDIHelperProperty(QUEUE_ENABLED);
		if (configured == null || configured.trim().isEmpty()) {
			return RedisConnectionConfig.isRedisEnabled();
		}
		// the queue is Redis-backed, so an explicit opt-in still needs Redis enabled
		return Boolean.parseBoolean(configured.trim()) && RedisConnectionConfig.isRedisEnabled();
	}

	/**
	 * Claims {@code roomId}'s turn for {@code runId} across the cluster.
	 *
	 * @return the lease, which the caller must close when the run settles, or
	 *         {@code null} when another node holds the room
	 */
	static RoomTurnLease tryClaim(String runId, String roomId) {
		if (!isQueueEnabled()) {
			return RoomTurnLease.NO_OP;
		}
		long activeTtlMs = Math.max(1000L, getLongProperty(ACTIVE_TTL_MS, DEFAULT_ACTIVE_TTL_MS));
		RoomMessageRedisClient redis = RoomMessageStore.redisClient();
		String token = UUID.randomUUID().toString();
		if (!redis.tryClaimActiveRun(roomId, runId, token, activeTtlMs)) {
			return null;
		}
		RoomTurnLease lease = new RoomTurnLease(redis, roomId, runId, token, activeTtlMs);
		lease.startRenewal();
		return lease;
	}

	private static long getLongProperty(String key, long defaultValue) {
		String value = Utility.getDIHelperProperty(key);
		if (value == null || value.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			return Long.parseLong(value.trim());
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	static final class RoomTurnLease implements AutoCloseable {
		static final RoomTurnLease NO_OP = new RoomTurnLease(null, null, null, null, 0L);

		private final RoomMessageRedisClient redis;
		private final String roomId;
		private final String runId;
		private final String token;
		private final long ttlMs;
		private volatile boolean closed;

		private RoomTurnLease(RoomMessageRedisClient redis, String roomId, String runId, String token, long ttlMs) {
			this.redis = redis;
			this.roomId = roomId;
			this.runId = runId;
			this.token = token;
			this.ttlMs = ttlMs;
		}

		private void startRenewal() {
			if (redis == null || ttlMs <= 0L) {
				return;
			}
			Thread renewalThread = new Thread(() -> {
				long sleepMs = Math.max(1000L, ttlMs / 3L);
				while (!closed) {
					try {
						Thread.sleep(sleepMs);
						if (!closed) {
							redis.renewActiveRun(roomId, runId, token, ttlMs);
						}
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					} catch (Exception e) {
						logger.warn("AgentRun active-run lease renewal failed for room={} runId={}", roomId, runId, e);
					}
				}
			}, "agent-run-active-renewal");
			renewalThread.setDaemon(true);
			renewalThread.start();
		}

		@Override
		public void close() {
			if (closed || redis == null) {
				return;
			}
			closed = true;
			try {
				redis.releaseActiveRun(roomId, runId, token);
			} catch (Exception e) {
				logger.warn("AgentRun active-run lease release failed for room={} runId={}", roomId, runId, e);
			}
		}
	}
}
