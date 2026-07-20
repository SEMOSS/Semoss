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
package prerna.cluster.sync.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.cluster.sync.IClusterSynchronizer;
import prerna.cluster.util.ClusterSyncMethod;
import prerna.redis.RedisConnectionConfig;
import prerna.redis.RedisConnectionFactory;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.UnifiedJedis;

public class RedisClusterSynchronizer implements IClusterSynchronizer {

	private static volatile RedisClusterSynchronizer sync = null;

	private static final Logger classLogger = LogManager.getLogger(RedisClusterSynchronizer.class);

	private static final String SYNC_PROJECT_CHANNEL = "/sync/project";
	private static final String SYNC_ENGINE_CHANNEL = "/sync/engine";
	private static final String SYNC_USER_CHANNEL = "/sync/user";

	// delay before re-subscribing after a subscription failure
	private static final long RESUBSCRIBE_BACKOFF_MS = 5000L;

	private UnifiedJedis client;
	private Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	/**
	 * Creates a synchronizer and immediately initializes Redis connectivity
	 */
	private RedisClusterSynchronizer() {
		initalizeClusterSyncronizer();
	}

	/**
	 * Returns the singleton synchronizer instance.
	 *
	 * @return singleton {@link RedisClusterSynchronizer}
	 * @throws Exception if synchronizer initialization fails
	 */
	static RedisClusterSynchronizer getInstance() throws Exception {
		if (sync != null) {
			return sync;
		}

		if (sync == null) {
			synchronized (RedisClusterSynchronizer.class) {
				if (sync != null) {
					return sync;
				}

				sync = new RedisClusterSynchronizer();
			}
		}

		return sync;
	}

	private void initalizeClusterSyncronizer() {
		RedisConnectionConfig config = RedisConnectionConfig.fromDIHelper();
		client = RedisConnectionFactory.getClient(config);

		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(() -> {
			while (true) {
				try {
					// subscribe blocks; UnifiedJedis borrows a dedicated connection for it
					client.subscribe(new JedisPubSub() {

						@Override
						public void onMessage(String channel, String message) {
							/*
							 * channel determines if it is an engine, project, or user that has been updated
							 * messages tells us the id of the engine, the id of the project, the id of the
							 * user asset engine
							 */
							classLogger.info("Recieved message from {}-{}", channel, message);

							boolean pull = false;
							if (channel.equals(SYNC_PROJECT_CHANNEL)) {
								pull = ClusterSynchronizerFactory.projectLoaded(message);
							} else if (channel.equals(SYNC_USER_CHANNEL)) {
								pull = ClusterSynchronizerFactory.userLoaded(message);
							} else {
								pull = ClusterSynchronizerFactory.engineLoaded(message);
							}

							if (pull) {
								// each command borrows its own pooled connection, so this
								// does not collide with the blocking subscriber connection
								{
									String json = client.get(channel + "/" + message);
									ClusterSyncEvent syncEvent = gson.fromJson(json, ClusterSyncEvent.class);
									String updatedByNodeId = syncEvent.getNodeId();
									// double check this host is not the one that pushed the message
									if (CONTAINER_IP.equals(updatedByNodeId)) {
										classLogger.info(
												"Loaded project {} was updated on this container. Ignoring message.",
												message);
									} else {
										try {
											List<Object> params = syncEvent.getParams();
											String methodToken = syncEvent.getMethodName();
											ClusterSyncMethod method = ClusterSyncMethod.fromWireName(methodToken);
											if (method == null) {
												classLogger.error(
														"Unrecognized cluster sync method '{}' for channel='{}', id='{}'. Skipping.",
														methodToken, channel, message);
											} else {
												method.invoke(params.toArray());
											}
										} catch (Exception e) {
											classLogger.error(
													"Failed to process cluster sync update for channel='{}', id='{}', method='{}', params='{}'.",
													channel, message, syncEvent.getMethodName(), syncEvent.getParams(),
													e);
										}
									}
								}
							}
						}
					}, SYNC_PROJECT_CHANNEL, SYNC_ENGINE_CHANNEL, SYNC_USER_CHANNEL);
				} catch (Exception e) {
					classLogger.error("Error occurred breaking out of redis subscription for container changes: {}",
							e.getMessage(), e);
					// back off before re-subscribing so a down Redis doesn't tight-loop
					try {
						Thread.sleep(RESUBSCRIBE_BACKOFF_MS);
					} catch (InterruptedException ie) {
						// honor the interrupt (e.g. shutdown) and stop re-subscribing
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
		});
	}

	@Override
	public void publishProjectChange(String projectId, ClusterSyncMethod method, Object... params) throws Exception {
		performWrite(SYNC_PROJECT_CHANNEL, projectId, method, params);
	}

	@Override
	public void publishEngineChange(String engineId, ClusterSyncMethod method, Object... params) throws Exception {
		performWrite(SYNC_ENGINE_CHANNEL, engineId, method, params);
	}

	@Override
	public void publishUserChange(String projectId, ClusterSyncMethod method, Object... params) throws Exception {
		performWrite(SYNC_USER_CHANNEL, projectId, method, params);
	}

	private void performWrite(String channel, String engineId, ClusterSyncMethod method, Object... params) {
		String key = channel + "/" + engineId;
		ClusterSyncEvent syncEvent = new ClusterSyncEvent(CONTAINER_IP, key, method.getWireName(), params);
		client.set(key, gson.toJson(syncEvent));
		client.publish(channel, engineId);
	}

}
