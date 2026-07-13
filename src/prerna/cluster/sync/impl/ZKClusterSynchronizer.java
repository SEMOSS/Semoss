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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.sync.IClusterSynchronizer;
import prerna.cluster.util.ClusterSyncMethod;

public class ZKClusterSynchronizer implements IClusterSynchronizer {

	private static volatile ZKClusterSynchronizer sync = null;

	private static final Logger classLogger = LogManager.getLogger(ZKClusterSynchronizer.class);

	private static final String ZK_SERVER_STRING = "ZK_SERVER";

	private static final String SYNC_PROJECT_PATH = "/sync/project";
	private static final String SYNC_ENGINE_PATH = "/sync/engine";
	private static final String SYNC_USER_PATH = "/sync/user";

	private CuratorFramework client = null;
	private CuratorCache projectCache;
	private CuratorCache engineCache;
	private CuratorCache userCache;

	/**
	 * Creates a synchronizer and immediately initializes ZooKeeper connectivity and
	 * listeners.
	 */
	private ZKClusterSynchronizer() {
		initalizeClusterSyncronizer();
	}

	/**
	 * Returns the singleton synchronizer instance.
	 *
	 * @return singleton {@link ZKClusterSynchronizer}
	 * @throws Exception if synchronizer initialization fails
	 */
	static ZKClusterSynchronizer getInstance() throws Exception {
		if (sync != null) {
			return sync;
		}

		if (sync == null) {
			synchronized (ZKClusterSynchronizer.class) {
				if (sync != null) {
					return sync;
				}

				sync = new ZKClusterSynchronizer();
			}
		}

		return sync;
	}

	/**
	 * Initializes Curator client state and ensures the watched synchronization
	 * paths exist in ZooKeeper.
	 */
	private void initalizeClusterSyncronizer() {
		classLogger.info("Starting up cluster synchronizer");

		// hostname:port, localhost:2181
		String zkServerString = CLOUD_PROPS.get(ZK_SERVER_STRING);
		if (zkServerString == null || zkServerString.isEmpty()) {
			throw new IllegalArgumentException("Zookeeper Server endpoint is not defined");
		}

		// make the curator
		try {
			client = CuratorFrameworkFactory.newClient(zkServerString, new RetryNTimes(3, 10));
			client.start();

			// Check if the ZNode exists before trying to create it - project
			if (client.checkExists().forPath(SYNC_PROJECT_PATH) == null) {
				client.create().creatingParentsIfNeeded().forPath(SYNC_PROJECT_PATH);
			}

			// Check if the ZNode exists before trying to create it - engine
			if (client.checkExists().forPath(SYNC_ENGINE_PATH) == null) {
				client.create().creatingParentsIfNeeded().forPath(SYNC_ENGINE_PATH);
			}

			// Check if the ZNode exists before trying to create it - user
			if (client.checkExists().forPath(SYNC_USER_PATH) == null) {
				client.create().creatingParentsIfNeeded().forPath(SYNC_USER_PATH);
			}

			projectCache = createCacheListener(SYNC_PROJECT_PATH);
			engineCache = createCacheListener(SYNC_ENGINE_PATH);
			userCache = createCacheListener(SYNC_USER_PATH);
		} catch (Exception e) {
			classLogger.error(
					"Failed to initialize ClusterSynchronizer with zkServer='{}' and host='{}'. Watch paths: '{}', '{}', '{}'.",
					zkServerString, CONTAINER_IP, SYNC_PROJECT_PATH, SYNC_ENGINE_PATH, SYNC_USER_PATH, e);
		}

	}

	/**
	 * Creates, starts, and wires a {@link CuratorCache} listener for a sync root
	 * path.
	 *
	 * @param pathToWatch path root to monitor for child updates
	 * @return initialized cache for the supplied path
	 */
	private CuratorCache createCacheListener(String pathToWatch) {
		CuratorCache cache = CuratorCache.build(client, pathToWatch);
		CuratorCacheListener listener = CuratorCacheListener.builder()
				.forPathChildrenCache(pathToWatch, client, new PathChildrenCacheListener() {
					@Override
					public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
						if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
							ByteArrayInputStream byteIn = new ByteArrayInputStream(event.getData().getData());
							ObjectInputStream in = new ObjectInputStream(byteIn);
							ClusterSyncEvent syncEvent = (ClusterSyncEvent) in.readObject();

							String updatedByNodeId = syncEvent.getNodeId();
							// if the host updated it, then its already ready - other nodes have to pull
							if (!CONTAINER_IP.equals(updatedByNodeId)) {
								String fullPath = event.getData().getPath();
								classLogger.info("{} updated, pulling latest data from cloud storage", fullPath);
								String id = null;
								boolean pull = false;
								if (fullPath.startsWith(SYNC_PROJECT_PATH)) {
									String[] path = fullPath.split(SYNC_PROJECT_PATH + "/");
									id = path[1];
									pull = ClusterSynchronizerFactory.projectLoaded(id);
								} else if (fullPath.startsWith(SYNC_USER_PATH)) {
									String[] path = fullPath.split(SYNC_USER_PATH + "/");
									id = path[1];
									pull = ClusterSynchronizerFactory.userLoaded(id);
								} else {
									String[] path = fullPath.split(SYNC_ENGINE_PATH + "/");
									id = path[1];
									pull = ClusterSynchronizerFactory.engineLoaded(id);
								}

								// always check if the engine has been loaded before pulling.
								if (pull) {
									try {
										List<Object> params = syncEvent.getParams();
										String methodToken = syncEvent.getMethodName();
										ClusterSyncMethod method = ClusterSyncMethod.fromWireName(methodToken);
										if (method == null) {
											classLogger.error(
													"Unrecognized cluster sync method '{}' for path='{}', id='{}'. Skipping.",
													methodToken, fullPath, id);
										} else {
											method.invoke(params.toArray());
										}
									} catch (Exception e) {
										classLogger.error(
												"Failed to process cluster sync update for path='{}', id='{}', method='{}', params='{}'.",
												fullPath, id, syncEvent.getMethodName(), syncEvent.getParams(), e);
									}
								}

							}
						}
					}
				}).build();

		cache.listenable().addListener(listener);
		cache.start();
		return cache;
	}

	/**
	 * Publishes an engine change event to ZooKeeper so other nodes can pull
	 * updates.
	 *
	 * @param engineId engine identifier
	 * @param method   the {@link ClusterSyncMethod} peers should run to pull the
	 *                 update
	 * @param params   serialized method arguments
	 * @throws Exception if path creation, serialization, or publish fails
	 */
	@Override
	public void publishEngineChange(String engineId, ClusterSyncMethod method, Object... params) throws Exception {
		String enginePath = SYNC_ENGINE_PATH + "/" + engineId;
		// this creates the path if it doesn't exist
		if (client.checkExists().forPath(enginePath) == null) {
			client.create().creatingParentsIfNeeded().forPath(enginePath);
		}

		classLogger.info("Publishing change for engine {} and for nodes to {}", engineId, method.getWireName());
		performWrite(enginePath, method, params);
	}

	/**
	 * Publishes a user asset change event to ZooKeeper so other nodes can pull
	 * updates.
	 *
	 * @param projectId user asset project identifier
	 * @param method    the {@link ClusterSyncMethod} peers should run to pull the
	 *                  update
	 * @param params    serialized method arguments
	 * @throws Exception if path creation, serialization, or publish fails
	 */
	@Override
	public void publishUserChange(String projectId, ClusterSyncMethod method, Object... params) throws Exception {
		String userPath = SYNC_USER_PATH + "/" + projectId;
		// this creates the path if it doesn't exist
		if (client.checkExists().forPath(userPath) == null) {
			client.create().creatingParentsIfNeeded().forPath(userPath);
		}

		classLogger.info("Publishing change for user asset {} and for nodes to {}", projectId, method.getWireName());
		performWrite(userPath, method, params);
	}

	/**
	 * Publishes a project change event to ZooKeeper so other nodes can pull
	 * updates.
	 *
	 * @param projectId project identifier
	 * @param method    the {@link ClusterSyncMethod} peers should run to pull the
	 *                  update
	 * @param params    serialized method arguments
	 * @throws Exception if path creation, serialization, or publish fails
	 */
	@Override
	public void publishProjectChange(String projectId, ClusterSyncMethod method, Object... params) throws Exception {
		String projectPath = SYNC_PROJECT_PATH + "/" + projectId;
		// this creates the path if it doesn't exist
		if (client.checkExists().forPath(projectPath) == null) {
			client.create().creatingParentsIfNeeded().forPath(projectPath);
		}

		classLogger.info("Publishing change for project {} and for nodes to {}", projectId, method.getWireName());
		performWrite(projectPath, method, params);
	}

	/**
	 * Serializes the change payload ({@code nodeId}, wire method name, and params)
	 * and writes it to the given ZooKeeper node, triggering the watchers on peer
	 * nodes.
	 *
	 * @param path   ZooKeeper node path to write the event to
	 * @param method the {@link ClusterSyncMethod} peers should run to pull the
	 *               update
	 * @param params serialized method arguments
	 * @throws Exception if serialization or the ZooKeeper write fails
	 */
	private void performWrite(String path, ClusterSyncMethod method, Object... params) throws Exception {
		ClusterSyncEvent syncEvent = new ClusterSyncEvent(CONTAINER_IP, path, method.getWireName(), params);
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(syncEvent);
		client.setData().forPath(path, byteOut.toByteArray());
	}

}
