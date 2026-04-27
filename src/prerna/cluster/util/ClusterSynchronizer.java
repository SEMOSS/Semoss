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
package prerna.cluster.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.UserAssetUtils;
import prerna.cluster.util.clients.AppCloudClientProperties;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Coordinates cross-node synchronization events through ZooKeeper for engines,
 * projects, and user asset state.
 * <p>
 * This component publishes change notifications to ZooKeeper paths and listens
 * for updates from other nodes so stale local resources can be refreshed from
 * shared cloud storage.
 */
public final class ClusterSynchronizer {

	private static volatile ClusterSynchronizer sync = null;

	private static final Logger classLogger = LogManager.getLogger(ClusterSynchronizer.class);

	public static final String ZK_SERVER_STRING = "ZK_SERVER";
	public static final String HOST_IP = "HOST_IP";

	public static final String SYNC_PROJECT_PATH = "/sync/project";
	public static final String SYNC_ENGINE_PATH = "/sync/engine";
	public static final String SYNC_USER_PATH = "/sync/user";

	private CuratorFramework client = null;
	private CuratorCache projectCache;
	private CuratorCache engineCache;
	private CuratorCache userCache;

	String host;

	/**
	 * Creates a synchronizer and immediately initializes ZooKeeper connectivity and
	 * listeners.
	 */
	private ClusterSynchronizer() {
		initalizeClusterSyncronizer();
	}

	/**
	 * Returns the singleton synchronizer instance.
	 *
	 * @return singleton {@link ClusterSynchronizer}
	 * @throws Exception if synchronizer initialization fails
	 */
	public static ClusterSynchronizer getInstance() throws Exception {
		if (sync != null) {
			return sync;
		}

		if (sync == null) {
			synchronized (ClusterSynchronizer.class) {
				if (sync != null) {
					return sync;
				}

				sync = new ClusterSynchronizer();
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
		AppCloudClientProperties clientProps = new AppCloudClientProperties();

		// what is the zk server ip
		String zk_server = clientProps.get(ZK_SERVER_STRING);
		// zk_server="localhost:2181";

		if (zk_server == null || zk_server.isEmpty()) {
			throw new IllegalArgumentException("Zookeeper Server endpoint is not defined");
		}

		// what is the host ip of the container/pod/box - this is used as a unique id
		// for the container/singleton
		host = clientProps.get(HOST_IP);
		if (host == null || host.isEmpty()) {
			classLogger.info("Host IP is not set");
			host = "node_" + Utility.getRandomString(5);
		}

		// make the curator
		try {
			client = CuratorFrameworkFactory.newClient(zk_server, new RetryNTimes(3, 10));
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
					zk_server, host, SYNC_PROJECT_PATH, SYNC_ENGINE_PATH, SYNC_USER_PATH, e);
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
							// Map<String, String> dataMap = (Map<String, String>) in.readObject();
							Map<String, Object> dataMap = (Map<String, Object>) in.readObject();

							String updatedByNodeId = (String) dataMap.get("nodeId");
							// if the host updated it, then its already ready - other nodes have to pull
							if (!updatedByNodeId.equals(host)) {
								String fullPath = event.getData().getPath();
								classLogger.info(fullPath + " updated, pulling latest data from cloud storage");
								String id;
								boolean pull;
								if (fullPath.startsWith(SYNC_PROJECT_PATH)) {
									String[] path = fullPath.split(SYNC_PROJECT_PATH + "/");
									id = path[1];
									pull = projectLoaded(id);
								} else if (fullPath.startsWith(SYNC_USER_PATH)) {
									String[] path = fullPath.split(SYNC_USER_PATH + "/");
									id = path[1];
									pull = userLoaded(id);
								} else {
									String[] path = fullPath.split(SYNC_ENGINE_PATH + "/");
									id = path[1];
									pull = engineLoaded(id);
								}

								// always check if the engine has been loaded before pulling.

								if (pull) {
									try {
										// actual method param types could be primitives or wrappers
										// params are all Objects (wrappers)
										// invoke method with utility to handle primitive lookups
										@SuppressWarnings("unchecked")
										List<Object> params = (List<Object>) dataMap.get("params");
										MethodUtils.invokeStaticMethod(ClusterUtil.class,
												dataMap.get("methodName").toString(), params.toArray());
									} catch (Exception e) {
										classLogger.error(
												"Failed to process cluster sync update for path='{}', id='{}', method='{}', params='{}'.",
												fullPath, id, dataMap.get("methodName"), dataMap.get("params"), e);
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
	 * Determines whether a project is currently loaded on this node.
	 *
	 * @param projectId project identifier
	 * @return {@code true} if loaded and should be refreshed, otherwise
	 *         {@code false}
	 */
	private static boolean projectLoaded(String projectId) {
		String projects = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS) + "";

		if (projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
				|| projects.endsWith(";" + projectId)) {
			classLogger.info("Loaded project " + projectId + " is out of date. Pulling latest changes");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Determines whether an engine is currently loaded on this node.
	 *
	 * @param engineId engine identifier
	 * @return {@code true} if loaded and should be refreshed, otherwise
	 *         {@code false}
	 */
	private static boolean engineLoaded(String engineId) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";

		if (engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
				|| engines.endsWith(";" + engineId)) {
			classLogger.info("Loaded engine " + engineId + " is out of date. Pulling latest changes");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Determines whether a user asset project is present locally on this node.
	 *
	 * @param projectId user asset project identifier
	 * @return {@code true} if local files indicate loaded state, otherwise
	 *         {@code false}
	 */
	private static boolean userLoaded(String projectId) {
		// User assetss are not registered in DIHelper like projects/engines.
		// Check if the SMSS file exists locally it is only written here by
		// pullUserAssetOrWorkspace, meaning this pod has previously fetched this
		// user's data and may have a stale copy.
		String userFolder = prerna.util.EngineUtility.USER_FOLDER;
		String assetSmss = userFolder + java.io.File.separator
				+ prerna.engine.impl.SmssUtilities.getUniqueName(UserAssetUtils.ASSET_APP_NAME, projectId) + ".smss";
		if (new java.io.File(assetSmss).exists()) {
			classLogger.info("User asset " + projectId + " is out of date. Pulling latest changes");
			return true;
		}
		return false;
	}

	/**
	 * Publishes an engine change event to ZooKeeper so other nodes can pull
	 * updates.
	 *
	 * @param engineId   engine identifier
	 * @param methodName {@code ClusterUtil} static method to execute on peers
	 * @param params     serialized method arguments
	 * @throws Exception if path creation, serialization, or publish fails
	 */
	public void publishEngineChange(String engineId, String methodName, Object... params) throws Exception {
		String enginePath = SYNC_ENGINE_PATH + "/" + engineId;

		// this creates the path if it doesnt exist
		if (client.checkExists().forPath(enginePath) == null) {
			client.create().creatingParentsIfNeeded().forPath(enginePath);
		}

		classLogger.info("Publishing change for engine " + engineId + " and for nodes to " + methodName);
		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put("nodeId", host);
		dataMap.put("methodName", methodName);
		List<Object> paramList = Arrays.asList(params);
		dataMap.put("params", paramList);

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(dataMap);

		client.setData().forPath(enginePath, byteOut.toByteArray());

	}

	/**
	 * Publishes a user asset change event to ZooKeeper so other nodes can pull
	 * updates.
	 *
	 * @param projectId  user asset project identifier
	 * @param methodName {@code ClusterUtil} static method to execute on peers
	 * @param params     serialized method arguments
	 * @throws Exception if path creation, serialization, or publish fails
	 */
	public void publishUserChange(String projectId, String methodName, Object... params) throws Exception {
		String userPath = SYNC_USER_PATH + "/" + projectId;

		// this creates the path if it doesnt exist
		if (client.checkExists().forPath(userPath) == null) {
			client.create().creatingParentsIfNeeded().forPath(userPath);
		}

		classLogger.info("Publishing change for user asset " + projectId + " and for nodes to " + methodName);
		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put("nodeId", host);
		dataMap.put("methodName", methodName);
		List<Object> paramList = Arrays.asList(params);
		dataMap.put("params", paramList);

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(dataMap);

		client.setData().forPath(userPath, byteOut.toByteArray());

	}

	/**
	 * Publishes a project change event to ZooKeeper so other nodes can pull
	 * updates.
	 *
	 * @param projectId  project identifier
	 * @param methodName {@code ClusterUtil} static method to execute on peers
	 * @param params     serialized method arguments
	 * @throws Exception if path creation, serialization, or publish fails
	 */
	public void publishProjectChange(String projectId, String methodName, Object... params) throws Exception {
		String projectPath = SYNC_PROJECT_PATH + "/" + projectId;

		// this creates the path if it doesnt exist
		if (client.checkExists().forPath(projectPath) == null) {
			client.create().creatingParentsIfNeeded().forPath(projectPath);
		}

		classLogger.info("Publishing change for project " + projectId + " and for nodes to " + methodName);
		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put("nodeId", host);
		dataMap.put("methodName", methodName);
		List<Object> paramList = Arrays.asList(params);
		dataMap.put("params", paramList);

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(dataMap);

		client.setData().forPath(projectPath, byteOut.toByteArray());
	}

//	/**
//	 * Manual entrypoint used for long-running local verification of listener
//	 * behavior.
//	 *
//	 * @param args command line args (unused)
//	 */
//	public static void main(String[] args) {
//		try {
//			ClusterSynchronizer instance = ClusterSynchronizer.getInstance();
//			Thread.sleep(Integer.MAX_VALUE);
//		} catch (Exception e) {
//			classLogger.error("ClusterSynchronizer main loop terminated unexpectedly.", e);
//		}
//	}

}
