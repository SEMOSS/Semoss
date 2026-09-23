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
package prerna.cluster.sync;

import prerna.cluster.util.ClusterSyncMethod;
import prerna.cluster.util.clients.AppCloudClientProperties;

/**
 * Contract for the backend that coordinates cross-node synchronization of
 * engines, projects, and user assets in a clustered SEMOSS deployment.
 * <p>
 * When one node mutates a shared resource it pushes the artifact to shared
 * cloud storage and then <em>publishes a change event</em> through an
 * implementation of this interface so peer nodes know to pull the latest copy.
 * Implementations wrap a specific coordination technology (e.g. ZooKeeper,
 * Redis); callers obtain the active one via
 * {@link prerna.cluster.sync.impl.ClusterSynchronizerFactory}.
 */
public interface IClusterSynchronizer {

	/** Env/DIHelper flag key that enables the ZooKeeper-backed synchronizer. */
	String SEMOSS_IS_CLUSTER_ZK_KEY = "SEMOSS_IS_CLUSTER_ZK";
	/** Env/DIHelper flag key that enables the Redis-backed synchronizer. */
	String SEMOSS_IS_CLUSTER_REDIS_KEY = "SEMOSS_IS_CLUSTER_REDIS";

	/** Shared accessor for env/DIHelper backed cluster configuration. */
	AppCloudClientProperties CLOUD_PROPS = AppCloudClientProperties.build();
	/**
	 * This node's unique identifier within the cluster. Written as the
	 * {@code nodeId} on every published event so a node can ignore the changes it
	 * published itself.
	 */
	String CONTAINER_IP = CLOUD_PROPS.getContainerIp();

	/**
	 * Publishes a project change so peer nodes refresh their local copy from cloud
	 * storage.
	 *
	 * @param projectId identifier of the project that changed
	 * @param method    the {@link ClusterSyncMethod} peers should run to pull the
	 *                  update
	 * @param params    arguments passed to {@code method} on the receiving node
	 * @throws Exception if the change could not be published
	 */
	void publishProjectChange(String projectId, ClusterSyncMethod method, Object... params) throws Exception;

	/**
	 * Publishes an engine change so peer nodes refresh their local copy from cloud
	 * storage.
	 *
	 * @param engineId identifier of the engine that changed
	 * @param method   the {@link ClusterSyncMethod} peers should run to pull the
	 *                 update
	 * @param params   arguments passed to {@code method} on the receiving node
	 * @throws Exception if the change could not be published
	 */
	void publishEngineChange(String engineId, ClusterSyncMethod method, Object... params) throws Exception;

	/**
	 * Publishes a user asset change so peer nodes refresh their local copy from
	 * cloud storage.
	 *
	 * @param projectId identifier of the user asset project that changed
	 * @param method    the {@link ClusterSyncMethod} peers should run to pull the
	 *                  update
	 * @param params    arguments passed to {@code method} on the receiving node
	 * @throws Exception if the change could not be published
	 */
	void publishUserChange(String projectId, ClusterSyncMethod method, Object... params) throws Exception;

}
