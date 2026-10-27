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

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import prerna.cluster.sync.IClusterSynchronizer;
import prerna.cluster.sync.impl.ClusterSynchronizerFactory;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.engine.api.IEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DefaultImageGeneratorUtil;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class ClusterUtil {

	private static final Logger classLogger = LogManager.getLogger(ClusterUtil.class);

	private static final String IS_CLUSTER_KEY = "SEMOSS_IS_CLUSTER";
	public static final boolean IS_CLUSTER = (Utility.getDIHelperProperty(IS_CLUSTER_KEY) != null
			&& !(Utility.getDIHelperProperty(IS_CLUSTER_KEY).isEmpty()))
					? Boolean.parseBoolean(Utility.getDIHelperProperty(IS_CLUSTER_KEY))
					: ((System.getenv().containsKey(IS_CLUSTER_KEY))
							? Boolean.parseBoolean(System.getenv(IS_CLUSTER_KEY))
							: false);

	private static final String STORAGE_PROVIDER_KEY = "SEMOSS_STORAGE_PROVIDER";
	public static final String STORAGE_PROVIDER = (Utility.getDIHelperProperty(STORAGE_PROVIDER_KEY) != null
			&& !(Utility.getDIHelperProperty(STORAGE_PROVIDER_KEY).isEmpty()))
					? Utility.getDIHelperProperty(STORAGE_PROVIDER_KEY)
					: System.getenv(STORAGE_PROVIDER_KEY);

	private static final String REMOTE_RSERVE_KEY = "REMOTE_RSERVE";
	public static final boolean REMOTE_RSERVE = (Utility.getDIHelperProperty(REMOTE_RSERVE_KEY) != null
			&& !(Utility.getDIHelperProperty(IS_CLUSTER_KEY).isEmpty()))
					? Boolean.parseBoolean(Utility.getDIHelperProperty(REMOTE_RSERVE_KEY))
					: ((System.getenv().containsKey(REMOTE_RSERVE_KEY))
							? Boolean.parseBoolean(System.getenv(REMOTE_RSERVE_KEY))
							: false);

	private static final String LOAD_ENGINES_LOCALLY_KEY = "SEMOSS_LOAD_ENGINES_LOCALLY";
	public static final boolean LOAD_ENGINES_LOCALLY = (Utility.getDIHelperProperty(LOAD_ENGINES_LOCALLY_KEY) != null
			&& !(Utility.getDIHelperProperty(IS_CLUSTER_KEY).isEmpty()))
					? Boolean.parseBoolean(Utility.getDIHelperProperty(LOAD_ENGINES_LOCALLY_KEY))
					: ((System.getenv().containsKey(LOAD_ENGINES_LOCALLY_KEY))
							? Boolean.parseBoolean(System.getenv(LOAD_ENGINES_LOCALLY_KEY))
							: false);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static String IMAGES_FOLDER_PATH = Utility.getBaseFolder() + DIR_SEPARATOR + "images";
	// A successful folder refresh covers every resource in that catalog. Keep only
	// a short, bounded in-memory record so stock-only cards do not each pull it
	// again.
	private static final Cache<String, Boolean> IMAGE_FOLDER_REFRESHES = CacheBuilder.newBuilder().maximumSize(64)
			.expireAfterWrite(30, TimeUnit.SECONDS).build();
	private static final String SCHEDULER_EXECUTOR_KEY = "SCHEDULER_EXECUTOR";

	private static final String IS_CLUSTERED_SCHEDULER_KEY = "SEMOSS_SCHEDULER_IS_CLUSTER";

	public static final boolean IS_CLUSTERED_SCHEDULER = (Utility
			.getDIHelperProperty(IS_CLUSTERED_SCHEDULER_KEY) != null
			&& !(Utility.getDIHelperProperty(IS_CLUSTERED_SCHEDULER_KEY).isEmpty()))
					? Boolean.parseBoolean(Utility.getDIHelperProperty(IS_CLUSTERED_SCHEDULER_KEY))
					: ((System.getenv().containsKey(IS_CLUSTERED_SCHEDULER_KEY))
							? Boolean.parseBoolean(System.getenv(IS_CLUSTERED_SCHEDULER_KEY))
							: IS_CLUSTER);

	/*
	 * private static final String MULTIPLE_STORAGE_ACCOUNTS_KEY =
	 * "MULTIPLE_STORAGE_ACCOUNTS"; public static final boolean
	 * MULTIPLE_STORAGE_ACCOUNTS =
	 * System.getenv().containsKey(MULTIPLE_STORAGE_ACCOUNTS_KEY) ?
	 * Boolean.parseBoolean(System.getenv(MULTIPLE_STORAGE_ACCOUNTS_KEY)) : false;
	 * 
	 * private static final String MAIN_STORAGE_ACCOUNT_KEY =
	 * "MAIN_STORAGE_ACCOUNT"; public static final String MAIN_STORAGE_ACOUNT =
	 * System.getenv(MAIN_STORAGE_ACCOUNT_KEY);
	 * 
	 * 
	 * //redis table info public static final String REDIS_STORAGE_ACCOUNT =
	 * "storageAccount"; public static final String REDIS_TIMESTAMP = "timestamp";
	 */

	/**
	 * Determines whether this node runs the scheduled jobs. Outside a cluster every
	 * node does. In a cluster the SCHEDULER_EXECUTOR setting decides, read from
	 * RDF_Map and then the environment, and without it the ZooKeeper leader
	 * election does.
	 *
	 * @return true if this node runs the scheduled jobs
	 */
	public static boolean isSchedulerExecutor() {
		if (ClusterUtil.IS_CLUSTER) {
			classLogger.info("Checking if pod is leader");
			// check rdf
			if (Utility.getDIHelperProperty(SCHEDULER_EXECUTOR_KEY) != null
					&& !(Utility.getDIHelperProperty(SCHEDULER_EXECUTOR_KEY).isEmpty())) {
				return Boolean.parseBoolean(Utility.getDIHelperProperty(SCHEDULER_EXECUTOR_KEY));
			}

			// then check env var
			String leader = System.getenv(SCHEDULER_EXECUTOR_KEY);
			if (leader != null && !leader.isEmpty()) {
				return Boolean.parseBoolean(leader);
			}

			// zk
			return SchedulerListener.getListener().isZKLeader();

			// finally dynamic

			// String hostName = System.getenv("HOSTNAME");
			// logger.info("pod host name is " + hostName);
			//
			// if(hostName == null || hostName.isEmpty()) {
			// throw new IllegalArgumentException("Hostname is null or empty along with no
			// reference to scheduler execution in RDF_Map or env vars");
			// }
			// try {
			// // TODO make this dynamic url instead of hard coded
			// JSONObject json = readJsonFromUrl("http://localhost:4040/");
			// String electedLeader = json.get("name").toString();
			// logger.info("elected leader is " + electedLeader);
			//
			// return hostName.equals(electedLeader);
			// } catch (JSONException e) {
			// logger.error(STACKTRACE, e);
			// } catch (IOException e) {
			// logger.error(STACKTRACE, e);
			// }
			// return false;
		} else {
			// if its not clustered, return true to say its a executor
			return true;
		}
	}

	/**
	 * Returns the client for the cluster's central cloud storage, which holds the
	 * engines, projects, insights, rooms, user assets and images every node pulls.
	 *
	 * @return the central cloud storage client
	 * @throws Exception if the client cannot be created from the storage settings
	 */
	public static CentralCloudStorage getCentralStorageClient() throws Exception {
		return CentralCloudStorage.getInstance();
	}

	/**
	 * Convenience accessor for the active cluster synchronizer, delegating to
	 * {@link ClusterSynchronizerFactory#getClusterSynchronizer()}.
	 *
	 * @return the active {@link IClusterSynchronizer} for the configured backend
	 * @throws Exception if no backend is configured or it fails to initialize
	 */
	public static IClusterSynchronizer getClusterSynchronizer() throws Exception {
		return ClusterSynchronizerFactory.getClusterSynchronizer();
	}

	/**
	 * Downloads an engine's folder and smss file from cloud storage to this node,
	 * reading its type from the security database. An engine open on this node
	 * keeps running on its old files; see
	 * {@link #pullEngine(String, IEngine.CATALOG_TYPE, boolean)} to close and
	 * reopen it. Does nothing outside a cluster.
	 *
	 * @param engineId the engine to pull
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullEngine(String engineId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullEngine(engineId);
			} catch (Exception e) {
				classLogger.error("Failed to pull engine '{}' from cloud storage", engineId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull engine '" + engineId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Downloads an engine's folder and smss file from cloud storage to this node.
	 * An engine open on this node keeps running on its old files. Does nothing
	 * outside a cluster.
	 *
	 * @param engineId   the engine to pull
	 * @param engineType the engine's type
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullEngine(String engineId, IEngine.CATALOG_TYPE engineType) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullEngine(engineId, engineType);
			} catch (Exception e) {
				classLogger.error("Failed to pull engine '{}' of type '{}' from cloud storage", engineId, engineType,
						e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull engine '" + engineId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Downloads an engine's folder and smss file from cloud storage to this node.
	 * When the engine is open here, it is closed first, under its engine lock, and
	 * opened again from the new files after, so the running engine picks them up.
	 * Does nothing outside a cluster.
	 *
	 * @param engineId            the engine to pull
	 * @param engineType          the engine's type, or null to read it from the
	 *                            open engine or the security database
	 * @param engineAlreadyLoaded whether the engine is open on this node
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullEngine(String engineId, IEngine.CATALOG_TYPE engineType, boolean engineAlreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullEngine(engineId, engineType, engineAlreadyLoaded);
			} catch (Exception e) {
				classLogger.error("Failed to pull engine '{}' of type '{}' (already loaded: {}) from cloud storage",
						engineId, engineType, engineAlreadyLoaded, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull engine '" + engineId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Uploads an engine's folder and smss file to cloud storage, then tells the
	 * other nodes in the cluster to pull the engine
	 * ({@link ClusterSyncMethod#PULL_ENGINE}). The upload runs only in a cluster,
	 * and the notice only when a cluster synchronizer is set up.
	 *
	 * @param engineId the engine to push
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushEngine(String engineId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushEngine(engineId);
			} catch (Exception e) {
				classLogger.error("Failed to push engine '{}' to cloud storage", engineId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to push engine '" + engineId + "' to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}

		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishEngineChange(engineId, ClusterSyncMethod.PULL_ENGINE, engineId);
			} catch (Exception e) {
				classLogger.error("Failed to publish engine '{}' change to ZK cluster", engineId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish engine '" + engineId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Uploads an engine's smss file to cloud storage after it changed, reading its
	 * type from the security database, then tells the other nodes to pull the
	 * engine ({@link ClusterSyncMethod#PULL_ENGINE}), so a node that has it open
	 * closes it and opens it again with the new smss. The upload runs only in a
	 * cluster, and the notice only when a cluster synchronizer is set up.
	 *
	 * @param engineId the engine whose smss changed
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushEngineSmss(String engineId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushEngineSmss(engineId);
			} catch (Exception e) {
				classLogger.error("Failed to push engine '{}' smss to cloud storage", engineId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to push engine '" + engineId + "'smss to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishEngineChange(engineId, ClusterSyncMethod.PULL_ENGINE, engineId);
			} catch (Exception e) {
				classLogger.error("Failed to publish engine '{}' smss change to ZK cluster", engineId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish engine '" + engineId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Uploads an engine's smss file to cloud storage after it changed, then tells
	 * the other nodes to pull the engine ({@link ClusterSyncMethod#PULL_ENGINE}),
	 * so a node that has it open closes it and opens it again with the new smss.
	 * The upload runs only in a cluster, and the notice only when a cluster
	 * synchronizer is set up.
	 *
	 * @param engineId   the engine whose smss changed
	 * @param engineType the engine's type
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushEngineSmss(String engineId, IEngine.CATALOG_TYPE engineType) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushEngineSmss(engineId, engineType);
			} catch (Exception e) {
				classLogger.error("Failed to push engine '{}' of type '{}' smss to cloud storage", engineId, engineType,
						e);
				SemossPixelException err = new SemossPixelException(
						"Failed to push engine '" + engineId + "'smss to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishEngineChange(engineId, ClusterSyncMethod.PULL_ENGINE, engineId);
			} catch (Exception e) {
				classLogger.error("Failed to publish engine '{}' of type '{}' smss change to ZK cluster", engineId,
						engineType, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish engine '" + engineId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Asks the other nodes in the cluster to close the engine where they have it
	 * open, so each one opens it again from its current files on next use. Runs
	 * through the active cluster synchronizer, ZooKeeper or Redis, and does nothing
	 * when no synchronizer is configured.
	 *
	 * @param engineId the engine to close on the other nodes
	 */
	public static void publishEngineClose(String engineId) {
		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishEngineChange(engineId, ClusterSyncMethod.CLOSE_ENGINE, engineId);
			} catch (Exception e) {
				classLogger.error("Failed to publish the close of engine '{}' to the cluster", engineId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish the close of engine '" + engineId + "' to the other nodes in the cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Deletes an engine's folder and smss file from cloud storage, reading its type
	 * from the security database. Does nothing outside a cluster.
	 *
	 * @param engineId the engine to delete
	 * @throws SemossPixelException if the delete fails, which stops the pixel
	 */
	public static void deleteEngine(String engineId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteEngine(engineId);
			} catch (Exception e) {
				classLogger.error("Failed to delete engine '{}' from cloud storage", engineId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to delete engine '" + engineId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Deletes an engine's folder and smss file from cloud storage. Does nothing
	 * outside a cluster.
	 *
	 * @param engineId   the engine to delete
	 * @param engineType the engine's type
	 * @throws SemossPixelException if the delete fails, which stops the pixel
	 */
	public static void deleteEngine(String engineId, IEngine.CATALOG_TYPE engineType) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteEngine(engineId, engineType);
			} catch (Exception e) {
				classLogger.error("Failed to delete engine '{}' of type '{}' from cloud storage", engineId, engineType,
						e);
				SemossPixelException err = new SemossPixelException(
						"Failed to delete engine '" + engineId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Uploads one file of an engine to cloud storage, at the same path relative to
	 * the engine folder that it has on this node. Does nothing outside a cluster.
	 *
	 * @param engineId   the engine the file belongs to
	 * @param engineType the engine's type
	 * @param filePath   the file's absolute path, inside the engine folder on this
	 *                   node
	 * @throws SemossPixelException if the upload fails, which stops the pixel
	 */
	public static void copyLocalFileToEngineCloudFolder(String engineId, IEngine.CATALOG_TYPE engineType,
			String filePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().copyLocalFileToEngineCloudFolder(engineId, engineType, filePath);
			} catch (Exception e) {
				SemossPixelException err = new SemossPixelException(
						"Failed to copy local file to engine '" + engineId + "' storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Downloads one file of an engine from cloud storage to the given path on this
	 * node, taking it from the same path relative to the engine folder. Does
	 * nothing outside a cluster.
	 *
	 * @param engineId   the engine the file belongs to
	 * @param engineType the engine's type
	 * @param filePath   the absolute path to write the file to, inside the engine
	 *                   folder on this node
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void copyEngineCloudFileToLocalFile(String engineId, IEngine.CATALOG_TYPE engineType,
			String filePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().copyEngineCloudFileToLocalFile(engineId, engineType, filePath);
			} catch (Exception e) {
				SemossPixelException err = new SemossPixelException(
						"Failed to copy storage file from engine '" + engineId + "' to local instance");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Deletes one file of an engine from cloud storage, the one at the same path
	 * relative to the engine folder as the given local path. Does nothing outside a
	 * cluster.
	 *
	 * @param engineId   the engine the file belongs to
	 * @param engineType the engine's type
	 * @param filePath   the file's absolute path inside the engine folder on this
	 *                   node
	 * @throws SemossPixelException if the delete fails, which stops the pixel
	 */
	public static void deleteEngineCloudFile(String engineId, IEngine.CATALOG_TYPE engineType, String filePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteEngineCloudFile(engineId, engineType, filePath);
			} catch (Exception e) {
				SemossPixelException err = new SemossPixelException(
						"Failed to delete storage file in engine '" + engineId + "'");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Downloads the image folder of one catalog type, such as databases, models or
	 * projects, from cloud storage, so this node has every uploaded image of that
	 * type. Does nothing outside a cluster.
	 *
	 * @param engineType the catalog type whose images to pull
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE engineType) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullEngineAndProjectImageFolder(engineType);
			} catch (Exception e) {
				classLogger.error("Failed to pull engine/project image folder for type '{}' from cloud storage",
						engineType, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull database image folder to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Uploads one engine or project image from the local image folder of its
	 * catalog type to cloud storage. Does nothing outside a cluster.
	 *
	 * @param engineType the catalog type the image belongs to
	 * @param fileName   the image's file name in that folder, named by the engine
	 *                   or project id
	 * @throws SemossPixelException if the upload fails, which stops the pixel
	 */
	public static void pushEngineAndProjectImage(IEngine.CATALOG_TYPE engineType, String fileName) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushEngineAndProjectImage(engineType, fileName);
			} catch (Exception e) {
				classLogger.error("Failed to push engine/project image '{}' of type '{}' to cloud storage", fileName,
						engineType, e);
				SemossPixelException err = new SemossPixelException("Failed to push database image to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}

	}

	/**
	 * Deletes one engine or project image from cloud storage. Does nothing outside
	 * a cluster.
	 *
	 * @param engineType the catalog type the image belongs to
	 * @param fileName   the image's file name
	 * @throws SemossPixelException if the delete fails, which stops the pixel
	 */
	public static void deleteEngineAndProjectImage(IEngine.CATALOG_TYPE engineType, String fileName) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteEngineAndProjectImage(engineType, fileName);
			} catch (Exception e) {
				classLogger.error("Failed to delete engine/project image '{}' of type '{}' from cloud storage",
						fileName, engineType, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to delete database image from the cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Deletes every image of an engine or project from cloud storage: the files in
	 * the local image folder of its catalog type whose names start with its id.
	 * Does nothing outside a cluster.
	 *
	 * @param engineType the catalog type the engine or project belongs to
	 * @param engineId   the engine or project id
	 * @throws SemossPixelException if the delete fails, which stops the pixel
	 */
	public static void deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE engineType, String engineId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteEngineAndProjectImageById(engineType, engineId);
			} catch (Exception e) {
				classLogger.error("Failed to delete engine/project image by id '{}' of type '{}' from cloud storage",
						engineId, engineType, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to delete engine/project image from the cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Downloads a project's folder and smss file from cloud storage to this node. A
	 * project open on this node keeps running on its old files; see
	 * {@link #pullProject(String, boolean)} to close and reopen it. Does nothing
	 * outside a cluster.
	 *
	 * @param projectId the project to pull
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullProject(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullProject(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to pull project '{}' from cloud storage", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull project '" + projectId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Downloads a project's folder and smss file from cloud storage to this node.
	 * When the project is open here, it is closed first, under its project lock,
	 * and opened again from the new files after. Does nothing outside a cluster.
	 *
	 * @param projectId            the project to pull
	 * @param projectAlreadyLoaded whether the project is open on this node
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullProject(String projectId, boolean projectAlreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullProject(projectId, projectAlreadyLoaded);
			} catch (Exception e) {
				classLogger.error("Failed to pull project '{}' (already loaded: {}) from cloud storage", projectId,
						projectAlreadyLoaded, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull project '" + projectId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Deletes a project's folder and smss file from cloud storage. Does nothing
	 * outside a cluster.
	 *
	 * @param projectId the project to delete
	 * @throws SemossPixelException if the delete fails, which stops the pixel
	 */
	public static void deleteProject(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteProject(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to delete project '{}' from cloud storage", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to delete project '" + projectId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Downloads a project's insights database from cloud storage, so this node
	 * lists the project's current insights. Does nothing outside a cluster.
	 *
	 * @param projectId the project whose insights database to pull
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullInsightsDB(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullInsightsDB(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to pull project '{}' insight database from cloud storage", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull project '" + projectId + "' insight database from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	/**
	 * Uploads a project's insights database to cloud storage after an insight was
	 * added, changed or removed, then tells the other nodes to pull it
	 * ({@link ClusterSyncMethod#PULL_INSIGHTS_DB}). The upload runs only in a
	 * cluster, and the notice only when a cluster synchronizer is set up.
	 *
	 * @param projectId the project whose insights database to push
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushInsightDB(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushInsightDB(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to push project '{}' insight database to cloud storage", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to push project '" + projectId + "' insight database to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishProjectChange(projectId, ClusterSyncMethod.PULL_INSIGHTS_DB, projectId);
			} catch (Exception e) {
				classLogger.error("Failed to publish project '{}' insight database change to ZK cluster", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish project '" + projectId + "' to sync with ZK cluster to pull insight db");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
		return;
	}

	/**
	 * Downloads a database's OWL file from cloud storage, through a write OWL
	 * engine taken from the database. See {@link #pullOwl(String, WriteOWLEngine)}.
	 *
	 * @param databaseId the database whose OWL to pull
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullOwl(String databaseId) {
		pullOwl(databaseId, null);
	}

	/**
	 * Downloads a database's OWL file and its positions file from cloud storage.
	 * The OWL is closed for the download and reloaded from the new file after. Does
	 * nothing outside a cluster.
	 *
	 * @param databaseId the database whose OWL to pull
	 * @param owlEngine  the write OWL engine to close and reload, or null to take
	 *                   one from the database
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullOwl(String databaseId, WriteOWLEngine owlEngine) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullOwl(databaseId, owlEngine);
			} catch (Exception e) {
				classLogger.error("Failed to pull owl for database '{}' from cloud storage", databaseId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull owl for database '" + databaseId + "' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	/**
	 * Uploads a database's OWL file to cloud storage, through a write OWL engine
	 * taken from the database. See {@link #pushOwl(String, WriteOWLEngine)}.
	 *
	 * @param databaseId the database whose OWL to push
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushOwl(String databaseId) {
		pushOwl(databaseId, null);
	}

	/**
	 * Uploads a database's OWL file and its positions file to cloud storage after
	 * the metadata changed, then tells the other nodes to pull the OWL
	 * ({@link ClusterSyncMethod#PULL_OWL}). The upload runs only in a cluster, and
	 * the notice only when a cluster synchronizer is set up.
	 *
	 * @param databaseId the database whose OWL to push
	 * @param owlEngine  the write OWL engine holding the OWL, or null to take one
	 *                   from the database
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushOwl(String databaseId, WriteOWLEngine owlEngine) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushOwl(databaseId, owlEngine);
			} catch (Exception e) {
				classLogger.error("Failed to push owl for database '{}' to cloud storage", databaseId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to push owl for database '" + databaseId + "' to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishEngineChange(databaseId, ClusterSyncMethod.PULL_OWL, databaseId);
			} catch (Exception e) {
				classLogger.error("Failed to publish owl change for '{}' to ZK cluster", databaseId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish owl change for '" + databaseId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
		return;
	}

	/**
	 * Uploads a project's folder and smss file to cloud storage, then tells the
	 * other nodes to pull the project ({@link ClusterSyncMethod#PULL_PROJECT}), so
	 * a node that has it open closes it and opens it again from the new files. The
	 * upload runs only in a cluster, and the notice only when a cluster
	 * synchronizer is set up.
	 *
	 * @param projectId the project to push
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushProject(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushProject(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to push project '{}' to cloud storage", projectId, e);
				NounMetadata noun = new NounMetadata("Failed to push project '" + projectId + "' to cloud storage",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}

		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishProjectChange(projectId, ClusterSyncMethod.PULL_PROJECT, projectId);
			} catch (Exception e) {
				classLogger.error("Failed to publish project '{}' change to ZK cluster", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish project '" + projectId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Asks the other nodes in the cluster to clear the project modules cached in
	 * the project python process where they have it running, so the next import
	 * reads the current .py files. Runs through the active cluster synchronizer,
	 * ZooKeeper or Redis, and does nothing when no synchronizer is configured.
	 *
	 * @param projectId the project to reload on the other nodes
	 */
	public static void publishProjectReload(String projectId) {
		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishProjectChange(projectId, ClusterSyncMethod.RELOAD_PROJECT, projectId);
			} catch (Exception e) {
				classLogger.error("Failed to publish the reload of project '{}' to the cluster", projectId, e);
				SemossPixelException err = new SemossPixelException("Failed to publish the reload of project '"
						+ projectId + "' to the other nodes in the cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Pushes a project's smss file to cloud storage after it changed, then tells
	 * the other nodes in the cluster to pull the project, so a node that has it
	 * open closes it and opens it again with the new smss.
	 * 
	 * @param projectId the project whose smss changed
	 */
	public static void pushProjectSmss(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushProjectSmss(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to push project '{}' smss to cloud storage", projectId, e);
				NounMetadata noun = new NounMetadata("Failed to push project '" + projectId + "' smss to cloud storage",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}

		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishProjectChange(projectId, ClusterSyncMethod.PULL_PROJECT, projectId);
			} catch (Exception e) {
				classLogger.error("Failed to publish project '{}' smss change to the cluster", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish project '" + projectId + "' to sync with the cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Uploads one folder of a project to cloud storage, then tells the other nodes
	 * to pull it. See {@link #pushProjectFolder(IProject, String, String)}.
	 *
	 * @param project      the project the folder belongs to
	 * @param absolutePath the folder's absolute path, inside the project folder on
	 *                     this node
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushProjectFolder(IProject project, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			pushProjectFolder(project, absolutePath, null);
		}
	}

	/**
	 * Uploads one folder of a project to cloud storage, at the same path relative
	 * to the project folder that it has on this node, then tells the other nodes to
	 * pull it ({@link ClusterSyncMethod#PULL_PROJECT_FOLDER}). Does nothing outside
	 * a cluster.
	 *
	 * @param project      the project the folder belongs to
	 * @param absolutePath the folder's absolute path, or a folder above it, inside
	 *                     the project folder on this node
	 * @param relativePath the folder's path below absolutePath, or null or blank
	 *                     when absolutePath is the folder
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushProjectFolder(IProject project, String absolutePath, String relativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			if (relativePath != null && !(relativePath = relativePath.trim()).isEmpty()) {
				if (absolutePath.endsWith(DIR_SEPARATOR)) {
					absolutePath += relativePath;
				} else {
					absolutePath += DIR_SEPARATOR + relativePath;
				}
			}

			String projectHome = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT,
					project.getProjectId(), project.getProjectName());
			Path projectHomePath = Paths.get(projectHome);
			Path relative = projectHomePath.relativize(Paths.get(absolutePath));
			ClusterUtil.pushProjectFolder(project.getProjectId(), absolutePath, relative.toString());
		}
	}

	/**
	 * Uploads one folder of a project to cloud storage at the given path, then
	 * tells the other nodes to pull it
	 * ({@link ClusterSyncMethod#PULL_PROJECT_FOLDER}) with the same paths. The
	 * upload runs only in a cluster, and the notice only when a cluster
	 * synchronizer is set up.
	 *
	 * @param projectId          the project the folder belongs to
	 * @param absolutePath       the folder's absolute path on this node
	 * @param remoteRelativePath the folder's path in cloud storage, relative to the
	 *                           project
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushProjectFolder(String projectId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushProjectFolder(projectId, absolutePath, remoteRelativePath);
			} catch (Exception e) {
				SemossPixelException err = new SemossPixelException(
						"Failed to push project '" + projectId + "' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishProjectChange(projectId, ClusterSyncMethod.PULL_PROJECT_FOLDER,
						projectId, absolutePath, remoteRelativePath);
			} catch (Exception e) {
				classLogger.error("Failed to publish project folder for '{}' to ZK cluster", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish project folder for '" + projectId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Downloads one folder of a project from cloud storage. See
	 * {@link #pullProjectFolder(IProject, String, String)}.
	 *
	 * @param project      the project the folder belongs to
	 * @param absolutePath the folder's absolute path, inside the project folder on
	 *                     this node
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullProjectFolder(IProject project, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			pullProjectFolder(project, absolutePath, null);
		}
	}

	/**
	 * Downloads one folder of a project from cloud storage, from the same path
	 * relative to the project folder that it has on this node. Does nothing outside
	 * a cluster.
	 *
	 * @param project      the project the folder belongs to
	 * @param absolutePath the folder's absolute path, or a folder above it, inside
	 *                     the project folder on this node
	 * @param relativePath the folder's path below absolutePath, or null or blank
	 *                     when absolutePath is the folder
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullProjectFolder(IProject project, String absolutePath, String relativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			if (relativePath != null && !(relativePath = relativePath.trim()).isEmpty()) {
				if (absolutePath.endsWith(DIR_SEPARATOR)) {
					absolutePath += relativePath;
				} else {
					absolutePath += DIR_SEPARATOR + relativePath;
				}
			}

			String projectHome = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT,
					project.getProjectId(), project.getProjectName());
			Path projectHomePath = Paths.get(projectHome);
			Path relative = projectHomePath.relativize(Paths.get(absolutePath));
			ClusterUtil.pullProjectFolder(project.getProjectId(), absolutePath, relative.toString());
		}
	}

	/**
	 * Downloads one folder of a project from the given path in cloud storage to the
	 * given path on this node. Does nothing outside a cluster.
	 *
	 * @param projectId          the project the folder belongs to
	 * @param absolutePath       the folder's absolute path on this node
	 * @param remoteRelativePath the folder's path in cloud storage, relative to the
	 *                           project
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullProjectFolder(String projectId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullProjectFolder(projectId, absolutePath, remoteRelativePath);
			} catch (Exception e) {
				classLogger.error("Failed to pull project '{}' folder from cloud storage", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull project '" + projectId + "' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Uploads one folder of an engine to cloud storage, then tells the other nodes
	 * to pull it. See {@link #pushEngineFolder(IEngine, String, String)}.
	 *
	 * @param engine       the engine the folder belongs to
	 * @param absolutePath the folder's absolute path, inside the engine folder on
	 *                     this node
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushEngineFolder(IEngine engine, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			pushEngineFolder(engine, absolutePath, null);
		}
	}

	/**
	 * Uploads one folder of an engine to cloud storage, at the same path relative
	 * to the engine folder that it has on this node, then tells the other nodes to
	 * pull it ({@link ClusterSyncMethod#PULL_ENGINE_FOLDER}). The other nodes
	 * replace the files without restarting the engine. Does nothing outside a
	 * cluster.
	 *
	 * @param engine       the engine the folder belongs to
	 * @param absolutePath the folder's absolute path, or a folder above it, inside
	 *                     the engine folder on this node
	 * @param relativePath the folder's path below absolutePath, or null or blank
	 *                     when absolutePath is the folder
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushEngineFolder(IEngine engine, String absolutePath, String relativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			if (relativePath != null && !(relativePath = relativePath.trim()).isEmpty()) {
				if (absolutePath.endsWith(DIR_SEPARATOR)) {
					absolutePath += relativePath;
				} else {
					absolutePath += DIR_SEPARATOR + relativePath;
				}
			}

			String engineHome = EngineUtility.getSpecificEngineBaseFolder(engine.getCatalogType(), engine.getEngineId(),
					engine.getEngineName());
			Path engineHomePath = Paths.get(engineHome);
			Path relative = engineHomePath.relativize(Paths.get(absolutePath));
			ClusterUtil.pushEngineFolder(engine.getEngineId(), absolutePath, relative.toString());
		}
	}

	/**
	 * Uploads one folder of an engine to cloud storage at the given path, then
	 * tells the other nodes to pull it
	 * ({@link ClusterSyncMethod#PULL_ENGINE_FOLDER}) with the same paths. The
	 * upload runs only in a cluster, and the notice only when a cluster
	 * synchronizer is set up.
	 *
	 * @param engineId           the engine the folder belongs to
	 * @param absolutePath       the folder's absolute path on this node
	 * @param remoteRelativePath the folder's path in cloud storage, relative to the
	 *                           engine
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushEngineFolder(String engineId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushEngineFolder(engineId, absolutePath, remoteRelativePath);
			} catch (Exception e) {
				SemossPixelException err = new SemossPixelException("Failed to push engine '" + engineId + "' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishEngineChange(engineId, ClusterSyncMethod.PULL_ENGINE_FOLDER, engineId,
						absolutePath, remoteRelativePath);
			} catch (Exception e) {
				classLogger.error("Failed to publish engine folder for '{}' to ZK cluster", engineId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish engine folder for '" + engineId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Downloads one folder of an engine from cloud storage. See
	 * {@link #pullEngineFolder(IEngine, String, String)}.
	 *
	 * @param engine       the engine the folder belongs to
	 * @param absolutePath the folder's absolute path, inside the engine folder on
	 *                     this node
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullEngineFolder(IEngine engine, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			pullEngineFolder(engine, absolutePath, null);
		}
	}

	/**
	 * Downloads one folder of an engine from cloud storage, from the same path
	 * relative to the engine folder that it has on this node. Does nothing outside
	 * a cluster.
	 *
	 * @param engine       the engine the folder belongs to
	 * @param absolutePath the folder's absolute path, or a folder above it, inside
	 *                     the engine folder on this node
	 * @param relativePath the folder's path below absolutePath, or null or blank
	 *                     when absolutePath is the folder
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullEngineFolder(IEngine engine, String absolutePath, String relativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			if (relativePath != null && !(relativePath = relativePath.trim()).isEmpty()) {
				if (absolutePath.endsWith(DIR_SEPARATOR)) {
					absolutePath += relativePath;
				} else {
					absolutePath += DIR_SEPARATOR + relativePath;
				}
			}

			String engineHome = EngineUtility.getSpecificEngineBaseFolder(engine.getCatalogType(), engine.getEngineId(),
					engine.getEngineName());
			Path engineHomePath = Paths.get(engineHome);
			Path relative = engineHomePath.relativize(Paths.get(absolutePath));
			ClusterUtil.pullEngineFolder(engine.getEngineId(), absolutePath, relative.toString());
		}
	}

	/**
	 * Downloads one folder of an engine from the given path in cloud storage to the
	 * given path on this node. Does nothing outside a cluster.
	 *
	 * @param engineId           the engine the folder belongs to
	 * @param absolutePath       the folder's absolute path on this node
	 * @param remoteRelativePath the folder's path in cloud storage, relative to the
	 *                           engine
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullEngineFolder(String engineId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullEngineFolder(engineId, absolutePath, remoteRelativePath);
			} catch (Exception e) {
				classLogger.error("Failed to pull engine '{}' folder from cloud storage", engineId, e);
				SemossPixelException err = new SemossPixelException("Failed to pull engine '" + engineId + "' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Uploads one insight's folder of a project to cloud storage after the insight
	 * was saved, then tells the other nodes to pull it
	 * ({@link ClusterSyncMethod#PULL_INSIGHT}). The upload runs only in a cluster,
	 * and the notice only when a cluster synchronizer is set up.
	 *
	 * @param projectId the project the insight belongs to
	 * @param rdbmsId   the insight to push
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushInsight(String projectId, String rdbmsId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushInsight(projectId, rdbmsId);
			} catch (Exception e) {
				SemossPixelException err = new SemossPixelException(
						"Failed to push project '" + projectId + "' insight '" + rdbmsId + "' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}

		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishProjectChange(projectId, ClusterSyncMethod.PULL_INSIGHT, projectId,
						rdbmsId);
			} catch (Exception e) {
				classLogger.error("Failed to publish insight '{}' for project '{}' to ZK cluster", rdbmsId, projectId,
						e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish insight folder for '" + projectId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Downloads one insight's folder of a project from cloud storage. Does nothing
	 * outside a cluster.
	 *
	 * @param projectId the project the insight belongs to
	 * @param rdbmsId   the insight to pull
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullInsight(String projectId, String rdbmsId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullInsight(projectId, rdbmsId);
			} catch (Exception e) {
				classLogger.error("Failed to pull insight '{}' for project '{}' from cloud storage", rdbmsId, projectId,
						e);
				SemossPixelException err = new SemossPixelException(
						"Failed to pull project '" + projectId + "' insight '" + rdbmsId + "' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Uploads an insight's image to cloud storage, deleting the image it replaces
	 * there first. Does nothing outside a cluster.
	 *
	 * @param projectId     the project the insight belongs to
	 * @param insightId     the insight the image belongs to
	 * @param oldImageName  the file name of the image it replaces, or null when
	 *                      there is none
	 * @param imageFileName the file name of the new image, in the insight folder on
	 *                      this node
	 * @throws SemossPixelException if the upload fails, which stops the pixel
	 */
	public static void pushInsightImage(String projectId, String insightId, String oldImageName, String imageFileName) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushInsightImage(projectId, insightId, oldImageName, imageFileName);
			} catch (Exception e) {
				classLogger.error("Failed to push insight image for project '{}' insight '{}' to cloud storage",
						projectId, insightId, e);
				SemossPixelException err = new SemossPixelException("Failed to push insight image to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Uploads a chat room's folder to cloud storage and waits for the upload. Does
	 * nothing outside a cluster.
	 *
	 * @param roomId the room to push
	 * @throws SemossPixelException if the upload fails, which stops the pixel
	 */
	public static void pushRoom(String roomId) {
		if (ClusterUtil.IS_CLUSTER) {
			classLogger.info("ClusterUtil.pushRoom - pushing room '{}' to cloud storage", roomId);
			try {
				getCentralStorageClient().pushRoomFolderToCloud(roomId);
			} catch (Exception e) {
				classLogger.error("Failed to push room '{}' to cloud storage", roomId, e);
				SemossPixelException err = new SemossPixelException("Failed to push room to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Uploads a chat room's folder to cloud storage on a virtual thread, without
	 * waiting for it. Nothing waits for the upload, so a failure is only logged on
	 * that thread. Does nothing outside a cluster.
	 *
	 * @param roomId the room to push
	 */
	public static void pushRoomAsync(String roomId) {
		if (ClusterUtil.IS_CLUSTER) {
			Thread.ofVirtual().start(() -> {
				classLogger.info("ClusterUtil.pushRoom - pushing room '{}' to cloud storage", roomId);
				try {
					getCentralStorageClient().pushRoomFolderToCloud(roomId);
				} catch (Exception e) {
					classLogger.error("Failed to push room '{}' to cloud storage", roomId, e);
					SemossPixelException err = new SemossPixelException("Failed to push room to cloud storage");
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			});
		}
	}

	/**
	 * Downloads a chat room's folder from cloud storage. Does nothing outside a
	 * cluster.
	 *
	 * @param roomId the room to pull
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullRoom(String roomId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullRoomFolderFromCloud(roomId);
			} catch (Exception e) {
				classLogger.error("Failed to pull room '{}' from cloud storage", roomId, e);
				SemossPixelException err = new SemossPixelException("Failed to pull room to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/*
	 * 
	 * USER ASSET
	 * 
	 */

	/**
	 * Uploads a user's asset project, which holds the user's own files, to cloud
	 * storage, then tells the other nodes to pull it
	 * ({@link ClusterSyncMethod#PULL_USER_ASSET}). The upload runs only in a
	 * cluster, and the notice only when a cluster synchronizer is set up.
	 *
	 * @param projectId the user's asset project
	 * @throws SemossPixelException if the upload fails, which stops the pixel, or
	 *                              if the other nodes cannot be told, which lets
	 *                              the pixel continue
	 */
	public static void pushUserAsset(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushUserAsset(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to push user asset project '{}' to cloud storage", projectId, e);
				SemossPixelException err = new SemossPixelException("Failed to push user asset project");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}

		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				getClusterSynchronizer().publishUserChange(projectId, ClusterSyncMethod.PULL_USER_ASSET, projectId);
			} catch (Exception e) {
				classLogger.error("Failed to publish user asset project '{}' change to ZK cluster", projectId, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish user asset project '" + projectId + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}

	/**
	 * Downloads a user's asset project from cloud storage, as one that is not open
	 * on this node. See {@link #pullUserAsset(String, boolean)}.
	 *
	 * @param projectId the user's asset project
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullUserAsset(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullUserAsset(projectId, false);
			} catch (Exception e) {
				classLogger.error("Failed to pull user asset project '{}' from cloud storage", projectId, e);
				SemossPixelException err = new SemossPixelException("Failed to pull user asset project");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Downloads a user's asset project from cloud storage. When it is open on this
	 * node, it is closed first and opened again from the new files after. Does
	 * nothing outside a cluster.
	 *
	 * @param projectId     the user's asset project
	 * @param alreadyLoaded whether the asset project is open on this node
	 * @throws SemossPixelException if the download fails, which stops the pixel
	 */
	public static void pullUserAsset(String projectId, boolean alreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullUserAsset(projectId, alreadyLoaded);
			} catch (Exception e) {
				classLogger.error("Failed to pull user asset project '{}' (alreadyLoaded: {}) from cloud storage",
						projectId, alreadyLoaded, e);
				SemossPixelException err = new SemossPixelException("Failed to pull user asset project");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * Returns an engine or project's image, with the default theme for the stock
	 * image. See
	 * {@link #getEngineAndProjectImage(String, IEngine.CATALOG_TYPE, String)}.
	 *
	 * @param engineId   the engine or project id
	 * @param engineType the catalog type the engine or project belongs to
	 * @return the uploaded image, or the shared stock image when there is none
	 * @throws Exception if the image folder cannot be pulled from cloud storage
	 */
	public static File getEngineAndProjectImage(String engineId, IEngine.CATALOG_TYPE engineType) throws Exception {
		return getEngineAndProjectImage(engineId, engineType, null);
	}

	/**
	 * Returns an existing resource image, refreshing the cloud image folder at most
	 * once per 30 seconds when an image is missing. A missing custom image uses the
	 * shared stock file without creating or uploading a per-resource copy. Locally
	 * available uploads take precedence immediately, even during the refresh
	 * window.
	 *
	 * @param engineId   the engine or project id
	 * @param engineType the catalog type the engine or project belongs to
	 * @param theme      the theme of the stock image, used only when the resource
	 *                   has no uploaded image; null for the default
	 * @return the uploaded image, or the shared stock image when there is none
	 * @throws Exception if the image folder cannot be pulled from cloud storage
	 */
	public static File getEngineAndProjectImage(String engineId, IEngine.CATALOG_TYPE engineType, String theme)
			throws Exception {
		File localEngineImageFolder = new File(EngineUtility.getLocalEngineImageDirectory(engineType));
		File image = findEngineAndProjectImage(localEngineImageFolder, engineId);
		if (image != null) {
			return image;
		}

		String folderKey = engineType + ":" + localEngineImageFolder.getAbsolutePath();
		IMAGE_FOLDER_REFRESHES.get(folderKey, () -> {
			getCentralStorageClient().pullEngineAndProjectImageFolder(engineType);
			return Boolean.TRUE;
		});
		image = findEngineAndProjectImage(localEngineImageFolder, engineId);
		if (image != null) {
			return image;
		}
		String imageFilePath = localEngineImageFolder.getAbsolutePath() + DIR_SEPARATOR + engineId + ".png";
		return DefaultImageGeneratorUtil.getStockImageForPath(imageFilePath, theme);
	}

	/**
	 * Finds an engine or project's uploaded image in an image folder.
	 *
	 * @param folder   the image folder of the catalog type
	 * @param engineId the engine or project id
	 * @return the first file whose name contains the id, or null when there is none
	 */
	private static File findEngineAndProjectImage(File folder, String engineId) {
		File[] images = folder.listFiles((dir, name) -> name.contains(engineId));
		return images != null && images.length > 0 ? images[0] : null;
	}

	/**
	 * Adds a hidden placeholder file to every empty folder under the given folder,
	 * at any depth, so the empty folders survive an upload to cloud storage, which
	 * keeps files rather than folders.
	 *
	 * @param folderPath the folder to check
	 */
	public static void validateFolder(String folderPath) {
		List<File> subdirs = getSubdirs(folderPath);
		for (File f : subdirs) {
			if (!(f.list().length > 0)) {
				addHiddenFileToDir(f);
			}
		}
	}

	/**
	 * Lists every folder under a folder, at any depth.
	 *
	 * @param path the folder to list
	 * @return the folders under it, each folder before the ones inside it
	 * @throws IllegalArgumentException if the path is not a folder
	 */
	public static List<File> getSubdirs(String path) {
		File file = new File(path);
		if (!file.isDirectory()) {
			throw new IllegalArgumentException("File path must be a directory");
		}
		List<File> subdirs = Arrays.asList(file.listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				return f.isDirectory();
			}
		}));
		subdirs = new ArrayList<File>(subdirs);

		List<File> deepSubdirs = new ArrayList<File>();
		for (File subdir : subdirs) {
			deepSubdirs.addAll(getSubdirs(subdir.getAbsolutePath()));
		}
		subdirs.addAll(deepSubdirs);
		return subdirs;
	}

	/**
	 * Creates the hidden placeholder file ({@link Constants#HIDDEN_FILE_EXTENSION})
	 * in a folder. A failure to create it is logged.
	 *
	 * @param folder the folder to add it to
	 */
	public static void addHiddenFileToDir(File folder) {
		File hiddenFile = new File(folder.getAbsolutePath() + DIR_SEPARATOR + Constants.HIDDEN_FILE_EXTENSION);
		try {
			hiddenFile.createNewFile();
		} catch (Exception e) {
			classLogger.error("Error creating hidden file in directory {}", folder.getAbsolutePath(), e);
		}
	}

}
