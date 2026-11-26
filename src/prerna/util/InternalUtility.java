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
package prerna.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.UserAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.ZKClient;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.pipeline.EngineCallPermission;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.masterdatabase.AddToMasterDB;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.project.api.IProject;
import prerna.tcp.PayloadStruct;
import prerna.tcp.SocketServerHandler;
import prerna.tcp.workers.EngineSocketWrapper;

/**
 * Helpers that act with the platform's own authority, such as loading and
 * registering a project from its smss, and every read or change of what
 * {@link DIHelper} holds besides a single setting: the RDF_Map settings as a
 * whole, the smss paths and ids of the registered engines and projects, the
 * loaded engines and projects, and the values the desktop tools keep in memory.
 * Only SEMOSS itself calls them: the project reactor sandbox
 * ({@link SemossClassLoader}) refuses this class, so custom reactors, their
 * jars and their Maven dependencies cannot use it. A single setting is read
 * with {@link Utility#getDIHelperProperty(String)}, and general helpers stay in
 * {@link Utility}.
 */
public final class InternalUtility {

	private static final Logger classLogger = LogManager.getLogger(InternalUtility.class);

	private InternalUtility() {

	}

	/**
	 * The RDF_Map settings. This is the Properties object itself, so a change to it
	 * changes the settings. A setting is read with
	 * {@link Utility#getDIHelperProperty(String)}.
	 *
	 * @return the settings
	 */
	public static Properties getCoreProp() {
		return DIHelper.getInstance().getCoreProp();
	}

	/**
	 * Replaces the RDF_Map settings, as start up and tests do.
	 *
	 * @param coreProp the settings
	 */
	public static void setCoreProp(Properties coreProp) {
		DIHelper.getInstance().setCoreProp(coreProp);
	}

	/**
	 * Sets one RDF_Map setting in memory, without writing the file.
	 *
	 * @param name  the setting
	 * @param value its value
	 */
	public static void setDIHelperProperty(String name, String value) {
		DIHelper.getInstance().putProperty(name, value);
	}

	/**
	 * Loads the RDF_Map settings from a file, as start up does.
	 *
	 * @param fileName the RDF_Map file
	 */
	public static void loadCoreProp(String fileName) {
		DIHelper.getInstance().loadCoreProp(fileName);
	}

	/**
	 * The path of the RDF_Map file the settings were loaded from.
	 *
	 * @return the path, or null when the settings were not loaded from a file
	 */
	public static String getRDFMapFileLocation() {
		return DIHelper.getInstance().getRDFMapFileLocation();
	}

	/**
	 * The RDF_Map file's contents with every secret value concealed, for the admin
	 * view of the file.
	 *
	 * @return the contents
	 * @throws IOException when the file cannot be read
	 */
	public static StringBuilder getConcealedRDFMapContents() throws IOException {
		return DIHelper.getInstance().getConcealedRDFMapContents();
	}

	/**
	 * A value the desktop tools and the socket server keep in memory, such as the
	 * main frame or the selected repositories.
	 *
	 * @param key the value's name
	 * @return the value, or null when there is none or DIHelper is not loaded
	 */
	public static Object getLocalProperty(String key) {
		DIHelper helper = DIHelper.getInstance();
		return helper == null ? null : helper.getLocalProp(key);
	}

	/**
	 * Keeps a value in memory under a name, for {@link #getLocalProperty(String)}.
	 *
	 * @param key   the value's name
	 * @param value the value
	 */
	public static void setLocalProperty(String key, Object value) {
		DIHelper.getInstance().setLocalProperty(key, value);
	}

	/**
	 * Forgets a value {@link #setLocalProperty(String, Object)} kept.
	 *
	 * @param key the value's name
	 */
	public static void removeLocalProperty(String key) {
		DIHelper.getInstance().removeLocalProperty(key);
	}

	/**
	 * The path of the smss file this server registered for an engine.
	 *
	 * @param engineId the engine
	 * @return the path, or null when the engine is not registered
	 */
	public static String getEngineSmssPath(String engineId) {
		Object path = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
		return path == null ? null : path.toString();
	}

	/**
	 * Registers the path of an engine's smss file on this server.
	 *
	 * @param engineId the engine
	 * @param smssPath the path of its smss file
	 */
	public static void setEngineSmssPath(String engineId, String smssPath) {
		DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, smssPath);
	}

	/**
	 * The ids of the engines this server has cataloged, separated by {@code ;}.
	 *
	 * @return the ids, or null before start up lists them
	 */
	public static String getEngineIds() {
		Object ids = DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		return ids == null ? null : ids.toString();
	}

	/**
	 * Replaces the ids of the engines this server has cataloged, as start up does.
	 *
	 * @param engineIds the ids, separated by {@code ;}
	 */
	public static void setEngineIds(String engineIds) {
		DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engineIds);
	}

	/**
	 * Adds an engine's id to the ids this server has cataloged, so the file watcher
	 * does not load the engine a second time while it is being created or uploaded.
	 *
	 * @param engineId the engine
	 * @return true when the id was added; false when it was already there
	 */
	public static boolean addEngineId(String engineId) {
		String ids = getEngineIds();
		if (isListed(ids, engineId)) {
			return false;
		}
		setEngineIds(ids == null || ids.isEmpty() ? engineId : ids + ";" + engineId);
		return true;
	}

	/**
	 * The engine object this server loaded for an id.
	 *
	 * @param engineId the engine
	 * @return the engine, or null when it is not loaded
	 */
	public static IEngine getLoadedEngine(String engineId) {
		Object engine = DIHelper.getInstance().getEngineProperty(engineId);
		return engine instanceof IEngine loaded ? loaded : null;
	}

	/**
	 * Forgets the engine object this server loaded for an id, so the next use loads
	 * it again. Its smss path and its id stay registered.
	 *
	 * @param engineId the engine
	 */
	public static void removeLoadedEngine(String engineId) {
		DIHelper.getInstance().removeEngineProperty(engineId);
	}

	/**
	 * The path of the smss file this server registered for a project.
	 *
	 * @param projectId the project
	 * @return the path, or null when the project is not registered
	 */
	public static String getProjectSmssPath(String projectId) {
		Object path = DIHelper.getInstance().getProjectProperty(projectId + "_" + Constants.STORE);
		return path == null ? null : path.toString();
	}

	/**
	 * Registers the path of a project's smss file on this server.
	 *
	 * @param projectId the project
	 * @param smssPath  the path of its smss file
	 */
	public static void setProjectSmssPath(String projectId, String smssPath) {
		DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, smssPath);
	}

	/**
	 * The ids of the projects this server has cataloged, separated by {@code ;}.
	 *
	 * @return the ids, or null before start up lists them
	 */
	public static String getProjectIds() {
		Object ids = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		return ids == null ? null : ids.toString();
	}

	/**
	 * Replaces the ids of the projects this server has cataloged, as start up does.
	 *
	 * @param projectIds the ids, separated by {@code ;}
	 */
	public static void setProjectIds(String projectIds) {
		DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projectIds);
	}

	/**
	 * Adds a project's id to the ids this server has cataloged, so the file watcher
	 * does not load the project a second time while it is being created or
	 * uploaded.
	 *
	 * @param projectId the project
	 * @return true when the id was added; false when it was already there
	 */
	public static boolean addProjectId(String projectId) {
		String ids = getProjectIds();
		if (isListed(ids, projectId)) {
			return false;
		}
		setProjectIds(ids == null || ids.isEmpty() ? projectId : ids + ";" + projectId);
		return true;
	}

	/**
	 * Registers the project object this server loaded for an id.
	 *
	 * @param projectId the project
	 * @param project   the loaded project
	 */
	public static void setLoadedProject(String projectId, IProject project) {
		DIHelper.getInstance().setProjectProperty(projectId, project);
	}

	/**
	 * Registers a project that is being created on this server: the path of its
	 * smss file, the project object and its id, so the file watcher leaves it to
	 * the code creating it.
	 *
	 * @param projectId the project
	 * @param project   the project object
	 * @param smssPath  the path of its smss file so far
	 */
	public static void registerNewProject(String projectId, IProject project, String smssPath) {
		setProjectSmssPath(projectId, smssPath);
		setLoadedProject(projectId, project);
		addProjectId(projectId);
	}

	private static boolean isListed(String ids, String id) {
		if (ids == null || ids.isEmpty()) {
			return false;
		}
		for (String listed : ids.split(";")) {
			if (listed.equals(id)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Loads a project from its smss, registers it with DIHelper and the security
	 * database, and returns it. A project that is already loaded is returned as it
	 * is.
	 *
	 * @param smssFilePath the project's smss file
	 * @param smssProp     the values read from it
	 * @return the project, or null when it could not be loaded
	 */
	static IProject loadProject(String smssFilePath, Properties smssProp) {
		// trim all the values the SMSS file
		for (String name : smssProp.stringPropertyNames()) {
			String value = smssProp.getProperty(name);
			if (value != null) {
				smssProp.setProperty(name, value.trim());
			}
		}

		IProject project = null;
		try {
			String projects = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS) + "";
			String projectId = smssProp.getProperty(Constants.PROJECT);
			String projectClass = smssProp.getProperty(Constants.PROJECT_TYPE);

			if (projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
					|| projects.endsWith(";" + projectId)) {
				classLogger.debug("Project {} is already loaded...", projectId);
				// engines are by default loaded so that we can keep track on the front end of
				// engine/all call so even though it is added here there is a good possibility
				// it is not loaded so check to see this
				if (DIHelper.getInstance().getProjectProperty(projectId) instanceof IProject) {
					return (IProject) DIHelper.getInstance().getProjectProperty(projectId);
				}
			}

			// we store the smss location in DIHelper
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, smssFilePath);

			// create and open the class
			project = (IProject) Class.forName(projectClass).getDeclaredConstructor().newInstance();
			project.setProjectId(projectId);
			project.open(smssFilePath);

			// set the engine in DIHelper
			DIHelper.getInstance().setProjectProperty(projectId, project);

			// Append the engine name to engines if not already present
			if (!(projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
					|| projects.endsWith(";" + projectId))) {
				projects = projects + ";" + projectId;
				DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projects);
			}

			// add the project if not an asset
			if (!project.isAsset()) {
				SecurityProjectUtils.addProject(projectId, null);
			}
		} catch (InstantiationException ie) {
			classLogger.error("Failed to load project from SMSS metadata: {}", ie.getMessage(), ie);
		} catch (IllegalAccessException iae) {
			classLogger.error("Failed to load project from SMSS metadata: {}", iae.getMessage(), iae);
		} catch (ClassNotFoundException cnfe) {
			classLogger.error("Failed to load project from SMSS metadata: {}", cnfe.getMessage(), cnfe);
		} catch (Exception e) {
			classLogger.error("Failed to load project from SMSS metadata: {}", e.getMessage(), e);
		}

		return project;
	}

	/**
	 * Loads an engine from its smss and opens it, and registers it with DIHelper,
	 * the local master database and the security database
	 * 
	 * @param smssFilePath
	 * @param smssProp
	 * @return
	 */
	static IEngine loadEngine(String smssFilePath, Properties smssProp) {
		String engineId = smssProp.getProperty(Constants.ENGINE);
		if (SystemEngineRegistry.isSystemEngine(engineId)) {
			classLogger.error("Blocked attempt to load system engine '{}' through InternalUtility.loadEngine(). "
					+ "Use SystemEngineRegistry.loadSystemEngine() instead.", engineId);
			return null;
		}

		// trim all the values the SMSS file
		for (String name : smssProp.stringPropertyNames()) {
			String value = smssProp.getProperty(name);
			if (value != null) {
				smssProp.setProperty(name, value.trim());
			}
		}

		IEngine engine = null;
		try {
			String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
			engineId = smssProp.getProperty(Constants.ENGINE);
			String engineName = smssProp.getProperty(Constants.ENGINE_ALIAS);
			String engineClass = smssProp.getProperty(Constants.ENGINE_TYPE);

			if (engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
					|| engines.endsWith(";" + engineId)) {
				classLogger.debug("Engine {} is already loaded...", engineId);
				// engines are by default loaded so that we can keep track on the front end of
				// engine/all call so even though it is added here there is a good possibility
				// it is not loaded so check to see this
				if (DIHelper.getInstance().getEngineProperty(engineId) instanceof IEngine) {
					return (IEngine) DIHelper.getInstance().getEngineProperty(engineId);
				}
			}

			// we store the smss location in DIHelper
			if (smssFilePath != null) {
				DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, smssFilePath);
			}

			// create and open the class
			engine = (IEngine) Class.forName(engineClass).getDeclaredConstructor().newInstance();
			engine.setEngineId(engineId);

			// before we open, let us see if we have the assets folder
			File engineAssets = new File(
					EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engineId, engineName));
			boolean hasAssetsFolder = engineAssets.exists() && engineAssets.isDirectory();
			classLogger.info("Engine assets directory for {} exists = {}",
					SmssUtilities.getUniqueName(engineName, engineId), hasAssetsFolder);

			IEngine.CATALOG_TYPE catalogType = engine.getCatalogType();
			try (EngineCallPermission.Opening opening = EngineCallPermission.opening(engineId,
					catalogType == null ? null : catalogType.name())) {
				if (smssFilePath == null) {
					engine.open(smssProp);
				} else {
					engine.open(smssFilePath);
				}
			}
			// set the engine in DIHelper
			DIHelper.getInstance().setEngineProperty(engineId, engine);

			// Append the engine name to engines if not already present
			if (!(engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
					|| engines.endsWith(";" + engineId))) {
				engines = engines + ";" + engineId;
				DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engines);
			}

			if (DIHelper.getInstance().getLocalProp("core") == null
					|| Boolean.parseBoolean(DIHelper.getInstance().getLocalProp("core") + "")) {
				// for database, load into local master as well
				if (engine.getCatalogType() == IEngine.CATALOG_TYPE.DATABASE) {
					if (!SystemDefaultEngines.getDatabaseIgnoreLocalMaster().contains(engineId)) {
						InternalUtility.synchronizeEngineMetadata(engineId);
					}
				}

				// always load into security
				SecurityEngineUtils.addEngine(engineId, null);
			}

			if (!hasAssetsFolder) {
				// we didn't have the assets directory when we started, do we have it now?
				hasAssetsFolder = engineAssets.exists() && engineAssets.isDirectory();
				if (hasAssetsFolder) {
					classLogger.info(
							"After loading engine {} assets directory exists. Sync to cloud storage if enabled",
							SmssUtilities.getUniqueName(engineName, engineId));
					ClusterUtil.pushEngine(engineId);
				} else {
					classLogger.info("After loading engine {} assets directory still does not exist",
							SmssUtilities.getUniqueName(engineName, engineId));
				}
			}
		} catch (Exception e) {
			// null out the engine
			engine = null;
			classLogger.error("Failed to load engine from SMSS metadata: {}", e.getMessage(), e);
		}
		return engine;
	}

	/**
	 * Loads a user asset project by id, pulling it from the cluster first when
	 * needed, and returns the project itself rather than a guarded proxy. It checks
	 * nothing about who asks: callers check the user first.
	 *
	 * @param projectId the user asset project
	 * @return the project, or null when it has no smss file
	 */
	public static IProject getUserAssetProject(String projectId) {
		IProject project = null;

		if (DIHelper.getInstance().getProjectProperty(projectId) != null) {
			project = (IProject) DIHelper.getInstance().getProjectProperty(projectId);
		} else {
			// Acquire the lock on the project, don't want several calls to try and load the
			// engine at the same time
			classLogger.info("Applying lock for user asset project {}", projectId);
			ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
			lock.lock();
			classLogger.info("User asset project {} is locked", projectId);
			try {
				// Need to do a double check here, so if a different thread was waiting for the
				// project to load, it doesn't go through this process again
				if (DIHelper.getInstance().getProjectProperty(projectId) != null) {
					return (IProject) DIHelper.getInstance().getProjectProperty(projectId);
				}

				if (ClusterUtil.IS_CLUSTER) {
					ClusterUtil.pullUserAsset(projectId, false);
				}

				// Now that the project has been pulled, grab the smss file
				String folderName = UserAssetUtils.ASSET_APP_NAME;
				String smssFile = Utility.getDIHelperProperty(Constants.BASE_FOLDER) + "/" + Constants.USER_FOLDER + "/"
						+ SmssUtilities.getUniqueName(folderName, projectId) + ".smss";
				// Start up the project using the details in the smss
				if (smssFile != null && new File(Utility.normalizePath(smssFile)).exists()) {
					// actual load project process
					project = loadProject(smssFile, Utility.loadProperties(Utility.normalizePath(smssFile)));
				} else {
					classLogger.debug("There is no SMSS File for the user asset project {}...", projectId);
				}
			} finally {
				lock.unlock();
				classLogger.info("User asset project {} is unlocked", projectId);
			}
		}

		return project;
	}

	/**
	 * Returns a model for the platform's own use: a feature that uses the model a
	 * platform setting names, for every user of the feature, whether or not they
	 * have access to the model. The access check on the returned proxy lets the
	 * platform's own calls through, where {@link Utility#getModel(String)} checks
	 * the signed in user. The model's pipeline and usage limits apply as on any
	 * call. Use it only for an engine id that comes from a platform setting, never
	 * for one a user sends.
	 *
	 * @param engineId the id of the model the platform setting names
	 * @return the model, or null when it cannot be loaded
	 */
	public static IModelEngine getPlatformModel(String engineId) {
		return EngineProxyFactory.createPlatformModelEngine((IModelEngine) baseGetEngine(engineId, true));
	}

	/**
	 * Returns a loaded engine, or pulls it from cloud storage when needed and loads
	 * it from its smss. This is the engine itself, before {@link Utility} wraps it
	 * in its proxy, so only the {@link Utility} engine getters call it.
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	static IEngine baseGetEngine(String engineId, boolean pullIfNeeded) {
		IEngine engine = null;

		String smssFile = null;
		boolean reloadDB = false;
		Properties prop = null;

		boolean runningInCore = DIHelper.getInstance().getLocalProp("core") == null
				|| Boolean.parseBoolean(DIHelper.getInstance().getLocalProp("core") + "");

		if (runningInCore) {
			// not sure why we need this after the first time but hey
			smssFile = (String) DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
		} else {
			// on the socket side
			// it will pull the smss
			// and reload the engine
			// once reloaded it will be present in the DI Helper
			// check DI Helper to see if this is needed
			// if not try to figure if reload is required
			if (DIHelper.getInstance().getEngineProperty(engineId) == null) // if already loaded.. no need to load again
			{
				prop = getEngineDetails(engineId);
				if (prop != null) {
					reloadDB = true;
				}
			} else {
				// this is already a loaded engine
				// engine socket wrapper is not persisted in the cache so we are all set here
				reloadDB = true;
			}
		}

		if (runningInCore || reloadDB) {
			// If the engine has already been loaded, then return it
			// Don't acquire the lock here, because that would slow things down
			if (DIHelper.getInstance().getEngineProperty(engineId) != null) {
				engine = (IEngine) DIHelper.getInstance().getEngineProperty(engineId);
			} else {
				// Acquire the lock on the engine, don't want several calls to try and load the
				// engine at the same time
				classLogger.info("Applying lock for engine {} to pull", Utility.cleanLogString(engineId));
				ReentrantLock lock = null;
				try {
					lock = EngineSyncUtility.getEngineLock(engineId);
					lock.lock();
					classLogger.info("Engine {} is locked", Utility.cleanLogString(engineId));

					// Need to do a double check here,
					// so if a different thread was waiting for the engine to load,
					// it doesn't go through this process again
					if (DIHelper.getInstance().getEngineProperty(engineId) != null) {
						return (IEngine) DIHelper.getInstance().getEngineProperty(engineId);
					}

					if (pullIfNeeded && ClusterUtil.IS_CLUSTER) {
						ClusterUtil.pullEngine(engineId);
					}

					// Now that the engine has been pulled, grab the smss file
					smssFile = (String) DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);

					// Start up the engine using the details in the smss
					if (smssFile != null) {
						// actual load engine process
						engine = loadEngine(smssFile, Utility.loadProperties(smssFile));
					} else if (prop != null) {
						engine = loadEngine(null, prop);
					} else {
						classLogger.info("There is no SMSS File for the engine {}...",
								Utility.cleanLogString(engineId));
					}

					// add start with check because the insights RDBMS has the id
					// security_InsightsRDBMS
					if (!SystemDefaultEngines.valueStartsWith(engineId,
							SystemDefaultEngines.getDatabaseIgnoreSecurity())) {
						Map<String, String> envMap = System.getenv();
						if (envMap.containsKey(ZKClient.ZK_SERVER)
								|| envMap.containsKey(ZKClient.ZK_SERVER.toUpperCase())) {
							if (ClusterUtil.LOAD_ENGINES_LOCALLY) {
								// Only publish if actually loading on this box
								// TODO >>>timb: this logic only works insofar as we are assuming a user-based
								// docker layer in addition to the app containers
								String host = "unknown";

								if (envMap.containsKey(ZKClient.HOST)) {
									host = envMap.get(ZKClient.HOST);
								}

								if (envMap.containsKey(ZKClient.HOST.toUpperCase())) {
									host = envMap.get(ZKClient.HOST.toUpperCase());
								}

								// we are in business
								ZKClient client = ZKClient.getInstance();
								client.publishDB(engineId + "@" + host);
							}
						}
					}
				} finally {
					if (lock != null) {
						lock.unlock();
						classLogger.info("Engine {} is unlocked", Utility.cleanLogString(engineId));
					}
				}
			}
		} else {
			// send the information of engine to the smssfile to the socket
			// this is happening on the socket side
			engine = new EngineSocketWrapper(engineId,
					(SocketServerHandler) DIHelper.getInstance().getLocalProp("SSH"));
		}
		return engine;
	}

	/**
	 * Changes a value within the properties file for a given key
	 * 
	 * @param filePath
	 * @param keyToAlter
	 * @param valueToProvide
	 * @throws IOException
	 */
	public static void changePropertiesFileValue(String filePath, String keyToAlter, String valueToProvide)
			throws IOException {
		changePropertiesFileValue(filePath, keyToAlter, valueToProvide, false);
	}

	/**
	 * Changes the value for one key in a properties file, adding the key at the end
	 * when no line has it.
	 * 
	 * @param filePath
	 * @param keyToAlter
	 * @param valueToProvide
	 * @param contains
	 * @throws IOException
	 */
	public static void changePropertiesFileValue(String filePath, String keyToAlter, String valueToProvide,
			boolean contains) throws IOException {
		Map<String, String> keyToNewValue = new HashMap<>();
		keyToNewValue.put(keyToAlter, valueToProvide);
		changePropertiesFileValue(filePath, keyToNewValue, contains);
	}

	/**
	 * Changes the values for several keys in a properties file. A line for a key is
	 * rewritten as the key, a tab and the new value; a key no line has is added at
	 * the end. With {@code contains}, a line matches when it contains the key,
	 * otherwise when it starts with the key and a tab or space.
	 * 
	 * @param filePath
	 * @param keyToNewValue
	 * @param contains
	 * @throws IOException
	 */
	public static void changePropertiesFileValue(String filePath, Map<String, String> keyToNewValue, boolean contains)
			throws IOException {
		File file = new File(filePath);

		/*
		 * 1) Loop through the smss file and add each line as a list of strings 2) For
		 * each line, see if it starts with the key to alter 3) if yes, write out the
		 * key with the new value passed in 4) if no, just write out the line as is
		 * 
		 */
		List<String> content = new ArrayList<>();
		Set<String> updatedKeys = new HashSet<>();
		try (FileReader fr = new FileReader(file); BufferedReader reader = new BufferedReader(fr);) {
			String line;
			// 1) add each line as a different string in list
			while ((line = reader.readLine()) != null) {
				content.add(line);
			}
		} catch (IOException ioe) {
			classLogger.error("Failed to update properties file values: {}", ioe.getMessage(), ioe);
			throw ioe;
		}

		try (FileOutputStream fileOut = new FileOutputStream(file);) {
			byte[] lineBreak = "\n".getBytes();
			// 2) iterate through each line if the smss file
			for (int i = 0; i < content.size(); i++) {

				// separate out logic for contains vs exact match
				if (contains) {
					boolean updated = false;
					// 3) if this line starts with the key to alter
					FOUND_LOOP: for (String keyToAlter : keyToNewValue.keySet()) {
						if (content.get(i).contains(keyToAlter)) {
							// create new line to write using the key and the new value
							String newKeyValue = keyToAlter + "\t" + keyToNewValue.get(keyToAlter);
							fileOut.write(newKeyValue.getBytes());
							updated = true;
							updatedKeys.add(keyToAlter);
							break FOUND_LOOP;
						}
					}

					// 4) if it doesn't, just write the next line as is
					if (!updated) {
						byte[] contentInBytes = content.get(i).getBytes();
						fileOut.write(contentInBytes);
					}
					// after each line, write a line break into the file
					fileOut.write(lineBreak);
				} else {
					boolean updated = false;
					for (String keyToAlter : keyToNewValue.keySet()) {
						FOUND_LOOP: if (content.get(i).startsWith(keyToAlter + "\t")
								|| content.get(i).startsWith(keyToAlter + " ")) {
							// create new line to write using the key and the new value
							String newKeyValue = keyToAlter + "\t" + keyToNewValue.get(keyToAlter);
							fileOut.write(newKeyValue.getBytes());
							updated = true;
							updatedKeys.add(keyToAlter);
							break FOUND_LOOP;
						}
					}

					// 4) if it doesn't, just write the next line as is
					if (!updated) {
						byte[] contentInBytes = content.get(i).getBytes();
						fileOut.write(contentInBytes);
					}
					// after each line, write a line break into the file
					fileOut.write(lineBreak);
				}
			}

			// if we have not updated the key
			// we will append at the end
			for (String key : keyToNewValue.keySet()) {
				if (!updatedKeys.contains(key)) {
					String newKeyValue = key + "\t" + keyToNewValue.get(key);
					fileOut.write(newKeyValue.getBytes());
					fileOut.write(lineBreak);
				}
			}
		} catch (IOException ioe) {
			classLogger.error("Failed to update properties file values: {}", ioe.getMessage(), ioe);
			throw ioe;
		}
	}

	/**
	 * Adds a new key-value pair into a properties file
	 * 
	 * @param propertiesFile
	 * @param locInFile
	 * @param mods
	 * @throws IOException
	 */
	public static void addKeysAtLocationIntoPropertiesFile(String propertiesFile, String locInFile,
			Map<String, String> mods) throws IOException {
		File file = new File(propertiesFile);

		/*
		 * 1) Loop through the properties file and add each line as a list of strings 2)
		 * iterate through the list of strings and write out each line 3) if the current
		 * line being printed starts with locInFile then the new key-value pair will be
		 * written right after it 4) if locInFile was never found, print at end of file
		 */

		List<String> content = new ArrayList<>();
		try (FileReader fr = new FileReader(file);
				BufferedReader reader = new BufferedReader(fr);
				FileOutputStream fileOut = new FileOutputStream(file);) {
			String line;
			// 1) add each line as a different string in list
			while ((line = reader.readLine()) != null) {
				content.add(line);
			}

			boolean found = false;
			for (int i = 0; i < content.size(); i++) {
				// 2) write out each line into the file
				byte[] contentInBytes = content.get(i).getBytes();
				fileOut.write(contentInBytes);
				fileOut.write("\n".getBytes());

				// 3) if the last line printed matches that in locInFile, then write the new
				// key-value pair after
				if (content.get(i).startsWith(locInFile + "\t") || content.get(i).startsWith(locInFile + " ")) {
					found = true;
					for (String keyToAdd : mods.keySet()) {
						String newProp = keyToAdd + "\t" + mods.get(keyToAdd);
						fileOut.write(newProp.getBytes());
						fileOut.write("\n".getBytes());
					}
				}
			}

			if (!found) {
				fileOut.write("\n".getBytes());
				for (String keyToAdd : mods.keySet()) {
					String newProp = keyToAdd + "\t" + mods.get(keyToAdd);
					fileOut.write(newProp.getBytes());
					fileOut.write("\n".getBytes());
				}
			}
		} catch (IOException ioe) {
			classLogger.error("Failed to update properties file values: {}", ioe.getMessage(), ioe);
			throw ioe;
		}
	}

	/**
	 * Registers the engine an smss file describes, unless it is registered already:
	 * reads its id from the file, records the smss path and adds the engine to the
	 * security database, and a database's metadata to the local master database, as
	 * the platform's own work with no user. The engine watchers call this at start
	 * up and the cluster calls it after pulling an engine.
	 *
	 * @param newFile       the smss file name
	 * @param folderToWatch the folder that holds it
	 * @return the engine id, or null when the file cannot be read
	 */
	public static String catalogEngine(String newFile, String folderToWatch) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
		String engineId = null;
		try {
			Properties prop = Utility
					.loadProperties(Utility.normalizePath(folderToWatch) + "/" + Utility.normalizePath(newFile));
			if (prop == null) {
				throw new NullPointerException("Unable to find/load properties file '" + newFile + "'");
			}
			engineId = prop.getProperty(Constants.ENGINE);
			if (engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
					|| engines.endsWith(";" + engineId)) {
				classLogger.debug("Engine {}<>{} is already loaded...", folderToWatch, newFile);
			} else {
				String filePath = folderToWatch + "/" + newFile;
				catalogEngineByType(filePath, prop, engineId);
			}
		} catch (Exception e) {
			classLogger.error("Failed to catalog engine from SMSS file {} in {}", newFile, folderToWatch, e);
		}

		return engineId;
	}

	/**
	 * Registers the project an smss file describes as a private project, unless it
	 * is registered already.
	 *
	 * @param newFile       the smss file name
	 * @param folderToWatch the folder that holds it
	 * @return the project id, or null when the file cannot be read
	 * @see #catalogProject(String, String, boolean)
	 */
	public static String catalogProject(String newFile, String folderToWatch) {
		return catalogProject(newFile, folderToWatch, false);
	}

	/**
	 * Registers the project an smss file describes, unless it is registered
	 * already: reads its id from the file, records the smss path and the project
	 * id, and adds the project to the security database, public or not, as the
	 * platform's own work with no user. The project watcher calls this at start up,
	 * public for the platform apps, and the cluster calls it after pulling a
	 * project.
	 *
	 * @param newFile       the smss file name
	 * @param folderToWatch the folder that holds it
	 * @param global        whether the project is public
	 * @return the project id, or null when the file cannot be read
	 */
	public static String catalogProject(String newFile, String folderToWatch, boolean global) {
		String projects = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS) + "";
		String projectId = null;
		try {
			String smssFile = Utility.normalizePath(folderToWatch) + "/" + Utility.normalizePath(newFile);
			Properties prop = Utility.loadProperties(smssFile);

			projectId = prop.getProperty(Constants.PROJECT);

			if (projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
					|| projects.endsWith(";" + projectId)) {
				classLogger.debug("Project {}<>{} is already loaded...", folderToWatch, newFile);
			} else {
				String fileName = folderToWatch + "/" + newFile;
				DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, fileName);

				String projectNames = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
				if (!(projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
						|| projects.endsWith(";" + projectId))) {
					projectNames = projectNames + ";" + projectId;
					DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projectNames);
				}

				SecurityProjectUtils.addProject(projectId, global, null);
			}
		} catch (Exception e) {
			classLogger.error("Failed to catalog project from smss file {}/{}", folderToWatch, newFile, e);
		}

		return projectId;
	}

	/**
	 * Registers an engine in DIHelper from its smss (its smss path, type and id),
	 * syncs a database engine's metadata to the local master database, and adds the
	 * engine to the security database.
	 * 
	 * @param smssFilePath
	 * @param smssProp
	 * @param engineId
	 */
	public static void catalogEngineByType(String smssFilePath, Properties smssProp, String engineId) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";

		boolean syncToLocalMaster = false;

		DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, smssFilePath);
		IEngine.CATALOG_TYPE engineType = null;
		String rawType = smssProp.get(Constants.ENGINE_TYPE).toString();
		try {
			IEngine emptyClass = (IEngine) Class.forName(rawType).getDeclaredConstructor().newInstance();
			engineType = emptyClass.getCatalogType();
			// FOR NOW
			// PGVECTOR IS A DATABASE ENGINE
			// BUT NO OWL
			if (emptyClass instanceof IDatabaseEngine && !(emptyClass instanceof IVectorDatabaseEngine)) {
				syncToLocalMaster = true;
			}
		} catch (Exception e) {
			classLogger.error("Unknown class name '{}' in smss file '{}': {}", rawType, smssFilePath, e.getMessage(),
					e);
		}
		if (engineType == null) {
			return;
		}

		DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.TYPE, engineType);
		String engineNames = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		if (!(engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
				|| engines.endsWith(";" + engineId))) {
			engineNames = engineNames + ";" + engineId;
			DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engineNames);
		}

		classLogger.info("Loading engine {} of type = {}", engineId, engineType);
		if (syncToLocalMaster) {
			// sync up the engine metadata now
			synchronizeEngineMetadata(engineId);
		}
		SecurityEngineUtils.addEngine(engineId, null);
	}

	/**
	 * Adds a database engine's metadata to the local master database when the local
	 * master's copy is missing or differs from the engine's OWL file.
	 *
	 * @param engineId the engine id
	 */
	public static void synchronizeEngineMetadata(String engineId) {
		/*
		 * This determines when it is necessary to add an engine into the local master
		 * database
		 * 
		 * Logical Flow
		 * 
		 * 1) Get timestamp of the engine within local master -> call this time_local 2)
		 * Get timestamp of last engine update, stored on engine's OWL file -> call this
		 * time_engine 3) If time_local is equal to time_engine, local master is up to
		 * date 4) In all other conditions, add the engine to the local master -> all
		 * other conditions means if either time_local or time_engine are null or if
		 * they do not equal
		 * 
		 * Note: that after the local master has the engine added, the timestamp in the
		 * local master is set to be equal to that in the engine's OWL file (if the
		 * engine's OWL file time stamp is null, it is set to the time when this routine
		 * started running)
		 */

		// generate the appropriate query to execute on the local master engine to get
		// the time stamp
		String smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE) + "";

		// this has all the details
		// the engine file is primarily the SMSS that is going to be utilized for the
		// purposes of retrieving all the data
		Properties prop = Utility.loadProperties(smssFile);

		String rawType = prop.get(Constants.ENGINE_TYPE).toString();
		if (rawType.contains("AppEngine")) {
			// this engine has no data! it is just a collection of insights
			// nothing to synchronize into local master
			return;
		}

		// TODO: NEED TO STILL BUILD THIS OUT!
		if (rawType.contains("RemoteSemossEngine")) {
			return;
		}

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date rdbmsDate = MasterDatabaseUtility.getEngineDate(engineId);
		File owlFile = SmssUtilities.getOwlFile(smssFile, prop);
		if (owlFile == null) {
			classLogger.warn("Engine {} does not have an OWL file", SmssUtilities.getUniqueName(prop));
			return;
		}
		// block for removal in future version
		// required for assets in engine
		// since we might not have the engine loaded yet
		{
			if (!owlFile.exists()) {
				// try the old location
				String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
				String engineBaseFolder = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE,
						engineId, engineName);
				String legacyLocation = engineBaseFolder + "/" + owlFile.getName();
				File legacyOwlFile = new File(legacyLocation);
				if (legacyOwlFile.exists() && legacyOwlFile.isFile()) {
					owlFile = legacyOwlFile;
				}
			}
		}
		String engineDbTime = df.format(new Date(owlFile.lastModified()));

		// 4) perform the necessary additions if the time stamps do not equal
		// this is broken out into 2 separate parts
		// 4.1) the local master doesn't have a time stamp
		// which means the engine is not present
		// -> i.e. we do not need to remove the engine and re-add it
		// 4.2) the time is present and we need to remove anything relating the engine
		// that was in the engine and then re-add it
		String engineRdbmsDbTime = "Dummy";
		if (rdbmsDate != null) {
			engineRdbmsDbTime = df.format(rdbmsDate);
		}

		if (rdbmsDate == null) {
			AddToMasterDB adder = new AddToMasterDB();
			// logic to register the engine into the local master
			adder.registerEngineLocal(smssFile, prop);
		} else if (!engineRdbmsDbTime.equalsIgnoreCase(engineDbTime)) {
			// if it has a time stamp, it means it was previously in local master
			// logic to delete an engine from the local master
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			remover.deleteEngineRDBMS(engineId);
			// logic to add the engine into the local master
			AddToMasterDB adder = new AddToMasterDB();
			adder.registerEngineLocal(smssFile, prop);
		}

		// clear the caching of engine metadata
		EngineSyncUtility.clearEngineCache(engineId);
	}

	/**
	 * Reads an engine's original smss values, and its OWL, through the socket
	 * server this process serves engines from.
	 * 
	 * @param engineId the engine id
	 * @return the smss values, or null when they cannot be read
	 */
	public static Properties getEngineDetails(String engineId) {
		// get the engine properties file
		// get the engine owl file
		// set the owl file location and start up

		CaseInsensitiveProperties prop = null;
		try {
			SocketServerHandler ssh = (SocketServerHandler) DIHelper.getInstance().getLocalProp("SSH");
			PayloadStruct ps = new PayloadStruct();
			ps.epoc = "t1";
			ps.operation = ps.operation.ENGINE;
			ps.objId = engineId;
			ps.methodName = "getOrigProp";
			ps.longRunning = true;
			ps.response = false;
			ps.payloadClasses = new Class[] {};

			PayloadStruct response = ssh.writeResponse(ps);
			prop = (CaseInsensitiveProperties) response.payload[0];

			// get the owl file and replace into properties
			ps = new PayloadStruct();
			ps.epoc = "t2";
			ps.operation = ps.operation.ENGINE;
			ps.objId = engineId;
			ps.methodName = "getOwl";
			ps.longRunning = true;
			ps.response = false;
			ps.payloadClasses = new Class[] {};

			response = ssh.writeResponse(ps);
			String owl = response.payload[0] + "";

			// find if the properties has a reload db on it
			boolean reload = false;
			if (prop.containsKey(Settings.LOAD_DB_ON_SOCKET)) {
				reload = prop.getProperty(Settings.LOAD_DB_ON_SOCKET).equalsIgnoreCase("True");
			}
			if (reload) {
				// write the owl file
				// replace it in the properties
				// write the properties file or not
				// return the properties
				String baseFolder = Utility.getDIHelperProperty(Constants.BASE_FOLDER);
				File engineDir = new File(Utility.childPath(
						Utility.normalizeParam(baseFolder) + File.separator + "engines", engineId, "engine id"));
				if (!engineDir.exists()) {
					engineDir.mkdirs();
				}

				File owlFile = new File(Utility.childPath(engineDir.getAbsolutePath(), engineId + ".owl", "engine id"));
				// engine owlFileName
				FileUtils.writeStringToFile(owlFile, owl);
				prop.replace(Constants.OWL, owlFile.getAbsolutePath());

				// give the properties back
				// write the properties file as well
				// File propFile = new File(engineDir.getAbsolutePath() + File.separator +
				// engineId + ".smss");
				// write the propfile also into it
				// prop.put("PROP_FILE_LOCATION", propFile.getAbsolutePath());
				// FileWriter fw = new FileWriter(propFile);
//				prop.list(new PrintWriter(fw));
//				fw.flush();
//				fw.close();
			}
		} catch (Exception ex) {
			classLogger.error("Failed to read engine metadata from SMSS file: {}", ex.getMessage(), ex);
		}

		return prop;
	}

	/**
	 * Builds the classpath for a spawned Java worker: the insight folder, the
	 * webapp's {@code WEB-INF/lib/*} and each class folder on the webapp class
	 * loader, such as {@code WEB-INF/classes}.
	 *
	 * @param insightFolder the worker's working directory, placed first on the
	 *                      classpath
	 * @return the classpath in double quotes, joined with the platform path
	 *         separator
	 */
	public static String getCP(String insightFolder) {
		boolean win = System.getProperty("os.name").toLowerCase().contains("win");
		List<String> entries = new ArrayList<>();
		entries.add(insightFolder);

		boolean webinfLibAdded = false;
		URL[] urls = ((URLClassLoader) Utility.class.getClassLoader()).getURLs();
		for (URL url : urls) {
			String path = URLDecoder.decode(url.getFile().replaceFirst("/", ""), StandardCharsets.UTF_8);
			if (!win) {
				path = "/" + path;
			}
			if (path.endsWith(".jar")) {
				if (!webinfLibAdded && path.contains("WEB-INF/lib")) {
					entries.add(path.substring(0, path.lastIndexOf('/')) + "/*");
					webinfLibAdded = true;
				}
			} else if (!path.contains("WEB-INF/lib")) {
				entries.add(path.endsWith("/") ? path.substring(0, path.length() - 1) : path);
			}
		}
		return "\"" + String.join(win ? ";" : ":", entries) + "\"";
	}

	/**
	 * Compiles the Java files under {@code folder/java}, skipping hidden folders,
	 * into {@code folder/classes}.
	 *
	 * @param folder    the project or insight folder
	 * @param classpath the classpath to compile against
	 * @return the compiler's status, or -1 when there is nothing to compile
	 */
	public static int compileJava(String folder, String classpath) {
		int status = -1;

		String javaFolder = folder + "/java";
		Path path = Paths.get(Utility.normalizePath(javaFolder));

		if (Files.isDirectory(path)) {
			classLogger.info("Compiling Java in Folder {}", javaFolder);
			try (Stream<Path> p = Files.walk(path, FileVisitOption.FOLLOW_LINKS)) {
				List<File> files = p.filter(Files::isRegularFile).map(Path::toAbsolutePath)
						.filter(InternalUtility::isJavaFile).filter(s -> !fileInRelativeHiddenDirectory(s, path))
						.map(Path::toFile).collect(Collectors.toList());

				if (files.size() > 0) {
					status = compileJava(files, folder, classpath);
				}
			} catch (IOException e) {
				classLogger.error("Failed to compile Java source files: {}", e.getMessage(), e);
			}
			classLogger.info("Done compiling Java in Folder {}", javaFolder);
		}

		return status;
	}

	private static int compileJava(List<File> files, String folder, String classpath) throws IOException {
		String outputFolder = folder + "/classes";
		Files.createDirectories(Paths.get(Utility.normalizePath(outputFolder)));

		List<String> options = new ArrayList<>();
		options.add("-d");
		options.add(outputFolder);
		options.add("-proc:none");
		options.add("-g:source,lines,vars");
		options.add("-Xlint:all");

		DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		if (compiler == null) {
			throw new NullPointerException("Could not find the java compiler");
		}

		StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);
		// add the clss path to the file manager
		String cp = classpath;
		// remove quotes if they exist
		if (cp.startsWith("\"") && cp.endsWith("\"")) {
			cp = cp.substring(1, cp.length() - 1);
		}
		List<File> classpathFiles = Arrays.stream(cp.split(File.pathSeparator)).map(File::new).filter(File::exists)
				.collect(Collectors.toList());
		try {
			fileManager.setLocation(StandardLocation.CLASS_PATH, classpathFiles);
		} catch (IOException e) {
			throw new RuntimeException("Failed to set classpath", e);
		}

		Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromFiles(files);

		Path error = Paths.get(Utility.normalizePath(outputFolder), "compileerror.out");
		Files.deleteIfExists(error);

		try (OutputStream os = Files.newOutputStream(error)) {
			return compileJava(os, compiler, fileManager, diagnostics, options, compilationUnits,
					Paths.get(Utility.normalizePath(outputFolder)), files);
		}
	}

	private static int compileJava(OutputStream os, JavaCompiler compiler, StandardJavaFileManager fileManager,
			DiagnosticCollector<JavaFileObject> diagnostics, List<String> options,
			Iterable<? extends JavaFileObject> compilationUnits, Path classesFolder, List<File> sources)
			throws IOException {

		try (fileManager;
				PrintWriter pw = new PrintWriter(
						new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)))) {
			boolean hold = compiler.getTask(pw, fileManager, diagnostics, options, null, compilationUnits).call();

			pw.flush();
			logDiagnostics(diagnostics.getDiagnostics(), pw);
			if (hold) {
				logSandboxNotices(classesFolder, sources, pw);
			}

			return hold ? 0 : -1;
		}
	}

	private static boolean fileInRelativeHiddenDirectory(Path file, Path folder) {
		do {
			file = file.getParent();

			if (file == null) {
				break;
			}

			// this is not applicable to windows, but is the current behavior in
			// GitAssetUtils.listAssets(String, String, String, List<String>, List<String>)
			if (file.getFileName().toString().startsWith(".")) {
				return true;
			}

		} while (!file.equals(folder));

		return false;
	}

	private static boolean isJavaFile(Path path) {
		return FilenameUtils.getExtension(path.toString()).equals("java");
	}

	private static void logDiagnostics(List<Diagnostic<? extends JavaFileObject>> diagnostics, PrintWriter pw) {
		for (Diagnostic<? extends JavaFileObject> x : diagnostics) {
			if (x.getSource() != null) {
				pw.println("[" + x.getKind().toString() + "] " + x.getSource().toUri() + ":" + x.getLineNumber() + " - "
						+ x.getMessage(Locale.getDefault()));
			} else {
				pw.println("[" + x.getKind().toString() + "] 'NO-SOURCE-MESSAGE':" + x.getLineNumber() + " - "
						+ x.getMessage(Locale.getDefault()));
			}
			pw.flush();
		}
	}

	/**
	 * Adds a line to the compile output, in the compiler's own format, for each
	 * call in the classes just compiled that the project sandbox treats
	 * differently: a security question that is deprecated in custom reactors is a
	 * warning, and a call that throws when it runs is an error. Each line names the
	 * call to make instead when there is one. Classes whose source was not part of
	 * this compile are left out.
	 *
	 * @param classesFolder the folder the classes were compiled into
	 * @param sources       the source files that were compiled
	 * @param pw            the compile output
	 * @throws IOException when the classes folder cannot be read
	 */
	private static void logSandboxNotices(Path classesFolder, List<File> sources, PrintWriter pw) throws IOException {
		List<Path> classFiles;
		try (Stream<Path> walk = Files.walk(classesFolder)) {
			classFiles = walk.filter(file -> file.toString().endsWith(".class")).sorted().toList();
		}
		for (Path classFile : classFiles) {
			List<SemossClassLoader.CompileNotice> notices;
			try {
				notices = SemossClassLoader.compileNotices(Files.readAllBytes(classFile));
			} catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
				classLogger.warn("Could not read {} for the compile notices: {}", classFile, e.getMessage());
				continue;
			}
			for (SemossClassLoader.CompileNotice notice : notices) {
				File source = compiledSource(notice.source(), sources);
				if (source != null) {
					Diagnostic.Kind kind = notice.refused() ? Diagnostic.Kind.ERROR : Diagnostic.Kind.WARNING;
					pw.println("[" + kind + "] " + source.toURI() + ":" + notice.line() + " - " + notice.message());
				}
			}
		}
		pw.flush();
	}

	/**
	 * The compiled source file a class came from.
	 *
	 * @param source  the source file as a path from the package root
	 * @param sources the source files that were compiled
	 * @return the file, or null when it was not part of the compile
	 */
	private static File compiledSource(String source, List<File> sources) {
		String suffix = "/" + source;
		for (File file : sources) {
			if (file.toURI().getPath().endsWith(suffix)) {
				return file;
			}
		}
		return null;
	}

	/**
	 * Downloads the text at a URL into a file, line by line.
	 * 
	 * @param urlString
	 * @param filePath
	 */
	public static void copyURLtoFile(String urlString, String filePath) {
		try (PrintWriter out = new PrintWriter(filePath)) {
			URL url = new URL(urlString);
			URLConnection conn = url.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String inputLine;

			// write file
			while ((inputLine = in.readLine()) != null) {
				out.write(inputLine + System.getProperty("line.separator"));
			}
			out.close();
			in.close();
		} catch (MalformedURLException mue) {
			classLogger.error("Failed to copy URL content to file: {}", mue.getMessage(), mue);
		} catch (IOException ioe) {
			classLogger.error("Failed to copy URL content to file: {}", ioe.getMessage(), ioe);
		}
	}

	/**
	 * Gives the owner and group full access to a folder and everything in it
	 * ({@code chmod -R 770}), on Linux and macOS.
	 *
	 * @param directory the folder
	 * @throws IOException          when chmod fails
	 * @throws InterruptedException when interrupted while waiting for chmod
	 */
	public static void setOwnerAndGroupPermissionsRecursively(File directory) throws IOException, InterruptedException {
		String osName = System.getProperty("os.name").toLowerCase();
		if (!osName.contains("nix") && !osName.contains("nux") && !osName.contains("mac")) {
			return;
		}

		if (directory == null) {
			throw new IllegalArgumentException("Directory cannot be null");
		}

		if (!directory.exists()) {
			throw new IllegalArgumentException("Directory does not exist: " + directory.getAbsolutePath());
		}

		if (!directory.isDirectory()) {
			throw new IllegalArgumentException("The specified file is not a directory: " + directory.getAbsolutePath());
		}

		// Construct the chmod command
		String[] command = { "chmod", "-R", "770", directory.getAbsolutePath() };

		// Use ProcessBuilder to execute the command
		ProcessBuilder processBuilder = new ProcessBuilder(command);
		processBuilder.redirectErrorStream(true); // Merge error stream with input stream

		Process process = processBuilder.start();

		// Capture and log the output
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				classLogger.info(line);
			}
		}

		// Wait for the process to complete
		int exitCode = process.waitFor();

		if (exitCode != 0) {
			throw new IOException("Failed to set permissions on " + directory.getAbsolutePath()
					+ ", chmod command exited with code " + exitCode);
		} else {
			classLogger.info("Changed permissions on {} with exit code {}", directory.getAbsolutePath(), exitCode);
		}
	}
}
