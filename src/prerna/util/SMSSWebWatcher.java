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

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.notifications.NotificationDbUtils;
import prerna.prompt.PromptUtils;
import prerna.reactor.automation.AutomationDatabaseUtility;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.theme.AbstractThemeUtils;
import prerna.usertracking.UserTrackingUtils;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSWebWatcher extends AbstractFileWatcher {

	private static final Logger classLogger = LogManager.getLogger(SMSSWebWatcher.class);

	/**
	 * Processes SMSS files.
	 * 
	 * @param Name of the file.
	 */
	@Override
	public void process(String fileName) {
		catalogEngine(fileName, folderToWatch);
	}

	/**
	 * Returns an array of strings naming the files in the directory. Goes through
	 * list and loads an existing database.
	 */
	public String loadExistingEngine(String fileName) {
		return loadNewEngine(fileName, folderToWatch);
	}

	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * 
	 * @param Specifies properties to load
	 */
	public static String loadNewEngine(String newFile, String folderToWatch) {
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
				classLogger.debug("DB {}<>{} is already loaded...", folderToWatch, newFile);
			} else {
				String filePath = folderToWatch + "/" + newFile;
				Utility.catalogEngineByType(filePath, prop, engineId);
			}
		} catch (Exception e) {
			classLogger.error("Failed to load engine from SMSS file {} in {}", newFile, folderToWatch, e);
		}

		return engineId;
	}

	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * 
	 * @param Specifies properties to load
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
				classLogger.debug("DB {}<>{} is already loaded...", folderToWatch, newFile);
			} else {
				String filePath = folderToWatch + "/" + newFile;
				Utility.catalogEngineByType(filePath, prop, engineId);
			}
		} catch (Exception e) {
			classLogger.error("Failed to catalog engine from SMSS file {} in {}", newFile, folderToWatch, e);
		}

		return engineId;
	}

	@Override
	public void init() {
		// we will load the local master database
		// and the security database before we load anything else
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list(this);

		// find the local master
		String localMasterDBName = Constants.LOCAL_MASTER_DB + this.extension;
		int localMasterIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, localMasterDBName);
		try {
			SystemEngineRegistry.loadSystemEngine(folderToWatch + "/" + fileNames[localMasterIndex]);
			MasterDatabaseUtility.initLocalMaster();
		} catch (Exception e) {
			classLogger.error("Failed to load and initialize the local master database", e);
			return;
		}

		// also need to load the security db
		String securityDBName = Constants.SECURITY_DB + this.extension;
		int securityIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, securityDBName);
		try {
			SystemEngineRegistry.loadSystemEngine(folderToWatch + "/" + fileNames[securityIndex]);
			AbstractSecurityUtils.loadSecurityDatabase();
		} catch (Exception e) {
			classLogger.error("Failed to load and initialize the security database", e);
			return;
		}

		if (Utility.isAuditLogsDatabaseEnabled()) {
			String auditLogsName = Constants.AUDIT_LOGS_DB + this.extension;
			int auditLogsIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, auditLogsName);
			if (auditLogsIndex > -1) {
				try {
					SystemEngineRegistry.loadSystemEngine(folderToWatch + "/" + fileNames[auditLogsIndex]);
					AuditLogsDbUtils.loadAuditLogsDatabase();
				} catch (Exception e) {
					classLogger.error("Failed to load and initialize the audit logs database", e);
				}
			}
		}

		// load user tracking database
		if (Utility.isUserTrackingEnabled()) {
			String userTrackerDBName = Constants.USER_TRACKING_DB + this.extension;
			int userTrackerDbNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, userTrackerDBName);
			if (userTrackerDbNameIndex > -1) {
				try {
					SystemEngineRegistry.loadSystemEngine(folderToWatch + "/" + fileNames[userTrackerDbNameIndex]);
					UserTrackingUtils.initUserTrackerDatabase();
				} catch (Exception e) {
					classLogger.error("Failed to load and initialize the user tracking database", e);
				}
			}
		}

		String themingDbName = Constants.THEMING_DB + this.extension;
		int themingDbNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, themingDbName);
		if (themingDbNameIndex > -1) {
			try {
				SystemEngineRegistry.loadSystemEngine(folderToWatch + "/" + fileNames[themingDbNameIndex]);
				AbstractThemeUtils.loadThemingDatabase();
			} catch (Exception e) {
				classLogger.error("Failed to load and initialize the theming database", e);
			}
		}

		// change to scheduler info
		if (!Utility.schedulerForceDisable()) {
			String schedulerDbName = Constants.SCHEDULER_DB + this.extension;
			int schedulerDbNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, schedulerDbName);
			if (schedulerDbNameIndex > -1) {
				try {
					SystemEngineRegistry.loadSystemEngine(folderToWatch + "/" + fileNames[schedulerDbNameIndex]);
					SchedulerDatabaseUtility.startServer();
					// Automation tables live in the scheduler DB, so only initialize them
					// after the scheduler DB has started successfully.
					AutomationDatabaseUtility.initialize();
					AutomationDatabaseUtility.markStaleRunsInterrupted();
				} catch (Exception e) {
					classLogger.error("Failed to load and start the scheduler database", e);
				}
			}
		}

		// load model inference logs database
		if (Utility.isModelInferenceLogsEnabled()) {
			String modelInferenceLogsDBName = Constants.MODEL_INFERENCE_LOGS_DB + this.extension;
			int modelInferenceLogsDBNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames,
					modelInferenceLogsDBName);
			if (modelInferenceLogsDBNameIndex > -1) {
				try {
					SystemEngineRegistry
							.loadSystemEngine(folderToWatch + "/" + fileNames[modelInferenceLogsDBNameIndex]);
					ModelInferenceLogsUtils.initModelInferenceLogsDatabase();
				} catch (Exception e) {
					classLogger.error("Failed to load and initialize the model inference logs database", e);
				}
			}
		}

		if (Utility.isPromptDatabaseEnabled()) {
			String promptDbName = Constants.PROMPT_DB + this.extension;
			int promptDbNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, promptDbName);
			if (promptDbNameIndex > -1) {
				try {
					SystemEngineRegistry.loadSystemEngine(folderToWatch + "/" + fileNames[promptDbNameIndex]);
					PromptUtils.loadPromptDatabase();
				} catch (Exception e) {
					classLogger.error("Failed to load and initialize the prompt database", e);
				}
			}
		}

		if (Utility.isNotificationDatabaseEnabled()) {
			String notificationDbName = Constants.NOTIFICATION_DB + this.extension;
			int notificationDbNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, notificationDbName);
			if (notificationDbNameIndex > -1) {
				try {
					SystemEngineRegistry.loadSystemEngine(folderToWatch + "/" + fileNames[notificationDbNameIndex]);
					NotificationDbUtils.loadNotificationDatabase();
				} catch (Exception e) {
					classLogger.error("Failed to load and initialize the notification database", e);
				}
			}
		}
	}

	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst() {
		// I need to get all the SMSS files
		// Read the engine names and profile the SMSS files i.e. capture that in some
		// kind of hashtable
		// and let it go that is it
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list(this);
		if (fileNames == null || fileNames.length == 0) {
			return;
		}
		Set<String> engineIds = new HashSet<>(fileNames.length);

		String localMasterDBName = Constants.LOCAL_MASTER_DB + this.extension;
		String securityDBName = Constants.SECURITY_DB + this.extension;
		String themeDBName = Constants.THEMING_DB + this.extension;
		String promptDBName = Constants.PROMPT_DB + this.extension;
		String schedulerDBName = Constants.SCHEDULER_DB + this.extension;
		String userTrackingDBName = Constants.USER_TRACKING_DB + this.extension;
		String modelInferenceLogsDB = Constants.MODEL_INFERENCE_LOGS_DB + this.extension;
		String notificationDB = Constants.NOTIFICATION_DB + this.extension;

		// loop through and load all the engines
		// but we will ignore the local master and security database
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			try {
				String fileName = fileNames[fileIdx];
				if (fileName.equals(localMasterDBName) || fileName.equals(securityDBName)
						|| fileName.equals(themeDBName) || fileName.equals(schedulerDBName)
						|| (fileName.equals(promptDBName) && !Utility.isPromptDatabaseEnabled())
						|| fileName.equals(userTrackingDBName)
						|| (fileName.equals(modelInferenceLogsDB) && !Utility.isModelInferenceLogsEnabled())
						|| (fileName.equals(notificationDB) && !Utility.isNotificationDatabaseEnabled())) {
					// ignore - we have already loaded these or they are disabled and need to be
					// ignored
					continue;
				}

				// I really dont want to load anything here
				// I only want to keep track of what are the engine names and their
				// corresponding SMSS files
				// so we will catalog instead of load
				String loadedEngineId = catalogEngine(fileName, folderToWatch);
				engineIds.add(loadedEngineId);
			} catch (RuntimeException ex) {
				classLogger.error("Failed to catalog database engine from SMSS file {}/{}", folderToWatch,
						fileNames[fileIdx], ex);
				classLogger.fatal("Database engine failed to load: {}/{}", folderToWatch, fileNames[fileIdx]);
			}
		}

		// remove unused databases
		if (!ClusterUtil.IS_CLUSTER) {
			List<String> engines = MasterDatabaseUtility.getAllDatabaseIds();
			DeleteFromMasterDB remover = new DeleteFromMasterDB();

			for (String engine : engines) {
				if (!engineIds.contains(engine)) {
					classLogger.info("Deleting the database engine from local master..... {}",
							Utility.cleanLogString(engine));
					remover.deleteEngineRDBMS(engine);
				}
			}

			engines = SecurityEngineUtils.getAllEngineIds(Arrays.asList(IEngine.CATALOG_TYPE.DATABASE.toString()));
			for (String engine : engines) {
				if (!engineIds.contains(engine)) {
					classLogger.info("Deleting the database engine {} from security", Utility.cleanLogString(engine));
					SecurityEngineUtils.deleteEngine(engine);
				}
			}
		}
	}

}
