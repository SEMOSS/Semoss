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
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.LegacyToProjectRestructurerHelper;
import prerna.engine.impl.OwlPrettyPrintFixer;
import prerna.engine.impl.OwlSeparatePixelFromConceptual;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.theme.AbstractThemeUtils;
import prerna.usertracking.UserTrackingUtils;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSWebWatcher extends AbstractFileWatcher {

	private static List<String> ignoreSmssList = new ArrayList<>();
	static {
		ignoreSmssList.add(Constants.LOCAL_MASTER_DB_NAME);
		ignoreSmssList.add(Constants.SECURITY_DB);
		ignoreSmssList.add(Constants.THEMING_DB);
		ignoreSmssList.add(Constants.SCHEDULER_DB);
		ignoreSmssList.add(Constants.USER_TRACKING_DB);
	}
	
	private static final Logger logger = LogManager.getLogger(SMSSWebWatcher.class);

	/**
	 * Processes SMSS files.
	 * @param	Name of the file.
	 */
	@Override
	public void process(String fileName) {
		catalogDB(fileName, folderToWatch);
	}

	/**
	 * Returns an array of strings naming the files in the directory. Goes through list and loads an existing database.
	 */
	public String loadExistingDB(String fileName) {
		return loadNewDB(fileName, folderToWatch);
	}

	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public static String loadNewDB(String newFile, String folderToWatch) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
		String engineId = null;
		try{
			Properties prop = Utility.loadProperties(Utility.normalizePath(folderToWatch) + "/"  + Utility.normalizePath(newFile));
			if(prop == null) {
				throw new NullPointerException("Unable to find/load properties file '" + newFile + "'");
			}
			
			// TODO: TO FIX ERRORS WITH PRETTY PRINT METHOD
			OwlPrettyPrintFixer.fixOwl(prop);
			// Update OWL
			OwlSeparatePixelFromConceptual.fixOwl(prop);
			
			engineId = prop.getProperty(Constants.ENGINE);
			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				logger.debug("DB " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
//				String fileName = folderToWatch + "/" + newFile;
//				Utility.loadEngine(fileName, prop);
				String filePath = folderToWatch + "/" + newFile;
				Utility.catalogEngineByType(filePath, prop, engineId);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		return engineId;
	}
	
	// this is an alternate method.. which will not load the database but would merely keep the name of the engine
	// and the SMSS file
	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */	
	public static String catalogDB(String newFile, String folderToWatch) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
		String engineId = null;
		try{
			Properties prop = Utility.loadProperties(Utility.normalizePath(folderToWatch) + "/"  + Utility.normalizePath(newFile));
			if(prop == null) {
				throw new NullPointerException("Unable to find/load properties file '" + newFile + "'");
			}
			// TODO: TO FIX ERRORS WITH PRETTY PRINT METHOD
			OwlPrettyPrintFixer.fixOwl(prop);
			// Update OWL
			OwlSeparatePixelFromConceptual.fixOwl(prop);
			
			engineId = prop.getProperty(Constants.ENGINE);
			
			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				logger.debug("DB " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String filePath = folderToWatch + "/" + newFile;
				Utility.catalogEngineByType(filePath, prop, engineId);
			}
		} catch(Exception e){
			logger.error(Constants.STACKTRACE, e);
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
		String localMasterDBName = Constants.LOCAL_MASTER_DB_NAME + this.extension;
		int localMasterIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, localMasterDBName);
		loadExistingDB(fileNames[localMasterIndex]);
		// initialize the local master
		try {
			MasterDatabaseUtility.initLocalMaster();
		} catch (Exception e) {
			// we couldn't initialize the db
			// remove it from DIHelper
			DIHelper.getInstance().removeEngineProperty(Constants.LOCAL_MASTER_DB_NAME);
			logger.error(Constants.STACKTRACE, e);
			return;
		}
					
		// also need to load the security db
		String securityDBName = Constants.SECURITY_DB + this.extension;
		int securityIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, securityDBName);
		loadExistingDB(fileNames[securityIndex]);
		// initialize the security database
		try {
			AbstractSecurityUtils.loadSecurityDatabase();
		} catch (Exception e) {
			// we couldn't initialize the db
			// remove it from DIHelper
			DIHelper.getInstance().removeEngineProperty(Constants.SECURITY_DB);
			logger.error(Constants.STACKTRACE, e);
			return;
		}
		
		String themingDbName = Constants.THEMING_DB + this.extension;
		int themingDbNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, themingDbName);
		if(themingDbNameIndex > -1) {
			loadExistingDB(fileNames[themingDbNameIndex]);
			// initialize the security database
			try {
				AbstractThemeUtils.loadThemingDatabase();
			} catch (SQLException e) {
				// we couldn't initialize the db
				// remove it from DIHelper
				DIHelper.getInstance().removeEngineProperty(Constants.THEMING_DB);
				logger.error(Constants.STACKTRACE, e);
			}
		}

		// change to scheduler info
		if(!Utility.schedulerForceDisable()) {
			String schedulerDbName = Constants.SCHEDULER_DB + this.extension;
			int schedulerDbNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, schedulerDbName);
			if(schedulerDbNameIndex > -1) {
				loadExistingDB(fileNames[schedulerDbNameIndex]);
				// initialize the scheduler database
				try {
					SchedulerDatabaseUtility.startServer();
				} catch (SQLException sqe) {
					// we couldn't initialize the db remove it from DIHelper
					DIHelper.getInstance().removeEngineProperty(Constants.SCHEDULER_DB);
					logger.error(Constants.STACKTRACE, sqe);
				} catch (IOException e) {
					DIHelper.getInstance().removeEngineProperty(Constants.SCHEDULER_DB);
					logger.error(Constants.STACKTRACE, e);
				}
			}	
		}
		
		// load user tracking database
		if(Utility.isUserTrackingEnabled()) {
			String userTrackerDBName = Constants.USER_TRACKING_DB + this.extension;
			int userTrackerDbNameIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, userTrackerDBName);
	
			if (userTrackerDbNameIndex > -1) {
				loadExistingDB(fileNames[userTrackerDbNameIndex]);
				try {
					UserTrackingUtils.initUserTrackerDatabase();
				} catch (Exception e) {
					// we couldn't initialize the db
					// remove it from DIHelper
					DIHelper.getInstance().removeEngineProperty(Constants.USER_TRACKING_DB);
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// THIS IS TEMPORARY UNTIL WE HAVE ALL USERS ON THE NEW VERSION
		// USING THE DB AND PROJECT SPLIT OF AN APP
		// TODO: need to update this for the cloud
		if (!ClusterUtil.IS_CLUSTER) {
			LegacyToProjectRestructurerHelper updater = new LegacyToProjectRestructurerHelper();
			updater.executeRestructure();
		}
	}

	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst() {
		// I need to get all the SMSS files
		// Read the engine names and profile the SMSS files i.e. capture that in some kind of hashtable
		// and let it go that is it
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list(this);
		String[] engineIds = new String[fileNames.length];
		String localMasterDBName = Constants.LOCAL_MASTER_DB_NAME + this.extension;
		String securityDBName = Constants.SECURITY_DB + this.extension;
		String themeDBName = Constants.THEMING_DB + this.extension;
		String schedulerDBName = Constants.SCHEDULER_DB + this.extension;
		String userTrackingDBName = Constants.USER_TRACKING_DB + this.extension;

		// loop through and load all the engines
		// but we will ignore the local master and security database
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			try {
				String fileName = fileNames[fileIdx];
				if(fileName.equals(localMasterDBName) || fileName.equals(securityDBName) || fileName.equals(themeDBName) || fileName.equals(schedulerDBName) || fileName.equals(userTrackingDBName)) {
					// again, ignore local master + security
					continue;
				}
				
				// I really dont want to load anything here
				// I only want to keep track of what are the engine names and their corresponding SMSS files
				// so we will catalog instead of load
				String loadedEngineId = catalogDB(fileName, folderToWatch);
				engineIds[fileIdx] = loadedEngineId;
			} catch (RuntimeException ex) {
				logger.error(Constants.STACKTRACE, ex);
				logger.fatal("Engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
		
		// remove unused databases
		if (!ClusterUtil.IS_CLUSTER) {
			List<String> engines = MasterDatabaseUtility.getAllDatabaseIds();
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			
			for(String engine : engines) {
				if(!ArrayUtilityMethods.arrayContainsValue(engineIds, engine)) {
					logger.info("Deleting the engine from local master..... " + Utility.cleanLogString(engine));
					remover.deleteEngineRDBMS(engine);
				}
			}
			
			engines = SecurityDatabaseUtils.getAllDatabaseIds();
			for(String engine : engines) {
				if(!ArrayUtilityMethods.arrayContainsValue(engineIds, engine)) {
					logger.info("Deleting the engine from security..... " + Utility.cleanLogString(engine));
					SecurityDatabaseUtils.deleteDatabase(engine);
				}
			}
		}
	}

	/**
	 * Processes new SMSS files.
	 */
	@Override
	public void run() {
		logger.info("Starting SMSSWebWatcher thread");
		synchronized(monitor) {
			loadFirst();
			super.run();
		}
	}

}
