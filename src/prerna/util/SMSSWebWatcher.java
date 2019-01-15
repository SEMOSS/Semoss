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
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUpdater;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.theme.AbstractThemeUtils;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSWebWatcher extends AbstractFileWatcher {

	private static List<String> ignoreSmssList = new Vector<String>();
	static {
		ignoreSmssList.add(Constants.LOCAL_MASTER_DB_NAME);
		ignoreSmssList.add(Constants.SECURITY_DB);
		ignoreSmssList.add(Constants.THEMING_DB);
	}
	
	private static final Logger LOGGER = LogManager.getLogger(SMSSWebWatcher.class.getName());

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
		String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
		FileInputStream fileIn = null;
		String engineId = null;
		try{
			Properties prop = new Properties();
			fileIn = new FileInputStream(folderToWatch + "/"  +  newFile);
			prop.load(fileIn);
			engineId = prop.getProperty(Constants.ENGINE);
			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				LOGGER.debug("DB " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String fileName = folderToWatch + "/" + newFile;
				Utility.loadEngine(fileName, prop);
			}
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(fileIn != null) {
					fileIn.close();
				}
			} catch(IOException e) {
				e.printStackTrace();
			}
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
		String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
		FileInputStream fileIn = null;
		String engineId = null;
		try{
			Properties prop = new Properties();
			fileIn = new FileInputStream(folderToWatch + "/"  +  newFile);
			prop.load(fileIn);
			engineId = prop.getProperty(Constants.ENGINE);
			
			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				LOGGER.debug("DB " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String fileName = folderToWatch + "/" + newFile;
				DIHelper.getInstance().getCoreProp().setProperty(engineId + "_" + Constants.STORE, fileName);
				String engineTypeString = null;
				String rawType = prop.get(Constants.ENGINE_TYPE).toString();
				if(rawType.contains("RDBMS")) {
					engineTypeString = "RDBMS";
				} else if(rawType.contains("AppEngine")) {
					engineTypeString = "APP";
				} else if(rawType.contains("RemoteSemossEngine")) {
					engineTypeString = "REMOTE";
				}
				// default is some rdf
				else {
					engineTypeString = "RDF";
				}
				DIHelper.getInstance().getCoreProp().setProperty(engineId + "_" + Constants.TYPE, engineTypeString);
				
				String engineNames = (String)DIHelper.getInstance().getLocalProp(Constants.ENGINES);
				if(!(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId))) {
					engineNames = engineNames + ";" + engineId;
					DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
				}

				// the issue with remote engines is it needs to be loaded to get the insights and the owl file
				// TODO need something that will modify this for the remote engines
				// if the db folder exsits.. nothing to do
				// else this needs to be open db
				// and then register this engine
//				if(prop.containsKey(Constants.ENGINE_TYPE) && prop.get(Constants.ENGINE_TYPE).toString().toUpperCase().contains("REMOTE"))
//				{
//					// this is a remote engine which needs to be registered
//					// but need to see if I need to open DB or not
//					// may be this logic is sitting within open DB
//					// or not
//					try {
//						engine = (IEngine) Class.forName(prop.get(Constants.ENGINE_TYPE)+"").newInstance();
//						engine.openDB(newFile);
//						engine.closeDB();
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
				
				//TODO: need to account for remote!!!!
				//TODO: need to account for remote!!!!
				//TODO: need to account for remote!!!!
				//TODO: need to account for remote!!!!
				if(!engineTypeString.equals("REMOTE") && !ignoreSmssList.contains(engineId)) {
					// sync up the engine metadata now
					Utility.synchronizeEngineMetadata(engineId);
					SecurityUpdateUtils.addApp(engineId);
				} else {
					System.out.println("Ignoring engine ... " + prop.getProperty(Constants.ENGINE_ALIAS) + " >>> " + engineId );
				}
			}
		} catch(Exception e){
			e.printStackTrace();
		} finally {
			try{
				if(fileIn != null) {
					fileIn.close();
				}
			} catch(IOException e) {
				e.printStackTrace();
			}
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
		} catch (SQLException e) {
			// we couldn't initialize the db
			// remove it from DIHelper
			DIHelper.getInstance().removeLocalProperty(Constants.LOCAL_MASTER_DB_NAME);
			e.printStackTrace();
		}
					
		/*
		 * AFTER WE LOAD THE LOCAL MASTER - UPDATE LEGACY DATABASES
		 * TO ADD USER SPECIFIC DATABASES / UNIQUE ENGINE NAMES
		 */
		SmssUpdater.run();
		
		// also need to load the security db
		String securityDBName = Constants.SECURITY_DB + this.extension;
		int securityIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, securityDBName);
		loadExistingDB(fileNames[securityIndex]);
		// initialize the security database
		try {
			AbstractSecurityUtils.loadSecurityDatabase();
		} catch (SQLException e) {
			// we couldn't initialize the db
			// remove it from DIHelper
			DIHelper.getInstance().removeLocalProperty(Constants.SECURITY_DB);
			e.printStackTrace();
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
				DIHelper.getInstance().removeLocalProperty(Constants.THEMING_DB);
				e.printStackTrace();
			}
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

		// loop through and load all the engines
		// but we will ignore the local master and security database
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			try {
				String fileName = fileNames[fileIdx];
				if(fileName.equals(localMasterDBName) || fileName.equals(securityDBName) || fileName.equals(themeDBName)) {
					// again, ignore local master + security
					continue;
				}
				
				// I really dont want to load anything here
				// I only want to keep track of what are the engine names and their corresponding SMSS files
				// so we will catalog instead of load
				String loadedEngineId = catalogDB(fileName, folderToWatch);
				engineIds[fileIdx] = loadedEngineId;
			} catch (RuntimeException ex) {
				ex.printStackTrace();
				LOGGER.fatal("Engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
		
		// remove unused databases
		if (!ClusterUtil.IS_CLUSTER) {
			List<String> engines = MasterDatabaseUtility.getAllEngineIds();
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			
			for(String engine : engines) {
				if(!ArrayUtilityMethods.arrayContainsValue(engineIds, engine)) {
					LOGGER.info("Deleting the engine from local master..... " + engine);
					remover.deleteEngineRDBMS(engine);
				}
			}
			
			engines = SecurityQueryUtils.getEngineIds();
			for(String engine : engines) {
				if(!ArrayUtilityMethods.arrayContainsValue(engineIds, engine)) {
					LOGGER.info("Deleting the engine from security..... " + engine);
					SecurityUpdateUtils.deleteApp(engine);
				}
			}
		}
	}

	/**
	 * Processes new SMSS files.
	 */
	@Override
	public void run() {
		LOGGER.info("Starting SMSSWebWatcher thread");
		synchronized(monitor) {
			loadFirst();
			super.run();
		}
	}

}
