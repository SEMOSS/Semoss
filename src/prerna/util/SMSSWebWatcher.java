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
import java.util.List;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.auth.AbstractSecurityUtils;
import prerna.auth.SecurityQueryUtils;
import prerna.auth.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.OwlConceptualNameModernizer;
import prerna.engine.impl.SmssUpdater;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSWebWatcher extends AbstractFileWatcher {

	private static final Logger LOGGER = LogManager.getLogger(SMSSWebWatcher.class.getName());

	/**
	 * Processes SMSS files.
	 * @param	Name of the file.
	 */
	@Override
	public void process(String fileName) {
		catalogDB(fileName);
	}

	/**
	 * Returns an array of strings naming the files in the directory. Goes through list and loads an existing database.
	 */
	public String loadExistingDB(String fileName) {
		return loadNewDB(fileName);
	}

	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public String loadNewDB(String newFile) {
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
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
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
	public String catalogDB(String newFile) {
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
				} else {
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
				if(prop.containsKey(Constants.ENGINE_TYPE) && prop.get(Constants.ENGINE_TYPE).toString().toUpperCase().contains("REMOTE"))
				{
					// this is a remote engine which needs to be registered
					// but need to see if I need to open DB or not
					// may be this logic is sitting within open DB
					// or not
					try {
						engine = (IEngine) Class.forName(prop.get(Constants.ENGINE_TYPE)+"").newInstance();
						engine.openDB(newFile);
						engine.closeDB();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			
				// we want to ignore AppEngine
				// since it contains no data / owl
				if(!engineTypeString.equals("APP")) {
					// THIS IS BECAUSE WE HAVE MADE A LOT OF MODIFICATIONS TO THE OWL
					// GOTTA MAKE SURE IT IS MODERNIZED VERSION
					// AS TIME GOES ON, THIS CODE CAN BE REMOVED
					// IF IT DOES CHANGE THE OWL, THE TIMESTAMP WILL CHAGNE AND LOCAL MASTER
					// WILL AUTOMATICALLY UPDATE
					OwlConceptualNameModernizer modernizer = new OwlConceptualNameModernizer(prop);
					modernizer.run();
				}
				
				boolean isLocal = engineId.equals(Constants.LOCAL_MASTER_DB_NAME);
				boolean isSecurity = engineId.equals(Constants.SECURITY_DB);
				if(!isLocal & !isSecurity) {
					// sync up the engine metadata now
					Utility.synchronizeEngineMetadata(engineId);
					SecurityUpdateUtils.addApp(engineId);
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		
		return engineId;
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
		String[] engineNames = new String[fileNames.length];
		String localMasterDBName = Constants.LOCAL_MASTER_DB_NAME + this.extension;
		// trying to figure out where the local master database is in the scheme of things
		// and then letting it load first
		
		// store the file index to start at
		// in case the files do not find the local master or security db
		// we at least start at the right index
		// although, things will not work out well if security or local master are not found
		int fileIdx = 0;
		
		int localMasterIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, localMasterDBName);
		if(localMasterIndex != -1) {
			String temp = fileNames[0];
			fileNames[0] = localMasterDBName;
			fileNames[localMasterIndex] = temp;
			localMasterIndex = 0;
			// let us first load the master index
			loadExistingDB(fileNames[0]);
			// initialize the local master
			MasterDatabaseUtility.initLocalMaster();
			// update file index
			fileIdx++;
		}
		
		/*
		 * AFTER WE LOAD THE LOCAL MASTER - UPDATE LEGACY DATABASES
		 * TO ADD USER SPECIFIC DATABASES / UNIQUE ENGINE NAMES
		 */
		SmssUpdater.run();
		
		// recalculate everything
		dir = new File(folderToWatch);
		fileNames = dir.list(this);
		engineNames = new String[fileNames.length];
		localMasterIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, localMasterDBName);
		if(localMasterIndex != -1) {
			String temp = fileNames[0];
			fileNames[0] = localMasterDBName;
			fileNames[localMasterIndex] = temp;
		}
		
		// now continue normal flow
		
		// also need to load the security db
		String securityDBName = Constants.SECURITY_DB + this.extension;
		// trying to figure out where the security database is in the scheme of things
		// and then letting it load second
		int securityIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, securityDBName);
		if(securityIndex != -1) {
			String temp = fileNames[1];
			fileNames[1] = securityDBName;
			fileNames[securityIndex] = temp;
			localMasterIndex = 1;
			// let us now load the security db
			loadExistingDB(fileNames[1]);
			AbstractSecurityUtils.loadSecurityDatabase();
			// update file index
			fileIdx++;
		}
		
		// now we start at index 2 since we index 0 is the local master and index 1 is security db
		for (; fileIdx < fileNames.length; fileIdx++) {
			try {
				// I really dont want to load anything here
				// I only want to keep track of what are the engine names and their corresponding SMSS files
				
				String loadedEngineName = catalogDB(fileNames[fileIdx]);
				// if the engine is hidden
				// this will return null since no engine has been loaded
				engineNames[fileIdx] = loadedEngineName;
			} catch (RuntimeException ex) {
				ex.printStackTrace();
				LOGGER.fatal("Engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
		
		// remove unused databases
		List<String> engines = MasterDatabaseUtility.getAllEngineIds();
		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		
		for(String engine : engines) {
			if(!ArrayUtilityMethods.arrayContainsValue(engineNames, engine)) {
				LOGGER.info("Deleting the engine from local master..... " + engine);
				remover.deleteEngineRDBMS(engine);
			}
		}
		
		engines = SecurityQueryUtils.getEngineIds();
		for(String engine : engines) {
			if(!ArrayUtilityMethods.arrayContainsValue(engineNames, engine)) {
				LOGGER.info("Deleting the engine from security..... " + engine);
				SecurityUpdateUtils.deleteApp(engine);
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
