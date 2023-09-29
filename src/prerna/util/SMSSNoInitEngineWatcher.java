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
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSNoInitEngineWatcher extends AbstractFileWatcher {

	private static final Logger classLogger = LogManager.getLogger(SMSSNoInitEngineWatcher.class);

	/**
	 * Processes SMSS files.
	 * @param	Name of the file.
	 */
	@Override
	public void process(String fileName) {
		catalogEngine(fileName, folderToWatch);
	}

	/**
	 * Returns an array of strings naming the files in the directory. Goes through list and loads an existing database.
	 */
	public String loadExistingEngine(String fileName) {
		return loadNewEngine(fileName, folderToWatch);
	}

	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public static String loadNewEngine(String newFile, String folderToWatch) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
		String engineId = null;
		try{
			Properties prop = Utility.loadProperties(Utility.normalizePath(folderToWatch) + "/"  + Utility.normalizePath(newFile));
			if(prop == null) {
				throw new NullPointerException("Unable to find/load properties file '" + newFile + "'");
			}
			
			engineId = prop.getProperty(Constants.ENGINE);
			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				classLogger.debug("Engine " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String filePath = folderToWatch + "/" + newFile;
				Utility.catalogEngineByType(filePath, prop, engineId);
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return engineId;
	}
	
	// this is an alternate method.. which will not load the database but would merely keep the name of the engine
	// and the SMSS file
	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */	
	public static String catalogEngine(String newFile, String folderToWatch) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
		String engineId = null;
		try{
			Properties prop = Utility.loadProperties(Utility.normalizePath(folderToWatch) + "/"  + Utility.normalizePath(newFile));
			if(prop == null) {
				throw new NullPointerException("Unable to find/load properties file '" + newFile + "'");
			}
			engineId = prop.getProperty(Constants.ENGINE);
			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				classLogger.debug("Engine " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String filePath = folderToWatch + "/" + newFile;
				Utility.catalogEngineByType(filePath, prop, engineId);
			}
		} catch(Exception e){
			classLogger.error(Constants.STACKTRACE, e);
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
		String[] engineIds = new String[fileNames.length];

		// loop through and load all the engines
		// but we will ignore the local master and security database
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			try {
				String fileName = fileNames[fileIdx];
				// I really dont want to load anything here
				// I only want to keep track of what are the engine names and their corresponding SMSS files
				// so we will catalog instead of load
				String loadedEngineId = catalogEngine(fileName, folderToWatch);
				engineIds[fileIdx] = loadedEngineId;
			} catch (RuntimeException ex) {
				classLogger.error(Constants.STACKTRACE, ex);
				classLogger.fatal("Engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
		
		// remove unused databases
		if (!ClusterUtil.IS_CLUSTER) {
			List<String> engines = SecurityEngineUtils.getAllEngineIds(Arrays.asList(getEngineType().name()));
			for(String engine : engines) {
				if(!ArrayUtilityMethods.arrayContainsValue(engineIds, engine)) {
					classLogger.info("Deleting the engine from security..... " + Utility.cleanLogString(engine));
					SecurityEngineUtils.deleteEngine(engine);
				}
			}
		}
	}
}
