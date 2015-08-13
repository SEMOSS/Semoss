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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.MasterDBHelper;
import prerna.nameserver.MasterDatabaseURIs;
import prerna.rdf.engine.wrappers.WrapperManager;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSWebWatcher extends AbstractFileWatcher {

	/**
	 * Processes SMSS files.
	 * @param	Name of the file.
	 */
	@Override
	public void process(String fileName) {
		loadNewDB(fileName, true);
	}
	
	/**
	 * Returns an array of strings naming the files in the directory. Goes through list and loads an existing database.
	 */
	public void loadExistingDB(String fileName, boolean checkForLocalMaster) {
		loadNewDB(fileName, checkForLocalMaster);
	}
	
	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public void loadNewDB(String newFile, boolean checkForLocalMaster) {
		String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
		FileInputStream fileIn = null;
		try{
			Properties prop = new Properties();
			fileIn = new FileInputStream(folderToWatch + "/"  +  newFile);
			prop.load(fileIn);
			String engineName = prop.getProperty(Constants.ENGINE);
			if(engines.startsWith(engineName) || engines.contains(";"+engineName)) {
				logger.debug("DB " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String fileName = folderToWatch + "/" + newFile;
				logger.debug("Loading DB " + folderToWatch + "<>" + newFile);
				Utility.loadEngine(fileName, prop);
				boolean hidden = (prop.getProperty(Constants.HIDDEN_DATABASE) != null && Boolean.parseBoolean(prop.getProperty(Constants.HIDDEN_DATABASE)));
				if(!hidden) {
					addToLocalMaster(engineName);
				} else {
					DeleteFromMasterDB deleter = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
					deleter.deleteEngine(engineName);
				}
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
	}
	
	private void addToLocalMaster(String engineName) {
		if(engineName.equals(Constants.LOCAL_MASTER_DB_NAME)) {
			return;
		}
		// first check if local master contains engine
		IEngine localMaster = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		if(localMaster == null) {
			throw new NullPointerException("Unable to find local master database in DIHelper.");
		}
		IEngine engineToAdd = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		if(engineToAdd == null) {
			throw new NullPointerException("Unable to find engine " + engineName + " in DIHelper.");
		}
		
		Map<String, Date> engines = MasterDBHelper.getEngineTimestamps(localMaster);
		if(engines.containsKey(engineName)) {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH);

			// engine present, check xml if changes exist
			String engineTimeStampQuery = "SELECT DISTINCT ?Time WHERE { {<" + MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName + "> <http://semoss.org/ontologies/Relation/Contains/TimeStamp> ?Time} }";
			RDFFileSesameEngine insightXML = ((AbstractEngine) engineToAdd).getInsightBaseXML();
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(insightXML, engineTimeStampQuery);
			String[] timeVar = wrapper.getVariables();
			Date timeInXML = null;
			while(wrapper.hasNext()) {
				try {
					timeInXML = format.parse(wrapper.next().getVar(timeVar[0]).toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			// if dates not equal, add
			if(engines.get(engineName).getTime() != timeInXML.getTime()) {
				DeleteFromMasterDB deleter = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
				deleter.deleteEngine(engineName); //TODO: enable adding the IEngine directly like AddToMasterDB
				
				AddToMasterDB adder = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
				adder.registerEngineLocal(engineToAdd);
			}
		} else {
			// engine not present, add to local master
			AddToMasterDB adder = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			adder.registerEngineLocal(engineToAdd);
		}
	}
	
	
	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst() {
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list(this);
		String localMasterDBName = Constants.LOCAL_MASTER_DB_NAME + ".smss";
		int localMasterIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, localMasterDBName);
		if(localMasterIndex != -1) {
			String temp = fileNames[0];
			fileNames[0] = localMasterDBName;
			fileNames[localMasterIndex] = temp;
			localMasterIndex = 0;
		}
		boolean addToLocal = true;
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			if(fileIdx == localMasterIndex) {
				addToLocal = false;
			}
			try {
				loadExistingDB(fileNames[fileIdx], addToLocal);
			} catch (RuntimeException ex) {
				ex.printStackTrace();
				logger.fatal("Engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
		
		if(localMasterIndex == 0) {
			// remove unused databases
			IEngine localMaster = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
			List<String> engines = MasterDBHelper.getAllEngines(localMaster);
			DeleteFromMasterDB remover = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			for(String engine : engines) {
				if(!ArrayUtilityMethods.arrayContainsValue(fileNames, engine + ".smss")) {
					remover.deleteEngine(engine);
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
