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
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.MasterDBHelper;
import prerna.poi.main.BaseDatabaseCreator;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.solr.SolrIndexEngine;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSWebWatcher extends AbstractFileWatcher {

	static final Logger logger = LogManager.getLogger(SMSSWebWatcher.class.getName());

	/**
	 * Processes SMSS files.
	 * @param	Name of the file.
	 */
	@Override
	public void process(String fileName) {
		loadNewDB(fileName, false);
	}

	/**
	 * Returns an array of strings naming the files in the directory. Goes through list and loads an existing database.
	 */
	public String loadExistingDB(String fileName, boolean isLocal) {
		return loadNewDB(fileName, isLocal);
	}

	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public String  loadNewDB(String newFile, boolean isLocal) {
		String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
		FileInputStream fileIn = null;
		String engineName = null;
		try{
			Properties prop = new Properties();
			fileIn = new FileInputStream(folderToWatch + "/"  +  newFile);
			prop.load(fileIn);
			engineName = prop.getProperty(Constants.ENGINE);
			if(engines.startsWith(engineName) || engines.contains(";"+engineName+";") || engines.endsWith(";"+engineName)) {
				logger.debug("DB " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String fileName = folderToWatch + "/" + newFile;
				IEngine engineLoaded = Utility.loadWebEngine(fileName, prop);
				boolean hidden = (prop.getProperty(Constants.HIDDEN_DATABASE) != null && Boolean.parseBoolean(prop.getProperty(Constants.HIDDEN_DATABASE)));
				if(!isLocal && !hidden) {
					addToLocalMaster(engineLoaded);
				} else if(!isLocal){ // never add local master to itself...
					DeleteFromMasterDB deleter = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
					deleter.deleteEngine(engineName);
					Utility.deleteFromSolr(engineName);
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

		return engineName;
	}

	//	private void clearLocalDB() {
	//		BigDataEngine localDB = (BigDataEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
	//		String query = "SELECT DISTINCT ?S ?P ?O WHERE {?S ?P ?O}";
	//		List<Object[]> results = new ArrayList<Object[]>();
	//		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(localDB, query);
	//		String[] names = wrap.getVariables();
	//		while(wrap.hasNext()) {
	//			ISelectStatement ss = wrap.next();
	//			boolean isConcept = false;
	//			if(ss.getRawVar(names[2]).toString().startsWith("http://")) {
	//				isConcept = true;
	//			}
	//			results.add(new Object[]{ss.getRawVar(names[0]).toString(), ss.getRawVar(names[1]).toString(), ss.getRawVar(names[2]).toString(), isConcept});
	//		}
	//		
	//		for(Object[] row : results) {
	//			localDB.doAction(ACTION_TYPE.REMOVE_STATEMENT, row);
	//		}
	//		localDB.commit();
	//	}

	private void addToLocalMaster(IEngine engineToAdd) {
		IEngine localMaster = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		if(localMaster == null) {
			throw new NullPointerException("Unable to find local master database in DIHelper.");
		}
		if(engineToAdd == null) {
			throw new NullPointerException("Unable to load engine ");
		}

		String engineName = engineToAdd.getEngineName();
		String engineURL = "http://semoss.org/ontologies/Concept/Engine/" + Utility.cleanString(engineName, true);
		String localDbQuery = "SELECT DISTINCT ?TIMESTAMP WHERE {<" + engineURL + "> <" + BaseDatabaseCreator.TIME_KEY + "> ?TIMESTAMP}";
		String engineQuery = "SELECT DISTINCT ?TIMESTAMP WHERE {<" + BaseDatabaseCreator.TIME_URL + "> <" + BaseDatabaseCreator.TIME_KEY + "> ?TIMESTAMP}";

		String localDbTimeForEngine = null;
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(localMaster, localDbQuery);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			localDbTimeForEngine = ss.getVar(names[0]) + "";
		}

		String engineDbTime = null;
		ISelectWrapper wrapper2 = WrapperManager.getInstance().getSWrapper( ((AbstractEngine)engineToAdd).getBaseDataEngine(), engineQuery);
		String[] names2 = wrapper2.getVariables();
		while(wrapper2.hasNext()) {
			ISelectStatement ss = wrapper2.next();
			engineDbTime = ss.getVar(names2[0]) + "";
		}

		if(engineDbTime == null) {
			DateFormat dateFormat = BaseDatabaseCreator.getFormatter();
			Calendar cal = Calendar.getInstance();
			engineDbTime = dateFormat.format(cal.getTime());
			((AbstractEngine)engineToAdd).getBaseDataEngine().doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{BaseDatabaseCreator.TIME_URL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});
			((AbstractEngine)engineToAdd).getBaseDataEngine().commit();
			try {
				((AbstractEngine)engineToAdd).getBaseDataEngine().exportDB();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (RDFHandlerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if(localDbTimeForEngine == null) {
			localMaster.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});

			AddToMasterDB adder = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			adder.registerEngineLocal(engineToAdd);
			localMaster.commit();
		} else if(!localDbTimeForEngine.equals(engineDbTime)) {
			localMaster.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, localDbTimeForEngine, false});
			localMaster.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});

			//if it has a time stamp, it means it was previously in local master
			//need to delete current information and read
			DeleteFromMasterDB remover = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			remover.deleteEngine(engineName);

			AddToMasterDB adder = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			adder.registerEngineLocal(engineToAdd);
			localMaster.commit();
		}
	}

	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst() {
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list(this);
		String[] engineNames = new String[fileNames.length];
		String localMasterDBName = Constants.LOCAL_MASTER_DB_NAME + ".smss";
		int localMasterIndex = ArrayUtilityMethods.calculateIndexOfArray(fileNames, localMasterDBName);
		if(localMasterIndex != -1) {
			String temp = fileNames[0];
			fileNames[0] = localMasterDBName;
			fileNames[localMasterIndex] = temp;
			localMasterIndex = 0;
		}
		boolean isLocal = false;
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			if(fileIdx == localMasterIndex) {
				isLocal = true;
			} else {
				isLocal = false;
			}
			try {
				String loadedEngineName = loadExistingDB(fileNames[fileIdx], isLocal);
				engineNames[fileIdx] = loadedEngineName;
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
				if(!ArrayUtilityMethods.arrayContainsValue(engineNames, engine)) {
					remover.deleteEngine(engine);
				}
			}
		}
		
		Map<String, Object> queryOptions = new HashMap<String, Object>();
		queryOptions.put(SolrIndexEngine.FACET_FIELD, SolrIndexEngine.CORE_ENGINE);
	
		try {
			if(SolrIndexEngine.getInstance().serverActive()) {
				Map<String, Map<String, Long>> facetReturn = SolrIndexEngine.getInstance().facetDocument(queryOptions);
				if(facetReturn != null && facetReturn.isEmpty()) {
					Map<String, Long> solrEngines = facetReturn.get(SolrIndexEngine.CORE_ENGINE);
					if(solrEngines != null) {
						Set<String> engineSet = solrEngines.keySet();
						for(String engine : engineSet) {
							if(!ArrayUtilityMethods.arrayContainsValue(engineNames, engine)) {
								Utility.deleteFromSolr(engine);
							}
						}
					}
				}
			}
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (KeyStoreException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
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
