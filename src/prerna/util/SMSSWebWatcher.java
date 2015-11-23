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
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.h2.jdbc.JdbcClob;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.MasterDBHelper;
import prerna.om.SEMOSSParam;
import prerna.poi.main.BaseDatabaseCreator;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.solr.SolrDocumentExportWriter;
import prerna.solr.SolrIndexEngine;

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
		loadNewDB(fileName, false);
	}

	/**
	 * Returns an array of strings naming the files in the directory. Goes through list and loads an existing database.
	 */
	public void loadExistingDB(String fileName, boolean isLocal) {
		loadNewDB(fileName, isLocal);
	}

	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public void loadNewDB(String newFile, boolean isLocal) {
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
				IEngine engineLoaded = Utility.loadEngine(fileName, prop);
				boolean hidden = (prop.getProperty(Constants.HIDDEN_DATABASE) != null && Boolean.parseBoolean(prop.getProperty(Constants.HIDDEN_DATABASE)));
				if(!isLocal && !hidden) {
					addToLocalMaster(engineLoaded);
					addToSolr(engineLoaded);
				} else if(!isLocal){ // never add local master to itself...
					DeleteFromMasterDB deleter = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
					deleter.deleteEngine(engineName);
					deleteFromSolr(engineName);
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

	private void addToSolr(IEngine engineToAdd) {
		SolrIndexEngine solrE = null;
		SolrDocumentExportWriter writer = null;
		try {
			solrE = SolrIndexEngine.getInstance();
			if(solrE.serverActive()) {
				String engineName = engineToAdd.getEngineName();

				// check if should always recreate and check if db currently exists
				if(AbstractEngine.RECREATE_SOLR || solrE.containsEngine(engineName)) {
				
					String folderPath = DIHelper.getInstance().getProperty("BaseFolder");
					folderPath = folderPath + "\\db\\" + engineName + "\\";
					String fileName = engineName + "_Solr.txt";
					File file = new File(folderPath + fileName);
					if (!file.exists()) {
						try {
							file.createNewFile();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
	
					try {
						writer = new SolrDocumentExportWriter(file);
					} catch (IOException e1) {
						e1.printStackTrace();
					}
	
					DateFormat dateFormat = SolrIndexEngine.getDateFormat();
					Date date = new Date();
					String currDate = dateFormat.format(date);
					String userID = "default";
					String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT, QUESTION_MAKEUP FROM QUESTION_ID";
	
					solrE.deleteEngine(engineName);
	
					// query the current insights in this db
					ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engineToAdd.getInsightDatabase(), query);
					while(wrapper.hasNext()){
						ISelectStatement ss = wrapper.next();
						int id = (int) ss.getVar("ID");
						String name = (String) ss.getVar("QUESTION_NAME");
						String layout = (String) ss.getVar("QUESTION_LAYOUT");
	
						JdbcClob obj = (JdbcClob) ss.getVar("QUESTION_MAKEUP"); 
						InputStream makeup = null;
						try {
							makeup = obj.getAsciiStream();
						} catch (SQLException e) {
							e.printStackTrace();
						}
	
						//load the makeup inputstream into a rc
						RepositoryConnection rc = null;
						try {
							Repository myRepository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()));
							myRepository.initialize();
							rc = myRepository.getConnection();
							rc.add(makeup, "semoss.org", RDFFormat.NTRIPLES);
						} catch (RuntimeException ignored) {
							ignored.printStackTrace();
						} catch (RDFParseException e) {
							e.printStackTrace();
						} catch (RepositoryException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						// set the rc in the in-memory engine
						InMemorySesameEngine myEng = new InMemorySesameEngine();
						myEng.setRepositoryConnection(rc);
	
						List<String> engineList = new ArrayList<String>();
						//Query the engine
						String engineQuery = "SELECT DISTINCT ?EngineName WHERE {{?Engine a <http://semoss.org/ontologies/Concept/Engine>}{?Engine <http://semoss.org/ontologies/Relation/Contains/Name> ?EngineName} }";
						ISelectWrapper engineWrapper = WrapperManager.getInstance().getSWrapper(myEng, engineQuery);
						while(engineWrapper.hasNext()) {
							ISelectStatement engineSS = engineWrapper.next();
							engineList.add(engineSS.getVar("EngineName") + "");
						}
	
						List<String> paramList = new ArrayList<String>();
						List<SEMOSSParam> params = engineToAdd.getParams(id + "");
						if(params != null && !params.isEmpty()) {
							for(SEMOSSParam p : params) {
								paramList.add(p.getName());
							}
						}
	
						// as you get each result, add the insight as a document in the solr index engine
						Map<String, Object>  queryResults = new  HashMap<> ();
						queryResults.put(SolrIndexEngine.NAME, name);
						queryResults.put(SolrIndexEngine.CREATED_ON, currDate);
						queryResults.put(SolrIndexEngine.MODIFIED_ON, currDate);
						queryResults.put(SolrIndexEngine.USER_ID, userID);
						queryResults.put(SolrIndexEngine.ENGINES, engineList);
						queryResults.put(SolrIndexEngine.PARAMS, paramList);
						queryResults.put(SolrIndexEngine.CORE_ENGINE, engineName);
						queryResults.put(SolrIndexEngine.CORE_ENGINE_ID, id);
						queryResults.put(SolrIndexEngine.LAYOUT, layout);
	
						try {
							solrE.addDocument(engineName + "_" + id, queryResults);
							writer.writeSolrDocument(file, engineName + "_" + id, queryResults);
						} catch (Exception e) {
							e.printStackTrace();
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
		} finally {
			//close writer
			if(writer != null) {
				writer.closeExport();
			}
		}
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
				loadExistingDB(fileNames[fileIdx], isLocal);
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
					deleteFromSolr(engine);
				}
			}
		}
	}

	private void deleteFromSolr(String engineName) {
		try {
			SolrIndexEngine.getInstance().deleteEngine(engineName);
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (KeyStoreException e) {
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
