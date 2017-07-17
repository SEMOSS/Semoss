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
package prerna.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.CacheFactory;
import prerna.ds.h2.H2Frame;
import prerna.sablecc.PKQLRunner;
import prerna.sablecc.meta.FilePkqlMetadata;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc2.PKSLRunner;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.sablecc2.om.VarStore;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.util.Constants;
import prerna.util.Utility;

public class Insight {

	private static final Logger LOGGER = LogManager.getLogger(Insight.class.getName());

	// this is the id it is assigned within the InsightCache
	// it varies from one instance of an insight to another instance of the same insight
	protected String insightId;
	protected String userId;
	protected String insightName;

	// if this is a saved insight
	// TODO: i feel like we need a central insight db... 
	protected String rdbmsId;
	protected String engineName;

	// keep a map to store various properties
	// this is passed into the pksl runner as well
	// so new assignments are also stored there
	private transient VarStore varStore = new VarStore();

	// need to account for multiple frames to be saved on the insight
	// we will use a special key 
	public static transient final String CUR_FRAME_KEY = "$CUR_FRAME_KEY";

	// ugh... going to write this for both 
//	private transient PKQLRunner pkqlRunner;
	private transient Map<String, Map<String, Object>> pkqlVarMap = new Hashtable<String, Map<String, Object>>();
//	private transient PKSLRunner pkslRunner;

	// list to store the pksls that make this insight
	// TODO: right now using this for both pkql and pksl
	private List<String> pkslList;

	// need a way to shift between old and new insights...
	// dont know how else to shift to this
	protected boolean isOldInsight = false;
	
	// I need to keep track of the layout here
	// otherwise, dashboard will not work...
	protected String layout;

	/* 
	 * TODO: find a better way of doing this
	 * keep a list of all the files that are used to create this insight
	 * this is important so we can save those files into full databases
	 * if the insight is saved
	*/
	private transient List<FilePkqlMetadata> filesUsedInInsight = new Vector<FilePkqlMetadata>();	
	
	private transient Map<String, InsightPanel> insightPanels = new Hashtable<String, InsightPanel>();
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////// START CONSTRUCTORS //////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Create an empty insight
	 */
	public Insight() {
		this.pkslList = new Vector<String>();
	}

	/**
	 * Open a saved insight
	 * @param engineName
	 * @param rdbmsId
	 */
	public Insight(String engineName, String rdbmsId) {
		super();
		this.engineName = engineName;
		this.rdbmsId = rdbmsId;
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////// END CONSTRUCTORS ///////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////// START EXECUTION OF PKQL ///////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	public Map<String, Object> runPkql(String pkslString) {
		PKQLRunner runner = getPkqlRunner();
		runner.setInsightId(pkslString);
		try {
			LOGGER.info("Running >>> " + pkslString);
			// we need to account for the fact that the data.output
			// will create a completely new insight object
			// so even though we add the reactors
			// we end up with a new translation that needs them again
			if(this.getDataMaker() != null) {
				runner.runPKQL(pkslString, this.getDataMaker());
			} else {
				// ugh... i dont like having to have a h2frame...
				// but FE never adds data.frame(grid)
				runner.runPKQL(pkslString, new H2Frame());
			}
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error with " + pkslString + "\n" + e.getMessage());
		}
		this.pkslList.add(pkslString);
		return collectPkqlResults(runner);
	}

	// run a new list of pksl routines
	public Map<String, Object> runPkql(List<String> pkslList) {
		PKQLRunner runner = getPkqlRunner();
		int size = pkslList.size();
		for(int i = 0; i < size; i++) {
			String pkslString = pkslList.get(i);
			try {
				LOGGER.info("Running >>> " + pkslString);
				if(this.getDataMaker() != null) {
					runner.runPKQL(pkslString, this.getDataMaker());
				} else {
					// ugh... i dont like having to have a h2frame...
					// but FE never adds data.frame(grid)
					runner.runPKQL(pkslString, new H2Frame());
				}
			} catch(Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Error with " + pkslString + "\n" + e.getMessage());
			}
			this.pkslList.add(pkslString);
		}
		return collectPkqlResults(runner);
	}
	
	/**
	 * A routine to grab all the random data we need for the previously run insight pksl routines
	 * @return
	 */
	private Map<String, Object> collectPkqlResults(PKQLRunner pkqlRunner) {
		Map<String, Object> returnObj = new HashMap<String, Object>();
		
		// add insight information
		returnObj.put("insightID", this.insightId);
		IDataMaker datamaker = pkqlRunner.getDataFrame();
		if(datamaker != null) {
			returnObj.put("dataID", datamaker.getDataId());
			// TODO: just cause i want as many things to be in future state as possible
			this.varStore.put(CUR_FRAME_KEY, new NounMetadata(datamaker, PkslDataTypes.FRAME, PkslOperationTypes.FRAME));
		}
		
		// add the pkql data
		returnObj.put("newColumns", pkqlRunner.getNewColumns());
		returnObj.put("newInsights", pkqlRunner.getNewInsights());
		returnObj.put("clear", pkqlRunner.getDataClear());
		returnObj.put("pkqlData", pkqlRunner.getResults());
		returnObj.put("feData", pkqlRunner.getFeData());
		
		// ... in case this is some dashboard object
		if(pkqlRunner.getDashboardData() != null) {
			Map dashboardMap = new HashMap();
			dashboardMap.putAll((Map)pkqlRunner.getDashboardData());
			returnObj.put("Dashboard", dashboardMap);
		}
		
		// need to grab the metadata
		// in case there is a file used that i need to keep track of
		// if this insight is later saved
		parseMetadataResponse(pkqlRunner.getMetadataResponse());
		
		// store the varmap after the operation is done
		this.pkqlVarMap = pkqlRunner.getVarMap();
		
		return returnObj;
	}
	
	// this is literally just aggregating the respones that i care about
	// at the moment, i only care about those pertaining to files
	// since i need to grab this info to save a full engine when this engine is saved
	// using those files
	private void parseMetadataResponse(List<IPkqlMetadata> metadataResponse) {
		for(IPkqlMetadata meta : metadataResponse) {
			if(meta instanceof FilePkqlMetadata) {
				this.filesUsedInInsight.add( (FilePkqlMetadata) meta);
			}
		}
	}
	
	public Map<String, Object> reRunInsight() {
		// just clear the varStore
		// TODO: need to do better clean up
		// like actually removing the data makers so we do not 
		// have too much in memory
		this.varStore.clear();
		this.insightPanels.clear();
		return runPkql(this.pkslList);
	}
	
	public Map<String, Object> reRunPkslInsight() {
		// just clear the varStore
		// TODO: need to do better clean up
		// like actually removing the data makers so we do not 
		// have too much in memory

		Set<String> keys = this.varStore.getKeys();
		for(String key : keys) {
			NounMetadata noun = this.varStore.get(key);
			if(noun.getValue() instanceof H2Frame) {
				H2Frame frame = (H2Frame) noun.getValue();
				frame.closeRRunner();
				frame.dropTable();
				if(!frame.isInMem()) {
					frame.dropOnDiskTemporalSchema();
				}
			}
		}
		this.varStore.clear();
		this.insightPanels.clear();
		return runPksl(this.pkslList);
	}
	
	/**
	 * Get a new instance of the pkql runner
	 * @return
	 */
	public PKQLRunner getPkqlRunner() {
		PKQLRunner runner = new PKQLRunner();
		runner.setInsightId(this.insightId);
		runner.setVarMap(this.pkqlVarMap);
		return runner;
	}
	
	public Map<String, InsightPanel> getInsightPanels() {
		return this.insightPanels;
	}
	
	public InsightPanel getInsightPanel(String panelId) {
		return this.insightPanels.get(panelId);
	}
	
	public void addNewInsightPanel(InsightPanel insightPanel) {
		this.insightPanels.put(insightPanel.getPanelId(), insightPanel);
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////// END EXECUTION OF PKQL ////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Load any cached objects relating to this insight
	 */
	public void loadInsightCache() {
		IDataMaker dm = CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).getDMCache(this);
		if(dm != null) {
			this.varStore.put(CUR_FRAME_KEY, new NounMetadata(dm, PkslDataTypes.FRAME, PkslOperationTypes.FRAME));
		}
		CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).getRCache(this);
	}

	// TODO ::: below is when I shift from PKQL to PKSL
	// TODO ::: below is when I shift from PKQL to PKSL
	// TODO ::: below is when I shift from PKQL to PKSL
	// TODO ::: below is when I shift from PKQL to PKSL
	// TODO ::: below is when I shift from PKQL to PKSL

	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////// START EXECUTION OF PKSL ///////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	
	// run a new pksl routine
	public Map<String, Object> runPksl(String pkslString) {
		PKSLRunner runner = getPkslRunner();
		try {
			LOGGER.info("Running >>> " + pkslString);
			runner.runPKSL(pkslString, this);
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error with " + pkslString + "\n" + e.getMessage());
		}
		this.pkslList.add(pkslString);
		return collectPkslData(runner);
	}

	// run a new list of pksl routines
	public Map<String, Object> runPksl(List<String> pkslList) {
		PKSLRunner runner = getPkslRunner();
		int size = pkslList.size();
		for(int i = 0; i < size; i++) {
			String pkslString = pkslList.get(i);
			try {
				LOGGER.info("Running >>> " + pkslString);
				runner.runPKSL(pkslString, this);
			} catch(Exception e) {
				throw new IllegalArgumentException("Error with " + pkslString + "\n" + e.getMessage());
			}
			this.pkslList.add(pkslString);
		}
		return collectPkslData(runner);
	}
	
	/**
	 * 
	 * @param runner
	 * @return
	 */
	private Map<String, Object> collectPkslData(PKSLRunner runner) {
		Map<String, Object> retData = new Hashtable<String, Object>();
		// get the return values
		List<NounMetadata> resultList = runner.getResults();
		// get the expression which created the return
		// this matches with the above by index
		List<String> pkslStrings = runner.getPkslExpressions();
		
		List<Map<String, Object>> retValues = new Vector<Map<String, Object>>();
		for(int i = 0; i < pkslStrings.size(); i++) {
			Map<String, Object> ret = new HashMap<String, Object>();
			
			// get the value to send to the FE
			
			NounMetadata noun = resultList.get(i);
			if(noun.getNounType() == PkslDataTypes.FRAME) {
				// if we have a frame
				// return the table name of the frame
				// FE needs this to create proper QS
				// this has no meaning for graphs
				String name = ((ITableDataFrame) noun.getValue()).getTableName();
				if(name == null) {
					ret.put("output", "Created frame of type " + ((ITableDataFrame) noun.getValue()).getDataMakerName());
				} else {
					ret.put("output", name);
				}
				ret.put("operationType", noun.getOpType());
			} else {
				ret.put("output", noun.getValue());
				ret.put("operationType", noun.getOpType());
			}
			
			// get the expression which created this return
			String expression = pkslStrings.get(i);
			ret.put("pkslExpression", expression);
			
			retValues.add(ret);
		}
		retData.put("pkslReturn", retValues);
		retData.put("insightID", this.insightId);
		return retData;
	}
	
	public PKSLRunner getPkslRunner() {
		PKSLRunner runner = new PKSLRunner();
		return runner;
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////// END EXECUTION OF PKSL ////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	
	// TODO: this is way too convoluted
	// need a more simplistic way of doing this
	public Map<String, Object> getInsightMetaModel() {
		IDataMaker dataMaker = getDataMaker();
		if(!(dataMaker instanceof ITableDataFrame)) {
			throw new IllegalArgumentException("This Insight is not eligible to navigate through Explore. The data maker is not of type ITableDataFrame.");
		}
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();
		Map<String, Object> nodesHash = new Hashtable<String, Object>();
		Map<String, Object> triplesHash = new Hashtable<String, Object>();
		if(this.getDataMaker() instanceof ITableDataFrame){
			ITableDataFrame tink = (ITableDataFrame) this.getDataMaker();
			Map<String, Set<String>> edgeHash = tink.getEdgeHash();
			Map<String, String> props = tink.getProperties();
			
			for(String sub: edgeHash.keySet()){
				Map<String, Object> nodeObj = new HashMap<String, Object>();
				Set<String> subEngineNameSet = tink.getEnginesForUniqueName(sub);
				// this is for the FE s.t. it doesn't break when no engines are sent back
				if(subEngineNameSet == null || subEngineNameSet.isEmpty()) {
					subEngineNameSet = new HashSet<String>();
					subEngineNameSet.add(Constants.LOCAL_MASTER_DB_NAME);
				}
				nodeObj.put("engineName", subEngineNameSet);
				HashMap<String, String> engineToSubPhysicalMap = new HashMap<String, String>();
				for (String engine : subEngineNameSet) {
					String physicalUri = tink.getPhysicalUriForNode(sub, engine);
					String engineDisplay = null;
					if(physicalUri.startsWith("http://semoss.org/ontologies/Relation/Contains/")) {
						String trimUri = physicalUri.replace("http://semoss.org/ontologies/Relation/Contains/", "");
						//TODO: because of different storage between OWL for RDF and RDBMS
						if(trimUri.contains("/")) {
							engineDisplay = trimUri.substring(0, trimUri.indexOf("/"));
						} else {
							engineDisplay = trimUri;
						}
					} else {
						engineDisplay = Utility.getInstanceName(physicalUri);
					}
					engineToSubPhysicalMap.put(engine, engineDisplay);
				}
				nodeObj.put("engineToPhysical", engineToSubPhysicalMap);
				if(props.containsKey(sub)){
					nodeObj.put("prop", props.get(sub));
				}
				nodesHash.put(sub, nodeObj);

				Set<String> objs = edgeHash.get(sub);
				for(String obj : objs){
					Map<String, Object> nodeObj2 = new HashMap<String, Object>();
					//						nodeObj2.put("uri", obj);
					Set<String> objEngineNameSet = tink.getEnginesForUniqueName(obj);
					// this is for the FE s.t. it doesn't break when no engines are sent back
					if(objEngineNameSet == null || objEngineNameSet.isEmpty()) {
						objEngineNameSet = new HashSet<String>();
						objEngineNameSet.add(Constants.LOCAL_MASTER_DB_NAME);
					}
					nodeObj2.put("engineName", objEngineNameSet);
					HashMap<String, String> engineToObjPhysicalMap = new HashMap<String, String>();
					for (String engine : objEngineNameSet) {
						String physicalUri = tink.getPhysicalUriForNode(obj, engine);
						String engineDisplay = null;
						if(physicalUri.startsWith("http://semoss.org/ontologies/Relation/Contains/")) {
							String trimUri = physicalUri.replace("http://semoss.org/ontologies/Relation/Contains/", "");
							//TODO: because of different storage between OWL for RDF and RDBMS
							if(trimUri.contains("/")) {
								engineDisplay = trimUri.substring(0, trimUri.indexOf("/"));
							} else {
								engineDisplay = trimUri;
							}
						} else {
							engineDisplay = Utility.getInstanceName(physicalUri);
						}
						engineToObjPhysicalMap.put(engine, engineDisplay);
					}
					nodeObj2.put("engineToPhysical", engineToObjPhysicalMap);
					if(props.containsKey(obj)){
						nodeObj2.put("prop", props.get(obj));
					}
					nodesHash.put(obj, nodeObj2);

					Map<String, String> nodeTriples = new Hashtable<String, String>();
					nodeTriples.put("fromNode", sub);
					nodeTriples.put("relationshipTriple", "fake");
					nodeTriples.put("toNode", obj);
					triplesHash.put(triplesHash.size()+"", nodeTriples);
				}
			}
		}
		//		}
		returnHash.put("nodes", nodesHash); // Nodes that will be used to build the metamodel in Single-View
		returnHash.put("triples", triplesHash);

		returnHash.put("insightID", this.insightId);

		return returnHash;
	}
	

	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// GETTERS AND SETTERS /////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	public List<String> getPkslRecipe() {
		return this.pkslList;
	}

	public void setPkslRecipe(List<String> pkslList) {
		this.pkslList = pkslList;
	}
	
	public String getInsightId() {
		return insightId;
	}

	public void setInsightId(String insightId) {
		this.insightId = insightId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getRdbmsId() {
		return rdbmsId;
	}

	public void setRdbmsId(String rdbmsId) {
		this.rdbmsId = rdbmsId;
	}

	public String getEngineName() {
		return engineName;
	}

	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	public String getInsightName() {
		return insightName;
	}

	public void setInsightName(String insightName) {
		this.insightName = insightName;
	}
	
	public VarStore getVarStore() {
		return this.varStore;
	}
	
	public void setVarStore(VarStore varStore) {
		this.varStore = varStore;
	}
	

	// TODO: need a better way of doing this...
	// need to keep track of files that made this insight
	public List<FilePkqlMetadata> getFilesUsedInInsight() {
		return this.filesUsedInInsight;
	}

	// TODO: methods i have but dont want to keep
	// TODO: methods i have but dont want to keep
	// TODO: methods i have but dont want to keep
	// TODO: methods i have but dont want to keep
	// TODO: methods i have but dont want to keep
	// TODO: methods i have but dont want to keep
	// TODO: methods i have but dont want to keep
	// TODO: methods i have but dont want to keep
	
	public void setIsOldInsight(boolean isOldInsight) {
		this.isOldInsight = isOldInsight;
	}
	
	public boolean isOldInsight() {
		return this.isOldInsight;
	}
	
	public IDataMaker getDataMaker() {
		NounMetadata curFrameNoun = this.varStore.get(CUR_FRAME_KEY);
		if(curFrameNoun != null) {
			return ((IDataMaker) curFrameNoun.getValue());
		}
		return null;
	}
	
	// currently only used via newdashboard in NameServer
	public void setDataMaker(IDataMaker datamaker) {
		this.varStore.put(CUR_FRAME_KEY, new NounMetadata(datamaker, PkslDataTypes.FRAME, PkslOperationTypes.FRAME));
	}
	
	public String getDataMakerName() {
		NounMetadata curFrameNoun = this.varStore.get(CUR_FRAME_KEY);
		if(curFrameNoun != null) {
			return ((IDataMaker) curFrameNoun.getValue()).getDataMakerName();
		}
		// TODO: how do i handle this???
		// might not be a grid in reality
		// causing issues since we want to load the data maker before we execute anything
		// but if we have multiple frames, we need to be smarter about how we do this
		return "H2Frame";
	}

	public Map<String, Object> getWebData() {
		return null;
	}

	public String getOrder() {
		return "0";
	}

	//TODO: need a way to run the analytical routines which were never converted to pkql
	public List<Object> processActions(List<ISEMOSSAction> actions) {
		LOGGER.info("We are processing " + actions.size() + " actions");
		List<Object> outputs = new ArrayList<Object>();
		for(ISEMOSSAction action : actions){
			action.setDataMakers(this.getDataMaker());
			action.setId("1");
			outputs.add(action.runMethod());
		}
		return outputs;
	}
	
	// need this for dashboard!
	public String getOutput() {
		return this.layout;
	}
	
	public void setOutput(String output) {
		this.layout = output;
	}

	// this is for the way current dashboards are done
//	public void setParentInsight(Insight parentInsight) {
//		this.setParentInsight = parentInsight;
//	}
}
