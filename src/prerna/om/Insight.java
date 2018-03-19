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

import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.cache.CacheFactory;
import prerna.comments.InsightComment;
import prerna.comments.InsightCommentHelper;
import prerna.ds.h2.H2Frame;
import prerna.sablecc.PKQLRunner;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.reactor.frame.FrameFactory;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.sablecc2.reactor.imports.FileMeta;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.ga.GATracker;

public class Insight {

	private static final Logger LOGGER = LogManager.getLogger(Insight.class.getName());
	// need to account for multiple frames to be saved on the insight
	// we will use a special key 
	public static transient final String CUR_FRAME_KEY = "$CUR_FRAME_KEY";
	
	// this is the id it is assigned within the InsightCache
	// it varies from one instance of an insight to another instance of the same insight
	protected String insightId;
	protected User user;
	protected String insightName;

	// if this is a saved insight
	protected String rdbmsId;
	protected String engineName;

	// list to store the pixels that make this insight
	private List<String> pixelList;
	
	// keep a map to store various properties
	// new variable assignments in pixel are also stored here
	private transient VarStore varStore = new VarStore();

	// this is the store holding all current tasks (iterators) that are run on the
	// data frames within this insight
	private transient TaskStore taskStore;

	// we will keep a central rJavaTranslator for the entire insight
	// that can be referenced through all the reactors
	// since reactors have access to insight
	private transient AbstractRJavaTranslator rJavaTranslator;
	
	/* 
	 * TODO: find a better way of doing this
	 * keep a list of all the files that are used to create this insight
	 * this is important so we can save those files into full databases
	 * if the insight is saved
	*/
	private transient List<FileMeta> filesUsedInInsight = new Vector<FileMeta>();	
	private transient Map<String, String> exportFiles = new Hashtable<String, String>();
	
	// insight comments
	private transient LinkedList<InsightComment> insightCommentList = null;
	
	// this is the store holding information around the panels associated with this insight
	private transient Map<String, InsightPanel> insightPanels = new Hashtable<String, InsightPanel>();
	private transient Map<String, Object> insightOrnament = new Hashtable<String, Object>();

	// old - for pkql
	@Deprecated
	private transient Map<String, Map<String, Object>> pkqlVarMap = new Hashtable<String, Map<String, Object>>();
	
	// need a way to shift between old and new insights...
	// dont know how else to shift to this
	protected boolean isOldInsight = false;
	
	// GA Values
	private String prevType = null;
	private String thisPrevExpression = null;
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////// START CONSTRUCTORS //////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Create an empty insight
	 */
	public Insight() {
		loadDefaultSettings();
	}

	/**
	 * Open a saved insight
	 * @param engineName
	 * @param rdbmsId
	 */
	public Insight(String engineName, String rdbmsId) {
		this();
		this.engineName = engineName;
		this.rdbmsId = rdbmsId;
	}
	
	private void loadDefaultSettings() {
		this.pixelList = new Vector<String>();
		this.taskStore = new TaskStore();
		this.insightId = UUID.randomUUID().toString();
		
		// since we require the use of ids on the databases
		// we will add the local alias to the global unique id
//		Map<String, String> aliasToId = MasterDatabaseUtility.getEngineAliasToId();
//		for(String alias : aliasToId.keySet()) {
//			this.varStore.put(alias, new NounMetadata(aliasToId.get(alias), PixelDataType.CONST_STRING));
//		}
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////// END CONSTRUCTORS ///////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////// START EXECUTION OF PIXEL //////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////


	// run a new pixel routine
	public synchronized Map<String, Object> runPixel(String pixelString) {
		PixelRunner runner = getPixelRunner();
		LOGGER.info("Running >>> " + pixelString);
		runner.runPixel(pixelString, this);
		return collectPixelData(runner);
	}

	// run a new list of pixel routines
	public synchronized Map<String, Object> runPixel(List<String> pixelList) {
		PixelRunner runner = getPixelRunner();
		int size = pixelList.size();
		for(int i = 0; i < size; i++) {
			String pixelString = pixelList.get(i);
			LOGGER.info("Running >>> " + pixelString);
			runner.runPixel(pixelString, this);
		}
		return collectPixelData(runner);
	}

	/**
	 * 
	 * @param runner
	 * @return
	 */
	private Map<String, Object> collectPixelData(PixelRunner runner) {
		// get the return values
		List<NounMetadata> resultList = runner.getResults();
		// get the expression which created the return
		// this matches with the above by index
		List<String> pixelStrings = runner.getPixelExpressions();
		List<Boolean> isMeta = runner.isMeta();
		Map<String, String> encodedTextToOriginal = runner.getEncodedTextToOriginal();
		boolean invalidSyntax = runner.isInvalidSyntax();
		System.out.println("");
		List<Map<String, Object>> retValues = new Vector<Map<String, Object>>();
		String expression = null;
		for (int i = 0; i < pixelStrings.size(); i++) {
			NounMetadata noun = resultList.get(i);
			Map<String, Object> ret = processNounMetadata(noun);
			// get the expression which created this return
			expression = pixelStrings.get(i);
			expression = PixelUtility.recreateOriginalPixelExpression(expression, encodedTextToOriginal);
			ret.put("pixelExpression", expression);
			// save the expression for future use
			// only if it is not meta
			// and if it is not invalid syntax
			if (!isMeta.get(i) && !invalidSyntax) {
				ret.put("isMeta", false);
				this.pixelList.add(expression);
			} else {
				ret.put("isMeta", true);
			}
			// add it to the list
			retValues.add(ret);
		}
		
		Map<String, Object> retData = new Hashtable<String, Object>();
		retData.put("pixelReturn", retValues);
		retData.put("insightID", this.insightId);
		return retData;
	}

	/**
	 * 
	 * @param curType
	 * @param curExpression
	 */
	public void trackPixels(String curType, String curExpression) {
		String thisType = curType;
		String prevType = this.prevType;
		String thisExpression = curExpression;
		String thisPrevExpression = this.thisPrevExpression;
		NounMetadata session = this.getVarStore().get("$SESSION_ID");
		// session is null if opening a saved insight
		// we don't need to track these pixels again
		if (session != null) {
			String userId = (String) session.getValue();
			// fire and release...
			GATracker.getInstance().track(thisExpression, thisType, thisPrevExpression, prevType, userId);
			// set this expression as insight level previous expression
			this.thisPrevExpression = thisExpression;
			this.prevType = curType;
		}
	}

	private Map<String, Object> processNounMetadata(NounMetadata noun) {
		Map<String, Object> ret = new HashMap<String, Object>();
		if(noun.getNounType() == PixelDataType.FRAME) {
			// if we have a frame
			// return the table name of the frame
			// FE needs this to create proper QS
			// this has no meaning for graphs
			Map<String, String> frameData = new HashMap<String, String>();
			ITableDataFrame frame = (ITableDataFrame) noun.getValue();
			frameData.put("type", FrameFactory.getFrameType(frame));
			String name = frame.getTableName();
			if(name != null) {
				frameData.put("name", name);
			}
			ret.put("output", frameData);
			ret.put("operationType", noun.getOpType());
			
			// add additional outputs
			List<Map<String, Object>> additionalOutputList = new Vector<Map<String, Object>>();
			List<NounMetadata> addReturns = noun.getAdditionalReturn();
			int numOutputs = addReturns.size();
			for(int i = 0; i < numOutputs; i++) {
				additionalOutputList.add(processNounMetadata(addReturns.get(i)));
			}
			if(!additionalOutputList.isEmpty()) {
				ret.put("additionalOutput", additionalOutputList);
			}
			
			// add message
			if(noun.getExplanation() != null && !noun.getExplanation().isEmpty()) {
				ret.put("message", noun.getExplanation());
			}
		} else if(noun.getNounType() == PixelDataType.CODE || noun.getNounType() == PixelDataType.TASK_LIST) {
			// code is a tough one to process
			// since many operations could have been performed
			// we need to loop through a set of noun meta datas to output
			ret.put("operationType", noun.getOpType());
			List<Map<String, Object>> outputList = new Vector<Map<String, Object>>();
			List<NounMetadata> codeOutputs = (List<NounMetadata>) noun.getValue();
			int numOutputs = codeOutputs.size();
			for(int i = 0; i < numOutputs; i++) {
				outputList.add(processNounMetadata(codeOutputs.get(i)));
			}
			ret.put("output", outputList);
		} else {
			ret.put("output", noun.getValue());
			ret.put("operationType", noun.getOpType());
		}
		return ret;
	}
	

	
	public PixelRunner getPixelRunner() {
		PixelRunner runner = new PixelRunner();
		return runner;
	}
	
	public void addFileUsedInInsight(FileMeta fileMeta) {
		this.filesUsedInInsight.add(fileMeta);
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////// END EXECUTION OF PIXEL ///////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	public Map<String, InsightPanel> getInsightPanels() {
		return this.insightPanels;
	}
	
	public InsightPanel getInsightPanel(String panelId) {
		return this.insightPanels.get(panelId);
	}
	
	public void addNewInsightPanel(InsightPanel insightPanel) {
		this.insightPanels.put(insightPanel.getPanelId(), insightPanel);
	}
	
	public LinkedList<InsightComment> getInsightComments() {
		if(this.insightCommentList == null) {
			this.insightCommentList = InsightCommentHelper.generateInsightCommentList(this.engineName, this.rdbmsId);
		}
		return this.insightCommentList;
	}
	
	public void addInsightComment(InsightComment newComment) {
		InsightCommentHelper.addInsightCommentToList(getInsightComments(), newComment);
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// GETTERS AND SETTERS /////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	public List<String> getPixelRecipe() {
		return this.pixelList;
	}

	public void setPixelRecipe(List<String> pixelList) {
		this.pixelList = pixelList;
	}
	
	public String getInsightId() {
		return insightId;
	}

	public void setInsightId(String insightId) {
		this.insightId = insightId;
	}

	public String getUserId() {
		if(this.user == null) {
			return "-1";
		}
		return user.getId();
	}

	public User getUser(User user) {
		return this.user;
	}
	
	public void setUser(User user) {
		this.user = user;
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
	
	public void setInsightOrnament(Map<String, Object> insightOrnament) {
		this.insightOrnament = insightOrnament;
	}
	
	public Map<String, Object> getInsightOrnament() {
		return this.insightOrnament;
	}
	
	public AbstractRJavaTranslator getRJavaTranslator(Logger logger) {
		if(this.rJavaTranslator == null) {
			this.rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(this, logger);
		} else {
			this.rJavaTranslator.setLogger(logger);
		}
		return this.rJavaTranslator;
	}
	
	public TaskStore getTaskStore() {
		return this.taskStore;
	}
	
	/////////////////////////////////////////////////////////////////
	
	/*
	 * For getting file exports from the insight
	 */
	
	public void addExportFile(String uniqueKey, String fileLocation) {
		this.exportFiles.put(uniqueKey, fileLocation);
	}
	
	public String getExportFileLocation(String uniqueKey) {
		return this.exportFiles.get(uniqueKey);
	}
	
	public Map<String, String> getExportFiles() {
		return this.exportFiles;
	}
	
	/////////////////////////////////////////////////////////////////
	
	// TODO: need a better way of doing this...
	// need to keep track of files that made this insight
	public void setFilesUsedInInsight(List<FileMeta> filesUsed) {
		this.filesUsedInInsight = filesUsed;
	}

	public List<FileMeta> getFilesUsedInInsight() {
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
	
	public void setDataMaker(IDataMaker datamaker) {
		this.varStore.put(CUR_FRAME_KEY, new NounMetadata(datamaker, PixelDataType.FRAME, PixelOperationType.FRAME));
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
	
	
	
	
	
	
	
	
	
	
	
	
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////// START EXECUTION OF PKQL ///////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	@Deprecated
	public Map<String, Object> runPkql(String pkqlString) {
		PKQLRunner runner = getPkqlRunner();
		try {
			LOGGER.info("Running >>> " + pkqlString);
			// we need to account for the fact that the data.output
			// will create a completely new insight object
			// so even though we add the reactors
			// we end up with a new translation that needs them again
			if(this.getDataMaker() != null) {
				runner.runPKQL(pkqlString, this.getDataMaker());
			} else {
				// ugh... i dont like having to have a h2frame...
				// but FE never adds data.frame(grid)
				runner.runPKQL(pkqlString, new H2Frame());
			}
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error with " + pkqlString + "\n" + e.getMessage());
		}
		this.pixelList.add(pkqlString);
		return collectPkqlResults(runner);
	}

	// run a new list of pkql routines
	@Deprecated
	public Map<String, Object> runPkql(List<String> pqlList) {
		PKQLRunner runner = getPkqlRunner();
		int size = pqlList.size();
		for(int i = 0; i < size; i++) {
			String pkqlString = pqlList.get(i);
			try {
				LOGGER.info("Running >>> " + pkqlString);
				if(this.getDataMaker() != null) {
					runner.runPKQL(pkqlString, this.getDataMaker());
				} else {
					// ugh... i dont like having to have a h2frame...
					// but FE never adds data.frame(grid)
					runner.runPKQL(pkqlString, new H2Frame());
				}
			} catch(Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Error with " + pkqlString + "\n" + e.getMessage());
			}
			this.pixelList.add(pkqlString);
		}
		return collectPkqlResults(runner);
	}
	
	/**
	 * A routine to grab all the random data we need for the previously run insight pixel routines
	 * @return
	 */
	@Deprecated
	private Map<String, Object> collectPkqlResults(PKQLRunner pkqlRunner) {
		Map<String, Object> returnObj = new HashMap<String, Object>();
		
		// add insight information
		returnObj.put("insightID", this.insightId);
		IDataMaker datamaker = pkqlRunner.getDataFrame();
		if(datamaker != null) {
			returnObj.put("dataID", datamaker.getDataId());
			// TODO: just cause i want as many things to be in future state as possible
			this.varStore.put(CUR_FRAME_KEY, new NounMetadata(datamaker, PixelDataType.FRAME, PixelOperationType.FRAME));
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
//		parseMetadataResponse(pkqlRunner.getMetadataResponse());
		
		// store the varmap after the operation is done
		this.pkqlVarMap = pkqlRunner.getVarMap();
		
		return returnObj;
	}
	
//	// this is literally just aggregating the respones that i care about
//	// at the moment, i only care about those pertaining to files
//	// since i need to grab this info to save a full engine when this engine is saved
//	// using those files
//	private void parseMetadataResponse(List<IPkqlMetadata> metadataResponse) {
//		for(IPkqlMetadata meta : metadataResponse) {
//			if(meta instanceof FilePkqlMetadata) {
//				this.filesUsedInInsight.add( (FilePkqlMetadata) meta);
//			}
//		}
//	}
	
	@Deprecated
	public Map<String, Object> reRunInsight() {
		// just clear the varStore
		// TODO: need to do better clean up
		// like actually removing the data makers so we do not 
		// have too much in memory
		this.varStore.clear();
		this.insightPanels.clear();
		return runPkql(this.pixelList);
	}
	
	public Map<String, Object> reRunPixelInsight() {
		// just clear the varStore
		// TODO: need to do better clean up
		// like actually removing the data makers so we do not 
		// have too much in memory
		Set<String> keys = this.varStore.getKeys();
		for(String key : keys) {
			NounMetadata noun = this.varStore.get(key);
			if(noun.getValue() instanceof H2Frame) {
				H2Frame frame = (H2Frame) noun.getValue();
				frame.dropTable();
				if(!frame.isInMem()) {
					frame.dropOnDiskTemporalSchema();
				}
			}
		}
		
		// copy over the recipe to a new list
		// and clear the current container
		List<String> newList = new Vector<String>();
		newList.addAll(this.pixelList);
		this.pixelList.clear();
		
		// clear the var store
		this.varStore.clear();
		// clear the panels
		this.insightPanels.clear();
		
		return runPixel(newList);
	}
	
	/**
	 * Get a new instance of the pkql runner
	 * @return
	 */
	@Deprecated
	public PKQLRunner getPkqlRunner() {
		PKQLRunner runner = new PKQLRunner();
		runner.setInsightId(this.insightId);
		runner.setVarMap(this.pkqlVarMap);
		return runner;
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////// END EXECUTION OF PKQL ////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Load any cached objects relating to this insight
	 */
	@Deprecated
	public void loadInsightCache() {
		IDataMaker dm = CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).getDMCache(this);
		if(dm != null) {
			this.varStore.put(CUR_FRAME_KEY, new NounMetadata(dm, PixelDataType.FRAME, PixelOperationType.FRAME));
		}
		CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).getRCache(this);
	}

	
}
