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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cache.CacheFactory;
import prerna.comments.InsightComment;
import prerna.comments.InsightCommentHelper;
import prerna.ds.h2.H2Frame;
import prerna.ds.py.PyExecutorThread;
import prerna.engine.impl.SaveInsightIntoWorkspace;
import prerna.sablecc.PKQLRunner;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.sablecc2.reactor.imports.FileMeta;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.sablecc2.reactor.workflow.GetOptimizedRecipeReactor;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class Insight {

	private static final Logger LOGGER = LogManager.getLogger(Insight.class.getName());
	// need to account for multiple frames to be saved on the insight
	// we will use a special key 
	public static transient final String CUR_FRAME_KEY = "$CUR_FRAME_KEY";
	
	// this is the id it is assigned within the InsightCache
	// it varies from one instance of an insight to another instance of the same insight
	protected String insightId;

	// new user object
	protected User user;
	protected String insightName;

	// if this is a saved insight
	protected String rdbmsId;
	protected String engineId;
	protected String engineName;
	protected boolean cacheable = true;
	
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
	private transient PyExecutorThread jepThread = null;
	
	private transient SaveInsightIntoWorkspace workspaceCacheThread = null;
	private transient boolean cacheInWorkspace = false;
	
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
	private transient Map<String, InsightPanel> insightPanels = new LinkedHashMap<String, InsightPanel>();
	private transient Map<String, Object> insightOrnament = new Hashtable<String, Object>();

	// old - for pkql
	@Deprecated
	private transient Map<String, Map<String, Object>> pkqlVarMap = new Hashtable<String, Map<String, Object>>();
	
	// need a way to shift between old and new insights...
	// dont know how else to shift to this
	protected boolean isOldInsight = false;
	
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
	 * @param engineId
	 * @param rdbmsId
	 */
	public Insight(String engineId, String engineName, String rdbmsId) {
		this(engineId, engineName, rdbmsId, true);
	}
	
	public Insight(String engineId, String engineName, String rdbmsId, boolean cacheable) {
		this();
		this.engineId = engineId;
		this.engineName = engineName;
		this.rdbmsId = rdbmsId;
		this.cacheable = cacheable;
	}
	
	private void loadDefaultSettings() {
		this.pixelList = new Vector<String>();
		this.taskStore = new TaskStore();
		this.insightId = UUID.randomUUID().toString();
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

	public synchronized PixelRunner runPixel(String pixelString) {
		List<String> pixelList = new Vector<String>();
		pixelList.add(pixelString);
		return runPixel(pixelList);
	}

	public synchronized PixelRunner runPixel(List<String> pixelList) {
		PixelRunner runner = getPixelRunner();
		int size = pixelList.size();
		if(size == 0) {
			// set the insight in the runner as it is used
			// to flush to FE
			runner.setInsight(this);
		} else {
			for(int i = 0; i < size; i++) {
				String pixelString = pixelList.get(i);
				LOGGER.info("Running >>> " + pixelString);
				try {
					runner.runPixel(pixelString, this);
				} catch(SemossPixelException e) {
					if(!e.isContinueThreadOfExecution()) {
						break;
					}
				} finally {
					// TODO: uncomment for default user saving of workspace
					// TODO: uncomment for default user saving of workspace
					// TODO: uncomment for default user saving of workspace
					// TODO: uncomment for default user saving of workspace
					// TODO: uncomment for default user saving of workspace
//					if(AbstractSecurityUtils.securityEnabled() && cacheInWorkspace) {
//						getWorkspaceCacheThread().addToQueue(this.pixelList);
//					}
				}
			}
		}
		// track the pixels
		this.trackPixel(runner);
		// return
		return runner;
	}
	
	private void trackPixel(PixelRunner runner) {
		IUserTracker tracker = UserTrackerFactory.getInstance();
		if(tracker.isActive()) {
			List<String> pixelStrings = runner.getPixelExpressions();
			List<Boolean> isMeta = runner.isMeta();
			for(int i = 0; i < pixelStrings.size(); i++) {
				String expression = pixelStrings.get(i);
				boolean meta = isMeta.get(i);
				tracker.trackPixelExecution(this, expression, meta);
			}
		}
	}

	public PixelRunner getPixelRunner() {
		PixelRunner runner = new PixelRunner();
		return runner;
	}
	
	public void addFileUsedInInsight(FileMeta fileMeta) {
		this.filesUsedInInsight.add(fileMeta);
	}
	
	private SaveInsightIntoWorkspace getWorkspaceCacheThread() {
		if(this.workspaceCacheThread == null && this.user != null && this.user.isLoggedIn()) {
			String worksapceId = this.user.getWorkspaceEngineId(this.user.getPrimaryLogin());
			if(worksapceId != null) {
				this.workspaceCacheThread = new SaveInsightIntoWorkspace(worksapceId, this.insightName);
			}
		}
		return this.workspaceCacheThread;
	}
	
	public void setCacheInWorkspace(boolean cacheInWorkspace) {
		this.cacheInWorkspace = cacheInWorkspace;
	}
	
	public void dropWorkspaceCache() {
		if(this.workspaceCacheThread != null) {
			this.workspaceCacheThread.killThread();
			this.workspaceCacheThread.dropWorkspaceCache();
		}
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////// END EXECUTION OF PIXEL ///////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	public Map<String, InsightPanel> getInsightPanels() {
		return this.insightPanels;
	}
	
	public void setInsightPanels(Map<String, InsightPanel> insightPanels) {
		this.insightPanels = insightPanels;
	}
	
	public InsightPanel getInsightPanel(String panelId) {
		return this.insightPanels.get(panelId);
	}
	
	public void addNewInsightPanel(InsightPanel insightPanel) {
		this.insightPanels.put(insightPanel.getPanelId(), insightPanel);
	}
	
	public LinkedList<InsightComment> getInsightComments() {
		if(this.insightCommentList == null) {
			this.insightCommentList = InsightCommentHelper.generateInsightCommentList(this.engineId, this.rdbmsId);
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
	
	/**
	 * This method returns the optimized pixel recipe
	 * 
	 * @return modifiedRecipe
	 */
	public List<String> getOptimizedPixelRecipe() {
		GetOptimizedRecipeReactor optimizer = new GetOptimizedRecipeReactor();
		List<String> recipe = optimizer.getOptimizedRecipe(this.pixelList);
		return recipe;
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

	public String getUserId(AuthProvider provider) {
		if(this.user == null) {
			return "-1";
		}
		return user.getAccessToken(provider).getId();
	}
	
	public String getUserId() {
		if(this.user == null) {
			return "-1";
		}
		return user.getAccessToken(user.getLogins().get(0)).getId();
	}

	public void setUser(User user) {
		this.user = user;
	}

	public User getUser() {
		return this.user;
	}

	public String getRdbmsId() {
		return rdbmsId;
	}

	public void setRdbmsId(String rdbmsId) {
		this.rdbmsId = rdbmsId;
	}

	public String getEngineId() {
		return engineId;
	}

	public void setEngineId(String engineId) {
		this.engineId = engineId;
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
	
	public boolean isCacheable() {
		return this.cacheable;
	}
	
	public void setCacheable(boolean cacheable) {
		this.cacheable = cacheable;
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
	
	public void setPy(PyExecutorThread jepThread) {
		this.jepThread = jepThread;
	}
	
	public PyExecutorThread getPy() {
		return this.jepThread;
	}
	
	public TaskStore getTaskStore() {
		return this.taskStore;
	}
	
	public void setTaskStore(TaskStore taskStore) {
		this.taskStore = taskStore;
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
	
	@Deprecated
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
	
	/**
	 * re-run the optimized version of the pixel recipe
	 * @return runPixel(newList) -- returns pixel data
	 */
	public PixelRunner reRunOptimizedPixelInsight() {
		Set<String> keys = this.varStore.getKeys();
		for(String key : keys) {
			NounMetadata noun = this.varStore.get(key);
			if(noun.getValue() instanceof ITableDataFrame) {
				((ITableDataFrame) noun.getValue()).close();
			}
		}
		
		// copy over the recipe to a new list
		// and clear the current container
		List<String> newList = new Vector<String>();
		newList.addAll(this.getOptimizedPixelRecipe());
		this.pixelList.clear();
		
		// clear the var store
		this.varStore.clear();
		// clear the panels
		this.insightPanels.clear();
		
		return runPixel(newList);
	}
	
	public PixelRunner reRunPixelInsight() {
		// just clear the varStore
		// TODO: need to do better clean up
		// like actually removing the data makers so we do not 
		// have too much in memory
		Iterator<String> keys = this.varStore.getKeys().iterator();
		while(keys.hasNext()) {
			String key = keys.next();
			if(key.equals(JobReactor.JOB_KEY) || key.equals(JobReactor.SESSION_KEY) || key.equals(JobReactor.INSIGHT_KEY)) {
				continue;
			}
			NounMetadata noun = this.varStore.get(key);
			if(noun.getValue() instanceof ITableDataFrame) {
				((ITableDataFrame) noun.getValue()).close();
			}
			// now remove the key
			keys.remove();
		}
		
		// copy over the recipe to a new list
		// and clear the current container
		List<String> newList = new Vector<String>();
		newList.addAll(this.pixelList);
		this.pixelList.clear();
		
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
