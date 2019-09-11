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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.comments.InsightComment;
import prerna.comments.InsightCommentHelper;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SaveInsightIntoWorkspace;
import prerna.engine.impl.SmssUtilities;
import prerna.sablecc.PKQLRunner;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.sablecc2.reactor.imports.FileMeta;
import prerna.sablecc2.reactor.workflow.GetOptimizedRecipeReactor;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class Insight {

	private static final Logger LOGGER = LogManager.getLogger(Insight.class.getName());
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// need to account for multiple frames to be saved on the insight
	// we will use a special key 
	public static transient final String CUR_FRAME_KEY = "$CUR_FRAME_KEY";
	private static transient final String INSIGHT_FOLDER_KEY = "INSIGHT_FOLDER";
	private static transient final String APP_FOLDER_KEY = "APP_FOLDER";

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
	private transient String insightFolder;
	private transient String appFolder;
	private transient List<FileMeta> filesUsedInInsight = new Vector<FileMeta>();
	private transient Map<String, String> exportFiles = new Hashtable<String, String>();

	private transient boolean deleteFilesOnDropInsight = true;
	private transient boolean deleteREnvOnDropInsight = true;
	
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
	
	// insight specific reactors
	private transient Map<String, Class> insightSpecificHash = new HashMap<String, Class>();
	
	public static Boolean isjavac = null;
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
	
	/**
	 * Open a saved insight and determine if it is cacheable
	 * @param engineId
	 * @param engineName
	 * @param rdbmsId
	 * @param cacheable
	 */
	public Insight(String engineId, String engineName, String rdbmsId, boolean cacheable) {
		this.engineId = engineId;
		this.engineName = engineName;
		this.rdbmsId = rdbmsId;
		this.cacheable = cacheable;
		loadDefaultSettings();
	}
	
	/**
	 * Init the insight
	 */
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
					if(this.user != null && !this.user.isAnonymous() && SaveInsightIntoWorkspace.isCacheUserWorkspace() && AbstractSecurityUtils.securityEnabled() 
							&& this.cacheInWorkspace && !this.pixelList.isEmpty()) {
						if(!runner.isMeta().isEmpty() && !runner.isMeta().get(runner.isMeta().size()-1)) {
							getWorkspaceCacheThread().addToQueue(this.pixelList);
						}
					}
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
				boolean isCacheOfCache = worksapceId.equals(this.engineId);
				this.workspaceCacheThread = new SaveInsightIntoWorkspace(worksapceId, this.rdbmsId, this.insightName, isCacheOfCache);
			}
		}
		return this.workspaceCacheThread;
	}
	
	public void setCacheInWorkspace(boolean cacheInWorkspace) {
		this.cacheInWorkspace = cacheInWorkspace;
	}
	
	public boolean isCacheInWorkspace() {
		return this.cacheInWorkspace;
	}
	
	public void dropWorkspaceCache() {
		if(this.workspaceCacheThread != null) {
			this.workspaceCacheThread.killThread();
			this.workspaceCacheThread.dropWorkspaceCache();
		}
		this.cacheInWorkspace = false;
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

	/**
	 * Get the app relative file key
	 * @return
	 */
	public static String getAppRelativeFolderKey() {
		return Insight.APP_FOLDER_KEY;
	}
	
	/**
	 * Get the APP_FOLDER/INSIGHT_FOLDER key
	 * @return
	 */
	public static String getAppInsightFolderKey() {
		return Insight.APP_FOLDER_KEY + DIR_SEPARATOR + Insight.INSIGHT_FOLDER_KEY;	
	}
	
	/**
	 * Get the prefix as APP_FOLDER/INSIGHT_FOLDER or INSIGHT_FOLDER depending on if it is saved
	 * This is so we know what to send the FE
	 * @return
	 */
	public static String getInsightRelativeFolderKey(Insight in) {
		if(in.isSavedInsight()) {
			return Insight.APP_FOLDER_KEY + DIR_SEPARATOR + Insight.INSIGHT_FOLDER_KEY;
		}
		return Insight.INSIGHT_FOLDER_KEY;
	}
	
	public String getInsightFolder() {
		if(this.insightFolder == null) {
			// account for unsaved insights vs. saved insights
			if(!isSavedInsight()) {
				String sessionId = ThreadStore.getSessionId();
				this.insightFolder = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + DIR_SEPARATOR + sessionId + DIR_SEPARATOR + this.insightId;
			} else {
				// grab from db folder... technically shouldn't be binding on db + we allow multiple locations
				// need to grab from engine
				this.insightFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
						+ DIR_SEPARATOR + "db" 
						+ DIR_SEPARATOR + SmssUtilities.getUniqueName(this.engineName, this.engineId) 
						+ DIR_SEPARATOR + "version"
						+ DIR_SEPARATOR + this.rdbmsId;

			}
		}
		
		return this.insightFolder;
	}
	
	public void setInsightFolder(String insightFolder) {
		this.insightFolder = insightFolder;
	}
	
	public String getAppFolder(String engineName, String engineId) {
		if(this.appFolder == null) {
			// account for unsaved insights vs. saved insights
			if(!isSavedInsight()) {
				return null;
			} else {
				// grab from db folder... technically shouldn't be binding on db + we allow multiple locations
				// need to grab from engine
				this.appFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
						+ DIR_SEPARATOR + "db" 
						+ DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) 
						+ DIR_SEPARATOR + "version"
						+ DIR_SEPARATOR + "assets";
				// if this folder does not exist create it and git init it
				File file = new File(appFolder);
				if(!file.exists())
				{
					file.mkdir();
					GitRepoUtils.init(appFolder);
				}
			}
		}
		
		return this.appFolder;
	}
	
	public void setAppFolder(String appFolder) {
		this.appFolder = appFolder;
	}
	
	/**
	 * If the path is a relative one, modify it for the specific insight
	 * @param filePath
	 * @return
	 */
	public String getAbsoluteInsightFolderPath(String filePath) {
		String relPath = Insight.getInsightRelativeFolderKey(this);
		if(filePath.startsWith(relPath)) {
			filePath = Pattern.compile(Matcher.quoteReplacement(relPath))
					.matcher(filePath).replaceFirst(Matcher.quoteReplacement(getInsightFolder()));
		}
		return filePath;
	}
	
	public boolean isSavedInsight() {
		return this.engineId != null && this.rdbmsId != null;
	}
	
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
		return this.insightId;
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
		if(this.user == null || this.user.isAnonymous()) {
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
		if(this.workspaceCacheThread != null) {
			this.workspaceCacheThread.setInsightName(insightName);
		}
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
	
	public AbstractRJavaTranslator getRJavaTranslator(String className) {
		Logger logger = LogManager.getLogger(className);
		return getRJavaTranslator(logger);
	}
	
	public AbstractRJavaTranslator getRJavaTranslator(Logger logger) {
		if(this.rJavaTranslator == null) {
			this.rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(this, logger);
		} else {
			this.rJavaTranslator.setLogger(logger);
		}
		return this.rJavaTranslator;
	}
	
	public void setRJavaTranslator(AbstractRJavaTranslator rJavaTranslator) {
		this.rJavaTranslator = rJavaTranslator;
	}
	
	public boolean rInstantiated() {
		return this.rJavaTranslator != null;
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
		String fileLocation = this.exportFiles.get(uniqueKey);
		String relPath = Insight.getInsightRelativeFolderKey(this);
		if(fileLocation.startsWith(relPath)) {
			fileLocation = fileLocation.replaceFirst(relPath, Matcher.quoteReplacement(getInsightFolder()));
		}
		return fileLocation;
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
	
	public boolean isDeleteFilesOnDropInsight() {
		return this.deleteFilesOnDropInsight;
	}
	
	public void setDeleteFilesOnDropInsight(boolean deleteFilesOnDropInsight) {
		this.deleteFilesOnDropInsight = deleteFilesOnDropInsight;
	}
	
	public boolean isDeleteREnvOnDropInsight() {
		return this.deleteREnvOnDropInsight;
	}
	
	public void setDeleteREnvOnDropInsight(boolean deleteREnvOnDropInsight) {
		this.deleteREnvOnDropInsight = deleteREnvOnDropInsight;
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
//			if(key.equals(JobReactor.JOB_KEY) || key.equals(JobReactor.SESSION_KEY) || key.equals(JobReactor.INSIGHT_KEY)) {
//				continue;
//			}
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
	
	
	
	
	///////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////
	////////////////////LOAD REACTORS//////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////
	
	
	// get insight specific class
	public IReactor getReactor(String className) {	
		
		// check to see if I can access javac class
		
		// try to get to see if this class already exists
		// no need to recreate if it does
		IReactor retReac = null;
		
		//String cp = getCP();
		
		if(isValidJava() && getInsightFolder() != null)
		{
			File insightDirectory = new File(insightFolder);
			// replace the version name to start with
			String insightId = insightDirectory.getName();
			// accounting for the version which is why the second getParent
			// This needs to be tagged to assets here
			String db = insightDirectory.getParentFile().getParentFile().getName();
	//		insightFolder = insightFolder.replaceAll("\\\\", "/");
	//		String insightId = Utility.getInstanceName(insightFolder);
	//		String db = Utility.getClassName(insightFolder);
	
			
			//String key = db + "." + insightId ;
			String key = insightId ;
			int randomNum = 0;
			//ReactorFactory.compileCache.remove(insightId);
			
			// see if I need to compile this again
			if(!ReactorFactory.compileCache.containsKey(insightId))
			{
				int status = Utility.compileJava(insightFolder, getCP());
				if(status == 0)
				{
					ReactorFactory.compileCache.put(insightId, Boolean.TRUE);
					if(ReactorFactory.randomNumberAdder.containsKey(insightId))
						randomNum = ReactorFactory.randomNumberAdder.get(insightId);				
					randomNum++;
					ReactorFactory.randomNumberAdder.put(insightId, randomNum);
					
					// add it to the key so we can reload
					key = key + randomNum;
					
					// reset the insight specific hash ?
					insightSpecificHash.clear();
				}
			}
			
			if(insightSpecificHash.size() == 0) 
			{
				//compileJava(insightFolder);
				insightSpecificHash = Utility.loadReactors(insightFolder, key);
			}
			// creates the insight specific map
			try {
				if(insightSpecificHash.containsKey(className.toUpperCase())) {
					Class thisReactorClass = insightSpecificHash.get(className.toUpperCase());
					retReac = (IReactor) thisReactorClass.newInstance();
					return retReac;
				}
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			
			// else try to find it the specific db
			// loading it inside of version/classes
			if(engineId != null)
			{
				IEngine engine = Utility.getEngine(engineId);
				return engine.getReactor(className);
		
			}
		}				
		return retReac;
	}
	
	public boolean isValidJava()
	{
		if(isjavac == null)
		{
			try
			{
				Class.forName("com.sun.tools.javac.Main");
				isjavac = true;
			}catch(ClassNotFoundException ex)
			{
				isjavac = false;
			}
		}
		return isjavac;

	}

	
	
	public String getCP()
	{
		String envClassPath = null;
		
		StringBuilder retClassPath = new StringBuilder("");
		ClassLoader cl = getClass().getClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
        	String thisURL = URLDecoder.decode((url.getFile().replaceFirst("/", "")));
        	if(thisURL.endsWith("/"))
        		thisURL = thisURL.substring(0, thisURL.length()-1);

        	retClassPath
        		//.append("\"")
        		.append(thisURL)
        		//.append("\"")
        		.append(";");
        	
        }
        // Adding it, even though this might not exist
        if(insightFolder != null)
        {
	        File file = new File(insightFolder);
	        if(file.exists())
	        {
	        	retClassPath.append(insightFolder + "/classes;");        
	        
	        //now the db
		        String dbDir = file.getParentFile().getParent() + "/classes";
		        retClassPath.append(dbDir);
	        }        
	        // this should also add the db classes folder and the insight classes folder if one exists
        }        
        envClassPath = "\"" + retClassPath.toString() + "\"";
        
        return envClassPath;
        
        // we should also add to sys.path for py and then also remove it
        // sys.path.add
        // sys.path.remove - the remove is tricky however
	}
}
