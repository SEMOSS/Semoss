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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.comments.InsightComment;
import prerna.comments.InsightCommentHelper;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.TCPPyTranslator;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SaveInsightIntoWorkspace;
import prerna.engine.impl.SmssUtilities;
import prerna.pyserve.NettyClient;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc.PKQLRunner;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.PyTranslatorFactory;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.sablecc2.reactor.imports.FileMeta;
import prerna.sablecc2.reactor.insights.SetInsightConfigReactor;
import prerna.sablecc2.reactor.workflow.GetOptimizedRecipeReactor;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.AssetUtility;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class Insight {

	public static Boolean isjavac = null;
	public static final String DEFAULT_SHEET_ID = "0";
	public static final String DEFAULT_SHEET_LABEL = "Sheet1";

	private static final Logger logger = LogManager.getLogger(Insight.class.getName());
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	// need to account for multiple frames to be saved on the insight
	// we will use a special key 
	public static transient final String CUR_FRAME_KEY = "$CUR_FRAME_KEY";
	private static transient final String INSIGHT_FOLDER_KEY = "INSIGHT_FOLDER";

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
	protected int count = 0;
	
	protected String tupleSpace = null;
	
	// list to store the pixels that make this insight
	private PixelList pixelList;
	
	// keep a map to store various properties
	// new variable assignments in pixel are also stored here
	private transient VarStore varStore = new VarStore();
	// separating out delayed messages
	private transient BlockingQueue<NounMetadata> delayedMessages = new ArrayBlockingQueue<NounMetadata>(1024);
	
	// this is the store holding all current tasks (iterators) that are run on the
	// data frames within this insight
	private transient TaskStore taskStore;

	// also store insight sheets
	private transient Map<String, InsightSheet> insightSheets = new LinkedHashMap<String, InsightSheet>();
	{
		// add a default insight
		// this is because old pixels didn't have an insight sheet
		// and dont want those recipes to break
		insightSheets.put(DEFAULT_SHEET_ID, new InsightSheet(DEFAULT_SHEET_ID, DEFAULT_SHEET_LABEL));
	}
	// this is the store holding information around the panels associated with this insight
	private transient Map<String, InsightPanel> insightPanels = new LinkedHashMap<String, InsightPanel>();
	private transient Map<String, Object> insightOrnament = new Hashtable<String, Object>();
	
	// insight comments
	private transient LinkedList<InsightComment> insightCommentList = null;
	
	// we will keep a central rJavaTranslator for the entire insight
	// that can be referenced through all the reactors
	// since reactors have access to insight
	private transient AbstractRJavaTranslator rJavaTranslator;
	private transient PyTranslator pyt;
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
	private transient String userFolder;
	private transient List<FileMeta> filesUsedInInsight = new Vector<FileMeta>();
	private transient Map<String, String> exportFiles = new Hashtable<String, String>();

	private transient boolean deleteFilesOnDropInsight = true;
	private transient boolean deleteREnvOnDropInsight = true;
	private transient boolean deletePythonTupleOnDropInsight = true;

	private transient boolean isSavedInsightMode = false;
	
	private transient List<String> queriedAppIds = new Vector<String>();

	// old - for pkql
	@Deprecated
	private transient Map<String, Map<String, Object>> pkqlVarMap = new Hashtable<String, Map<String, Object>>();
	
	// need a way to shift between old and new insights...
	// dont know how else to shift to this
	protected boolean isOldInsight = false;
	
	// insight specific reactors
	private transient Map<String, Class> insightSpecificHash = new HashMap<String, Class>();
		
	// last panel id touched
	private String lastPanelId = null;
	
	// pragamp for all the pragmas like cache / raw / parquet etc. 
	private Map pragmap = new HashMap();
	
	public NettyClient nc = null;
	
	// base URL
	private String baseURL = null;
	
	// cmd util proxy
	CmdExecUtil cmdUtil = null;
	
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
		this.pixelList = new PixelList();
		this.taskStore = new TaskStore();
		this.insightId = UUID.randomUUID().toString();
		
		// put the pragmap
		if(DIHelper.getInstance().getCoreProp().containsKey("X_CACHE")) {
			this.pragmap.put("xCache", DIHelper.getInstance().getCoreProp().getProperty("X_CACHE"));
		}
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

	public PixelRunner runPixel(String pixelString) {
		List<String> pixelList = new Vector<String>();
		pixelList.add(pixelString);
		return runPixel(pixelList);
	}

	public PixelRunner runPixel(List<String> pixelList) {
		return runPixel(getPixelRunner(), pixelList);
	}
	
	public PixelRunner runPixel(PixelRunner runner, List<String> pixelList) {
		int size = pixelList.size();
		if(size == 0) {
			// set the insight in the runner as it is used
			// to flush to FE
			runner.setInsight(this);
		} else {
			for(int i = 0; i < size; i++) {
				String pixelString = pixelList.get(i);
				if(this.user != null) {
					logger.info(User.getSingleLogginName(this.user) + " Running >>> " + Utility.cleanLogString(pixelString));
				} else {
					logger.info("No User Running >>> " + Utility.cleanLogString(pixelString));
				}
				try {
					runner.runPixel(pixelString, this);
				} catch(SemossPixelException e) {
					if(!e.isContinueThreadOfExecution()) {
						break;
					}
				} finally {
					if(this.user != null && !this.user.isAnonymous() && SaveInsightIntoWorkspace.isCacheUserWorkspace() && AbstractSecurityUtils.securityEnabled() 
							&& this.cacheInWorkspace && !this.pixelList.isEmpty()) {
						List<Pixel> returnedPixelList = runner.getReturnPixelList();
						if(!returnedPixelList.isEmpty() && !returnedPixelList.get(returnedPixelList.size()-1).isMeta()) {
							getWorkspaceCacheThread().addToQueue(this.pixelList.getPixelRecipe());
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
			List<Pixel> returnedPixelList = runner.getReturnPixelList();
			for(Pixel p : returnedPixelList) {
				tracker.trackPixelExecution(this, p.getPixelString(), p.isMeta());
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

	/////////////////////////////////////////////////////////
	// insight panels
	
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
	
	/////////////////////////////////////////////////////////
	// insight sheets
	
	public Map<String, InsightSheet> getInsightSheets() {
		return this.insightSheets;
	}
	
	public void setInsightSheets(Map<String, InsightSheet> insightSheets) {
		this.insightSheets = insightSheets;
	}
	
	public InsightSheet getInsightSheet(String sheetId) {
		return this.insightSheets.get(sheetId);
	}
	
	public void addNewInsightSheet(InsightSheet insightSheet) {
		this.insightSheets.put(insightSheet.getSheetId(), insightSheet);
	}
	
	/////////////////////////////////////////////////////////
	// insight comments
	
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
	 * Get the prefix as APP_FOLDER/INSIGHT_FOLDER or INSIGHT_FOLDER depending on if it is saved
	 * This is so we know what to send the FE
	 * @return
	 */
	public static String getInsightRelativeFolderKey() {
		return Insight.INSIGHT_FOLDER_KEY;
	}
	
	public String getInsightFolder() {
		if(this.insightFolder == null) {
			// account for unsaved insights vs. saved insights
			if(!isSavedInsight()) {
				String sessionId = ThreadStore.getSessionId();
				sessionId = InsightUtility.getFolderDirSessionId(sessionId);
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
	
	public String getAppFolder() {
		if(this.appFolder == null) {
			// account for unsaved insights vs. saved insights
			if(!isSavedInsight()) {
				return null;
			} else {
				// grab from db folder... technically shouldn't be binding on db + we allow multiple locations
				// need to grab from engine
				this.appFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
						+ DIR_SEPARATOR + "db" 
						+ DIR_SEPARATOR + SmssUtilities.getUniqueName(this.engineName, this.engineId) 
						+ DIR_SEPARATOR + "version"
						+ DIR_SEPARATOR + "assets";
				// if this folder does not exist create it and git init it
				File file = new File(appFolder);
				if(!file.exists())
				{
					file.mkdir();
					//GitRepoUtils.init(appFolder);
				}
			}
		}
		
		return this.appFolder;
	}
	
	public void setAppFolder(String appFolder) {
		this.appFolder = appFolder;
	}
	
	// gets the user folder as well
	public String getUserFolder() {
		AuthProvider provider = user.getPrimaryLogin();
		String appId = user.getAssetEngineId(provider);
		this.userFolder = AssetUtility.getAppAssetVersionFolder("Asset", appId);
		return userFolder;
	}
	
	/**
	 * If the path is a relative one, modify it for the specific insight
	 * @param filePath
	 * @return
	 */
	public String getAbsoluteInsightFolderPath(String filePath) {
		String relPath = Insight.getInsightRelativeFolderKey();
		if(filePath.startsWith(relPath)) {
			filePath = Pattern.compile(Matcher.quoteReplacement(relPath))
					.matcher(filePath).replaceFirst(Matcher.quoteReplacement(getInsightFolder()));
		}
		return filePath;
	}
	
	public boolean isSavedInsight() {
		return this.engineId != null && this.rdbmsId != null;
	}
	
	public PixelList getPixelList() {
		return this.pixelList;
	}
	
	/**
	 * This method returns the optimized pixel recipe
	 * 
	 * @return modifiedRecipe
	 */
	public List<String> getOptimizedPixelRecipe() {
		GetOptimizedRecipeReactor optimizer = new GetOptimizedRecipeReactor();
		List<String> recipe = optimizer.getOptimizedRecipe(this.pixelList.getPixelRecipe());
		return recipe;
	}

	public void setPixelList(PixelList pixelList) {
		this.pixelList = pixelList;
	}
	
	public void setPixelRecipe(List<String> pixelRecipe) {
		this.pixelList.clear();
		this.pixelList.addPixel(pixelRecipe);
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
	
	public void addDelayedMessage(NounMetadata noun) {
		this.delayedMessages.add(noun);
	}
	
	public List<NounMetadata> getDelayedMessages() {
		List<NounMetadata> messages = new Vector<NounMetadata>();
		NounMetadata noun = null;
		while( (noun = delayedMessages.poll()) != null) {
			messages.add(noun);
		}
		return messages;
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
		String relPath = Insight.getInsightRelativeFolderKey();
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
	
	public boolean isDeletePythonTupleOnDropInsight() {
		return this.deletePythonTupleOnDropInsight;
	}
	
	public void setDeletePythonTupleOnDropInsight(boolean deletePythonTupleOnDropInsight) {
		this.deletePythonTupleOnDropInsight = deletePythonTupleOnDropInsight;
	}
	
	public void setRunSavedInsightMode(boolean isSavedInsightMode) {
		this.isSavedInsightMode = isSavedInsightMode;
	}

	public boolean isSavedInsightMode() {
		return this.isSavedInsightMode;
	}
	
	/**
	 * Store the app ids that are queried
	 * @param appId
	 */
	public void addQueriedEngine(String appId) {
		if(!this.queriedAppIds.contains(appId)) {
			this.queriedAppIds.add(appId);
		}
	}
	
	public List<String> getQueriedEngines() {
		return this.queriedAppIds;
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
	
	public ITableDataFrame getCurFrame()
	{
		Object frame = getDataMaker();
		if(frame != null)
			return (ITableDataFrame)frame;
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
			logger.info("Running >>> " + pkqlString);
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
		this.pixelList.addPixel(pkqlString);
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
				logger.info("Running >>> " + pkqlString);
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
			this.pixelList.addPixel(pkqlString);
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
		return runPkql(this.pixelList.getPixelRecipe());
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
	
	/**
	 * 
	 * @param appendInsightConfig
	 * @return
	 */
	public PixelRunner reRunPixelInsight(boolean appendInsightConfig) {
		return reRunPixelInsight(appendInsightConfig, false);
	}
	
	/**
	 * 
	 * @param appendInsightConfig
	 * @param appendPanel0
	 * @return
	 */
	public PixelRunner reRunPixelInsight(boolean appendInsightConfig, boolean appendPanel0) {
		synchronized(this) {
			// set the mode
			setRunSavedInsightMode(true);
			
			Map<String, NounMetadata> currentParameters = this.varStore.pullParameters();
			
			// always add the insight config
			boolean hasInsightConfig = false;
			if(appendInsightConfig) {
				NounMetadata noun = varStore.get(SetInsightConfigReactor.INSIGHT_CONFIG);
				if(noun != null) {
					Gson gson = new GsonBuilder().disableHtmlEscaping().create();
					StringBuilder builder = new StringBuilder("META | SetInsightConfig(");
					builder.append(gson.toJson(noun.getValue()));
					builder.append(");");
					Pixel pixel = this.pixelList.addPixel(builder.toString());
					pixel.setMeta(true);
					hasInsightConfig = true;
				}
			}
			
			// clear the insight
			// dropping frames and everything in the varstore
			InsightUtility.clearInsight(this, false);
			// clear the sheets and add the default one
			this.insightSheets.clear();
			this.insightSheets.put(DEFAULT_SHEET_ID, new InsightSheet(DEFAULT_SHEET_ID, DEFAULT_SHEET_LABEL));
			// clear the panels
			this.insightPanels.clear();
			if(appendPanel0) {
				this.insightPanels.put("0", new InsightPanel("0", DEFAULT_SHEET_ID));
			}
			
			// copy over the recipe to a new list
			// and clear the current container
			// maintain the pixelIds so they are consistent
			List<String> currentPixelIds = this.pixelList.getNonMetaPixelIds();
			List<Map<String, Object>> currentPixelPositions = this.pixelList.getNonMetaPixelPositions();
			// grab all the pixel recipes
			List<String> currentRecipe = this.pixelList.getPixelRecipe();
//			int counterVal = this.pixelList.getCounter();
			
			// create a new pixelList
			this.pixelList = new PixelList();
			
			// execution
			PixelRunner results = getPixelRunner();
			results.setMaintainErrors(true);
			runPixel(results, currentRecipe);
			// now update the pixel list to the new ids
			// realize the pixel objects are the same
			List<Pixel> pixelReturns = results.getReturnPixelList();
			int size = pixelReturns.size();
			if(hasInsightConfig) {
				size--;
			}
			for(int i = 0; i < size; i++) {
				String id = currentPixelIds.get(i);
				Map<String, Object> position = currentPixelPositions.get(i);
				Pixel p = pixelReturns.get(i);
				p.setId(id);
				if(position != null && !position.isEmpty()) {
					p.setPositionMap(position);
				}
			}
			this.pixelList.recalculateIdToIndexHash();
			// and set the counter properly
			// so that way the counter doesn't exponentially
			// increase with every rerun
//			this.pixelList.setCounter(counterVal);
			
			// add back the insight parameters
			for(String paramKey : currentParameters.keySet()) {
				this.varStore.put(paramKey, currentParameters.get(paramKey));
			}
			
			// set the mode back
			setRunSavedInsightMode(false);
			return results;
		}
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
			// check with all the engines used
			if(engineId != null)
			{
				IEngine engine = Utility.getEngine(engineId);
				retReac = engine.getReactor(className, null);				
			}
			// check all other dbs
			// first one wins
			for(int engineIndex = 0;engineIndex < queriedAppIds.size() && retReac == null;engineIndex++)
			{
				String thisEngine = queriedAppIds.get(engineIndex);
				IEngine engine = Utility.getEngine(thisEngine);
				retReac = engine.getReactor(className, null);
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

        if(System.getProperty("os.name").toLowerCase().contains("win")) {
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
        } else {
            for(URL url: urls){
            	String thisURL = URLDecoder.decode((url.getFile()));
            	if(thisURL.endsWith("/"))
            		thisURL = thisURL.substring(0, thisURL.length()-1);

            	retClassPath
            		//.append("\"")
            		.append(thisURL)
            		//.append("\"")
            		.append(":");
            }
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
	
	public void setLastPanelId(String panelId) {
		this.lastPanelId = panelId;
	}
	
	public String getLastPanelId() {
		return this.lastPanelId;
	}
	
	public void setFinalViewOptions(String panelId, SelectQueryStruct lastQs, TaskOptions taskOptions) {
		if(insightPanels.containsKey(panelId)) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if(panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			panel.setFinalViewOptions(lastQs, taskOptions);
		}
		this.lastPanelId = panelId;
	}
	
	public void setLastQS(SelectQueryStruct lastQs, String panelId) {
		if(panelId != null) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if(panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			panel.setLastQs(lastQs);
		}
	}
	
	public SelectQueryStruct getLastQS(String panelId) {
		if(panelId != null && insightPanels.containsKey(panelId)) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if(panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			return panel.getLastQs();
		}
		return null;
	}

	public TaskOptions getLastTaskOptions() {
		return getLastTaskOptions(lastPanelId);
	}
	
	public TaskOptions getLastTaskOptions(String panelId) {
		if(panelId != null && insightPanels.containsKey(panelId)) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if(panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			return panel.getTaskOptions();
		}
		return null;
	}
	
	// sets the pragma map to be used
	public void setPragmap(Map pragmap) {
		this.pragmap = pragmap;
	}
	
	// gets the pragma map
	public Map getPragmap() {
		return this.pragmap;
	}
	
	public void clearPragmap() {
		this.pragmap.clear();
	}
	
	public int getCount() {
		count++;
		return count;
	}
	
	public String getTupleSpace() {
		return this.tupleSpace;
	}
	
	public void setTupleSpace(String tupleSpace) {
		this.tupleSpace = tupleSpace;
	}
	
	// gets the frame name to be more useful
	public String predictFrameName() {
		StringBuilder frameName = new StringBuilder("");
		for(int engineIndex = 0; engineIndex < queriedAppIds.size(); engineIndex++) {
			String thisEngine = queriedAppIds.get(engineIndex);
			IEngine engine = Utility.getEngine(thisEngine);
			String name = engine.getEngineName();
			if(frameName.length() != 0 && engineIndex != queriedAppIds.size()) {
				frameName.append("_");
			} else if(engineIndex == queriedAppIds.size()) {
				frameName.append("_and_"); 
			}
			frameName.append(name);
		}
		
		return frameName.toString();
	}
	
	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}
	
	public String getBaseURL() {
		return this.baseURL;
	}
	
	public String getInsightURL() {
		StringBuilder retURL = new StringBuilder(this.baseURL);
		retURL.append("insight?engine=").append(engineId).append("&").append("id=").append(rdbmsId);
		return retURL.toString();
	}

	public String getLiveURL() {
		StringBuilder retURL = new StringBuilder(this.baseURL);
		retURL.append("insight?insightId=").append(insightId);
		return retURL.toString();
	}
	
	/**
	 * Utility method to pull from VarStore
	 * @param varName
	 * @return
	 */
	public Object getVar(String varName) {
		Object retObject = this.varStore.get(varName);
		if(retObject != null) {
			return ((NounMetadata)retObject).getValue();
		}
		return null;
	}
	
	public String getProperty(String propName)
	{
		String retOutput = DIHelper.getInstance().getProperty(propName);
		//if(retOutput == null)
		{
			Object retObject = this.varStore.get(propName);
			if(retObject != null)
				retOutput = ((NounMetadata)retObject).getValue().toString();
		}
		return retOutput;
	}

	public boolean setContext(String context)
	{
		// sets the context space for the user
		// also set rhe cmd context right here
		if(this.user != null)
		{
			Map <String, StringBuffer> appMap = this.user.getAppMap();
			if(!appMap.containsKey(context))
			{
				// attempt once to directly map it with same name
				boolean success = this.user.addVarMap(context, context); 
			}	
			if(appMap.containsKey(context))
			{
				this.user.setContext(context);
				return true;
			}
			return false;
		}
		else
		{
			String id = Utility.getEngineData(context);
			String mountDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db"
			+ DIR_SEPARATOR + context + "__" + id + DIR_SEPARATOR +  "version";
	
			this.cmdUtil = new CmdExecUtil(context, mountDir);
			return true;
		}
	}
	
	public CmdExecUtil getCmdUtil()
	{
		if(this.user != null)
			return this.user.getCmdUtil();
		else
			return this.cmdUtil;
	}

	///////////////////////////////////////// PYTHON SPECIFIC METHODS ///////////////////////////////////////////
	
	public void setPy(PyExecutorThread jepThread) {
		
		this.jepThread = jepThread;
		// need to do the check here
		if(this.pyt == null)
		{
			pyt = new PyTranslator();
			pyt.setInsight(this);
			pyt.setPy(jepThread);
		}
	}
	
	public void setPyTranslator(PyTranslator pyt)
	{
		//if(this.pyt == null)
		this.pyt = pyt;
		if (pyt != null) {
			pyt.setInsight(this);
		}
	}
	
	public PyTranslator getPyTranslator() {
		// this is really where I need to pull from user
		// I need to recreate since i make the translator specific to 
		if(user != null) {
			this.pyt = user.getPyTranslator();
		} else {
			// there is no user
			this.pyt = PyTranslatorFactory.getTranslator();			
		}		
		
		if(this.pyt == null) {
			this.pyt = user.getPyTranslator();
			// need to recreate the translator
			if(this.pyt instanceof TCPPyTranslator) {
				NettyClient nc1 = ((TCPPyTranslator)pyt).nc;
				this.pyt = new TCPPyTranslator();
				((TCPPyTranslator)pyt).nc = nc1;
			} else if(this.pyt instanceof PyTranslator) {
				this.jepThread = pyt.getPy();
				this.pyt = new PyTranslator();
				this.pyt.setPy(this.jepThread);
				this.jepThread = pyt.getPy();
			}
		}
		this.pyt.setInsight(this);
		return this.pyt;
	}
	
	public PyExecutorThread getPy() {
		return this.jepThread;
	}
	
	
	public void setNettyClient(NettyClient nc) {
		if(nc != null) {
			this.nc = nc;
			this.pyt = new prerna.ds.py.TCPPyTranslator();
			this.pyt.setInsight(this);
		}
	}

	public void dropPythonTupleSpace() {
		
		if(this.tupleSpace != null && nc == null) {
			try {
				File closer = new File(tupleSpace + "/alldone.closeall");
				closer.createNewFile();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if(this.nc != null)
		{
			//nc.disconnect();
		}
	}



	///////////////////////////////////////// END PYTHON SPECIFIC METHODS ///////////////////////////////////////////

}
