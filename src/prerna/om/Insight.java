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
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
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
import prerna.auth.utils.SecurityProjectUtils;
import prerna.comments.InsightComment;
import prerna.comments.InsightCommentHelper;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.impl.SaveInsightIntoWorkspace;
import prerna.project.api.IProject;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.IReactor;
import prerna.reactor.InsightCustomReactorCompilator;
import prerna.reactor.export.IFormatter;
import prerna.reactor.frame.py.PySingleton;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.reactor.frame.r.util.TCPRTranslator;
import prerna.reactor.insights.SetInsightConfigReactor;
import prerna.reactor.job.JobReactor;
import prerna.reactor.workflow.GetOptimizedRecipeReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.tcp.client.SocketClient;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.AssetUtility;
import prerna.util.ChromeDriverUtility;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class Insight implements Serializable {

	public static final String DEFAULT_SHEET_ID = "0";
	public static final String DEFAULT_SHEET_LABEL = "Sheet1";

	private static final Logger logger = LogManager.getLogger(Insight.class.getName());
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	// need to account for multiple frames to be saved on the insight
	// we will use a special key 
	public static transient final String CUR_FRAME_KEY = "$CUR_FRAME_KEY";
	private static transient final String INSIGHT_FOLDER_KEY = "INSIGHT_FOLDER";
	public static transient final String FILTER_REFRESH_KEY = "$FILTER_REFRESH";

	// this is the id it is assigned within the InsightCache
	// it varies from one instance of an insight to another instance of the same insight
	protected String insightId;
	
	// new user object
	protected User user;
	protected String insightName;

	// if this is a saved insight
	protected String rdbmsId;
	protected String projectId;
	protected String projectName;
	protected boolean cacheable = true;
	protected int cacheMinutes = -1;
	protected String cacheCron;
	protected boolean cacheEncrypt = false;
	private transient ZonedDateTime cachedDateTime = null;
	protected int count = 0;
	
	// list to store the pixels that make this insight
	private transient PixelList pixelList;
	
	// keep a map to store various properties
	// new variable assignments in pixel are also stored here
	private transient VarStore varStore = new VarStore();

	// separating out delayed messages
	private transient BlockingQueue<NounMetadata> delayedMessages = new ArrayBlockingQueue<NounMetadata>(1024);
	
	// this is the store holding all current tasks (iterators) that are run on the
	// data frames within this insight
	private transient TaskStore taskStore;

	// temporal cache for a frame to point to a new frame with 1 column of just the unique values
	private transient Map<String, ITableDataFrame> cachedFitlerModelFrame = new HashMap<>();
	
	// also store insight sheets
	private transient Map<String, InsightSheet> insightSheets = new LinkedHashMap<String, InsightSheet>();
	
	// this is the store holding information around the panels associated with this insight
	private transient Map<String, InsightPanel> insightPanels = new LinkedHashMap<String, InsightPanel>();
	private transient Map<String, Object> insightOrnament = new Hashtable<String, Object>();
	
	// insight comments
	private transient LinkedList<InsightComment> insightCommentList = null;
	
	// we will keep a central rJavaTranslator for the entire insight
	// that can be referenced through all the reactors
	// since reactors have access to insight
	protected String tupleSpace = null;
	private transient AbstractRJavaTranslator rJavaTranslator; // need a way keep the environment name so it is communicated
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
	private transient List<InsightFile> loadInsightFiles = new Vector<>();
	private transient Map<String, InsightFile> exportInsightFiles = new HashMap<>();

	private transient boolean deleteFilesOnDropInsight = true;
	private transient boolean deleteREnvOnDropInsight = true;
	private transient boolean deletePythonTupleOnDropInsight = true;

	private transient boolean isTemporaryInsight = false;
	private transient boolean isSchedulerMode = false;
	private transient boolean isSavedInsightMode = false;
	
	private transient Set<String> queriedDatabaseIds = new HashSet<String>();

	// old - for pkql
	@Deprecated
	private transient Map<String, Map<String, Object>> pkqlVarMap = new Hashtable<String, Map<String, Object>>();
	
	// need a way to shift between old and new insights...
	// dont know how else to shift to this
	protected boolean isOldInsight = false;
	
	// insight specific reactors
	private transient Map<String, Class> insightSpecificHash = new HashMap<>();

	// last panel id touched
	private String lastPanelId = null;
	
	// pragamp for all the pragmas like cache / raw / parquet etc. 
	private Map pragmap = new HashMap();
	
	public transient SocketClient nc = null;
	
	// base URL
	private String baseURL = null;
	
	// cmd util proxy
	private CmdExecUtil cmdUtil = null;
	private String contextProjectId = null;
	private String contextProjectName = null;
	
	// chrome proxy
	private transient ChromeDriverUtility chromeUtil = null;
	
	private String rEnvName = null;
	
	private boolean contextReinitialized = false;
	
	Map <String, GenExpressionWrapper> sqlWrapperMap = new HashMap<String, GenExpressionWrapper>();
	Map <String, String> id2SQLMapper = new HashMap<String, String>();
	int idCount = 0;
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////// START CONSTRUCTORS //////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Create an empty insight
	 */
	public Insight() {
		loadDefaultSettings(500);
		
		{
			// add a default insight
			// this is because old pixels didn't have an insight sheet
			// and dont want those recipes to break
			insightSheets.put(DEFAULT_SHEET_ID, new InsightSheet(DEFAULT_SHEET_ID, DEFAULT_SHEET_LABEL));
		}
	}

	/**
	 * Open a saved insight and determine if it is cacheable
	 * @param projectId
	 * @param projectName
	 * @param rdbmsId
	 * @param cacheable
	 */
	public Insight(String projectId, String projectName, String rdbmsId, boolean cacheable, int cacheMinutes, String cacheCron, boolean cacheEncrypt, int capacity) {
		this.projectId = projectId;
		this.projectName = projectName;
		this.rdbmsId = rdbmsId;
		this.cacheable = cacheable;
		this.cacheCron = cacheCron;
		this.cacheMinutes = cacheMinutes;
		this.cacheEncrypt = cacheEncrypt;
		loadDefaultSettings(capacity);
	}
	
	/**
	 * Init the insight
	 */
	private void loadDefaultSettings(int capacity) {
		this.pixelList = new PixelList(capacity);
		this.taskStore = new TaskStore();
		this.insightId = UUID.randomUUID().toString();
		
		// put the pragmap
		if(DIHelper.getInstance().getCoreProp().containsKey("X_CACHE")) {
			this.pragmap.put("xCache", DIHelper.getInstance().getCoreProp().getProperty("X_CACHE"));
		}
		// put the pragmap
		if(Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
			if(this.user != null) {
				this.user.getUserMountHelper().mountFolder(getInsightFolder(), getInsightFolder(), false);
			}
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
	
	public PixelRunner runPixel(PixelRunner runner, String pixelString) {
		List<String> pixelList = new Vector<String>();
		pixelList.add(pixelString);
		return runPixel(runner, pixelList);
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
					logger.error(Constants.ERROR_MESSAGE, e);
					if(!e.isContinueThreadOfExecution()) {
						break;
					}
				} catch(Exception e) {
					logger.error(Constants.ERROR_MESSAGE, e);
				} finally {
					if(this.user != null && !this.user.isAnonymous() && SaveInsightIntoWorkspace.isCacheUserWorkspace() 
							&& this.cacheInWorkspace && !this.pixelList.isEmpty()) {
						List<Pixel> returnedPixelList = runner.getReturnPixelList();
						if(!returnedPixelList.isEmpty() && !returnedPixelList.get(returnedPixelList.size()-1).isMeta()) {
							SaveInsightIntoWorkspace thread = getWorkspaceCacheThread();
							if(thread != null) {
								thread.addToQueue(this.pixelList.getPixelRecipe());
							}
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
		try {
			IUserTracker tracker = UserTrackerFactory.getInstance();
			if(tracker.isActive()) {
				List<Pixel> returnedPixelList = runner.getReturnPixelList();
				for(Pixel p : returnedPixelList) {
					tracker.trackPixelExecution(this, p.getPixelString(), p.isMeta());
				}
			}
		} catch(Exception e) {
			logger.error(Constants.ERROR_MESSAGE, e);
		}
	}

	public PixelRunner getPixelRunner() {
		PixelRunner runner = new PixelRunner();
		return runner;
	}
	
	private SaveInsightIntoWorkspace getWorkspaceCacheThread() {
		if(this.workspaceCacheThread == null && this.user != null && this.user.isLoggedIn()) {
			String worksapceId = this.user.getWorkspaceProjectId(this.user.getPrimaryLogin());
			if(worksapceId != null) {
				boolean isCacheOfCache = worksapceId.equals(this.projectId);
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
			this.insightCommentList = InsightCommentHelper.generateInsightCommentList(this.projectId, this.rdbmsId);
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

	public String getInsightFolder() {
		if(this.insightFolder == null) {
			// account for unsaved insights vs. saved insights
			if(!isSavedInsight()) {
				String sessionId = ThreadStore.getSessionId();
				if(sessionId == null) {
					sessionId = (String) this.varStore.get(JobReactor.SESSION_KEY).getValue();
				}
				sessionId = InsightUtility.getFolderDirSessionId(sessionId);
				this.insightFolder = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + 
						DIR_SEPARATOR + sessionId + DIR_SEPARATOR + this.insightId;
			} else {
				this.insightFolder = AssetUtility.getProjectVersionFolder(this.projectName, this.projectId)
						+ DIR_SEPARATOR + this.rdbmsId;
			}
		}
		
		// make the folder if it doesn't already exist
		File f = new File(Utility.normalizePath(this.insightFolder));
		if(!f.exists() || !f.isDirectory()) {
			f.mkdirs();
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
				this.appFolder = AssetUtility.getProjectAssetFolder(this.projectName, this.projectId);
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
		String projectId = user.getAssetProjectId(provider);
		this.userFolder = AssetUtility.getUserAssetAndWorkspaceVersionFolder("Asset", projectId);
		return userFolder;
	}
	
	/**
	 * If the path is a relative one, modify it for the specific insight
	 * @param filePath
	 * @return
	 */
	public String getAbsoluteInsightFolderPath(String filePath) {
		// is this one that starts with INSIGHT_FOLDER
		if(filePath.startsWith(Insight.INSIGHT_FOLDER_KEY)) {
			filePath = Pattern.compile(Matcher.quoteReplacement(Insight.INSIGHT_FOLDER_KEY))
					.matcher(filePath).replaceFirst(Matcher.quoteReplacement(getInsightFolder()));
		} else {
			// make sure this is not relative
			// if it is
			// turn to absolute based on the insight folder location
			if(!(new File(filePath).exists())) {
				String filePrefix = getInsightFolder();
				if(filePath.startsWith("\\") || filePath.startsWith("/")) {
					filePath = filePrefix + filePath;
				} else {
					filePath = filePrefix + DIR_SEPARATOR + filePath;
				}
			}
		}
		
		return filePath;
	}
	
	public boolean isSavedInsight() {
		return this.projectId != null && this.rdbmsId != null;
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

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public String getContextProjectId() {
		return contextProjectId;
	}
	
	public void setContextProjectId(String contextProjectId) {
		this.contextProjectId = contextProjectId;
	}
	
	public String getContextProjectName() {
		return contextProjectName;
	}
	
	public void setContextProjectName(String contextProjectName) {
		this.contextProjectName = contextProjectName;
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
	
	public int getCacheMinutes() {
		return cacheMinutes;
	}

	public void setCacheMinutes(int cacheMinutes) {
		this.cacheMinutes = cacheMinutes;
	}
	
	public String getCacheCron() {
		return cacheCron;
	}

	public void setCacheCron(String cacheCron) {
		this.cacheCron = cacheCron;
	}

	public boolean isCacheEncrypt() {
		return this.cacheEncrypt;
	}
	
	public void setCacheEncrypt(boolean cacheEncrypt) {
		this.cacheEncrypt = cacheEncrypt;
	}
	
	public ZonedDateTime getCachedDateTime() {
		return cachedDateTime;
	}

	public void setCachedDateTime(ZonedDateTime cachedDateTime) {
		this.cachedDateTime = cachedDateTime;
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
		
			// set the netty client if the translator is TCP R translator
			if(this.rJavaTranslator instanceof TCPRTranslator)
			{
				// do this so that the netty client is initialized
				//getPyTranslator();
				// now set the netty client
				if(this.user != null)
				{
					((TCPRTranslator)this.rJavaTranslator).setClient( this.user.getSocketClient(true) );
				}
				else
				{
					((TCPRTranslator)this.rJavaTranslator).setClient( PySingleton.getTCPServer() );
				}
				this.rJavaTranslator.setInsight(this);
				this.rJavaTranslator.startR();
			}
		}
		return this.rJavaTranslator;
	}
	
	public void setRJavaTranslator(AbstractRJavaTranslator rJavaTranslator) {
		this.rJavaTranslator = rJavaTranslator;
		this.rEnvName = rJavaTranslator.env;
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
	
	/**
	 * Set the temp frame for caching the filter model
	 * @param uniqueKey
	 * @param tempFrame
	 */
	public void addCachedFitlerModelFrame(String uniqueKey, ITableDataFrame tempFrame) {
		this.cachedFitlerModelFrame.put(uniqueKey, tempFrame);
	}
	
	/**
	 * Get the temp frame cached for the filter model
	 * @param uniqueKey
	 * @return
	 */
	public ITableDataFrame getCachedFitlerModelFrame(String uniqueKey) {
		return this.cachedFitlerModelFrame.get(uniqueKey);
	}
	
	/**
	 * Get all the cached filter model frames
	 * @return
	 */
	public Map<String, ITableDataFrame> getCachedFilterModelFrame() {
		return this.cachedFitlerModelFrame;
	}
	
	/////////////////////////////////////////////////////////////////
	
	/*
	 * For getting file exports from the insight
	 */
	
	public void addExportFile(String uniqueKey, InsightFile fileLocation) {
		this.exportInsightFiles.put(uniqueKey, fileLocation);
	}
	
	public String getExportFileLocation(String uniqueKey) {
		InsightFile insightFile = this.exportInsightFiles.get(uniqueKey);
		if(insightFile == null) {
			throw new IllegalArgumentException("The unique key '" + uniqueKey + "' is an incorrect identifier for the file");
		}
		String fileLocation = insightFile.getFilePath();
		return getAbsoluteInsightFolderPath(fileLocation);
	}
	
	public Map<String, InsightFile> getExportInsightFiles() {
		return this.exportInsightFiles;
	}
	
	public void addLoadInsightFile(InsightFile fileMeta) {
		this.loadInsightFiles.add(fileMeta);
	}
	
	public void setLoadInsightFiles(List<InsightFile> insightFiles) {
		this.loadInsightFiles = insightFiles;
	}

	public List<InsightFile> getLoadInsightFiles() {
		return this.loadInsightFiles;
	}
	
	public boolean isDeleteFilesOnDropInsight() {
		return this.deleteFilesOnDropInsight;
	}
	
	public void setDeleteFilesOnDropInsight(boolean deleteFilesOnDropInsight) {
		this.deleteFilesOnDropInsight = deleteFilesOnDropInsight;
	}
	
	/////////////////////////////////////////////////////////////////

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
	
	public boolean isSchedulerMode() {
		return isSchedulerMode;
	}

	public void setSchedulerMode(boolean isSchedulerMode) {
		this.isSchedulerMode = isSchedulerMode;
	}
	
	public boolean isTemporaryInsight() {
		return isTemporaryInsight;
	}

	public void setTemporaryInsight(boolean isTemporaryInsight) {
		this.isTemporaryInsight = isTemporaryInsight;
	}

	/**
	 * Store the database ids that were queried
	 * @param databaseId
	 */
	public void addQueriedDatabasesese(String databaseId) {
		// this is a set
		this.queriedDatabaseIds.add(databaseId);
	}
	
	public Set<String> getQueriedDatabaseIds() {
		return this.queriedDatabaseIds;
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

//	@Deprecated
//	public Map<String, Object> runPkql(String pkqlString) {
//		PKQLRunner runner = getPkqlRunner();
//		try {
//			logger.info("Running >>> " + pkqlString);
//			// we need to account for the fact that the data.output
//			// will create a completely new insight object
//			// so even though we add the reactors
//			// we end up with a new translation that needs them again
//			if(this.getDataMaker() != null) {
//				runner.runPKQL(pkqlString, this.getDataMaker());
//			} else {
//				// ugh... i dont like having to have a h2frame...
//				// but FE never adds data.frame(grid)
//				runner.runPKQL(pkqlString, new H2Frame());
//			}
//		} catch(Exception e) {
//			e.printStackTrace();
//			throw new IllegalArgumentException("Error with " + pkqlString + "\n" + e.getMessage());
//		}
//		this.pixelList.addPixel(pkqlString);
//		return collectPkqlResults(runner);
//	}
//
//	// run a new list of pkql routines
//	@Deprecated
//	public Map<String, Object> runPkql(List<String> pqlList) {
//		PKQLRunner runner = getPkqlRunner();
//		int size = pqlList.size();
//		for(int i = 0; i < size; i++) {
//			String pkqlString = pqlList.get(i);
//			try {
//				logger.info("Running >>> " + pkqlString);
//				if(this.getDataMaker() != null) {
//					runner.runPKQL(pkqlString, this.getDataMaker());
//				} else {
//					// ugh... i dont like having to have a h2frame...
//					// but FE never adds data.frame(grid)
//					runner.runPKQL(pkqlString, new H2Frame());
//				}
//			} catch(Exception e) {
//				e.printStackTrace();
//				throw new IllegalArgumentException("Error with " + pkqlString + "\n" + e.getMessage());
//			}
//			this.pixelList.addPixel(pkqlString);
//		}
//		return collectPkqlResults(runner);
//	}
	
	/**
	 * A routine to grab all the random data we need for the previously run insight pixel routines
	 * @return
	 */
//	@Deprecated
//	private Map<String, Object> collectPkqlResults(PKQLRunner pkqlRunner) {
//		Map<String, Object> returnObj = new HashMap<String, Object>();
//		
//		// add insight information
//		returnObj.put("insightID", this.insightId);
//		IDataMaker datamaker = pkqlRunner.getDataFrame();
//		if(datamaker != null) {
//			returnObj.put("dataID", datamaker.getDataId());
//			// TODO: just cause i want as many things to be in future state as possible
//			this.varStore.put(CUR_FRAME_KEY, new NounMetadata(datamaker, PixelDataType.FRAME, PixelOperationType.FRAME));
//		}
//		
//		// add the pkql data
//		returnObj.put("newColumns", pkqlRunner.getNewColumns());
//		returnObj.put("newInsights", pkqlRunner.getNewInsights());
//		returnObj.put("clear", pkqlRunner.getDataClear());
//		returnObj.put("pkqlData", pkqlRunner.getResults());
//		returnObj.put("feData", pkqlRunner.getFeData());
//		
//		// ... in case this is some dashboard object
//		if(pkqlRunner.getDashboardData() != null) {
//			Map dashboardMap = new HashMap();
//			dashboardMap.putAll((Map)pkqlRunner.getDashboardData());
//			returnObj.put("Dashboard", dashboardMap);
//		}
//		
//		// need to grab the metadata
//		// in case there is a file used that i need to keep track of
//		// if this insight is later saved
////		parseMetadataResponse(pkqlRunner.getMetadataResponse());
//		
//		// store the varmap after the operation is done
//		this.pkqlVarMap = pkqlRunner.getVarMap();
//		
//		return returnObj;
//	}
	
//	@Deprecated
//	public Map<String, Object> reRunInsight() {
//		// just clear the varStore
//		// TODO: need to do better clean up
//		// like actually removing the data makers so we do not 
//		// have too much in memory
//		this.varStore.clear();
//		this.insightPanels.clear();
//		return runPkql(this.pixelList.getPixelRecipe());
//	}
	
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
			Map<String, NounMetadata> preAppliedParameters = this.varStore.pullPreAppliedParameters();
			
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
			if(!this.isSavedInsight()) {
				this.insightSheets.put(DEFAULT_SHEET_ID, new InsightSheet(DEFAULT_SHEET_ID, DEFAULT_SHEET_LABEL));
			}
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
			this.pixelList = new PixelList(currentRecipe.size());
			
			// add back the insight parameters
			// so that we can set the value inside of them
			for(String paramKey : currentParameters.keySet()) {
				this.varStore.put(paramKey, currentParameters.get(paramKey));
			}
			
			// add back the preApplied parameters
			// so that we can set the value inside of them
			for(String paramKey : preAppliedParameters.keySet()) {
				this.varStore.put(paramKey, preAppliedParameters.get(paramKey));
			}
			
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
			
			// set the mode back
			setRunSavedInsightMode(false);
			return results;
		}
	}
	
//	/**
//	 * Get a new instance of the pkql runner
//	 * @return
//	 */
//	@Deprecated
//	public PKQLRunner getPkqlRunner() {
//		PKQLRunner runner = new PKQLRunner();
//		runner.setInsightId(this.insightId);
//		runner.setVarMap(this.pkqlVarMap);
//		return runner;
//	}
	
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
		// try to get to see if this class already exists
		// no need to recreate if it does
		IReactor retReac = null;
		
		String key = InsightCustomReactorCompilator.getKey(this);
		// see if I need to compile this again
		if(!InsightCustomReactorCompilator.isCompiled(key)) {
			int status = Utility.compileJava(insightFolder, getCP());
			if(status == 0) {
				InsightCustomReactorCompilator.setCompiled(key);
			}
		}
		
		if(insightSpecificHash == null || insightSpecificHash.isEmpty()) {
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
			logger.error(Constants.STACKTRACE, e);
		} catch (IllegalAccessException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		// user has manually set the specific context
		if(this.contextProjectId != null) {
			IProject project = Utility.getProject(this.contextProjectId);
			retReac = project.getReactor(className, null);		
		}
		
		// else try to find it the project the insight is saved in
		// loading it inside of version/classes
		if(retReac == null && this.projectId != null) {
			IProject project = Utility.getProject(this.projectId);
			retReac = project.getReactor(className, null);				
		}
		
		// set the insight into the reactor
		if(retReac != null) {
			retReac.setInsight(this);
		}
		
		return retReac;
	}
	
	public void resetClassCache() {
		String key = InsightCustomReactorCompilator.getKey(this);
		InsightCustomReactorCompilator.reset(key);
	}

	public String getCP() {
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
	        File file = new File(Utility.normalizePath(insightFolder));
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
	
	public void setFinalViewOptions(String panelId, SelectQueryStruct qs, TaskOptions taskOptions, IFormatter formatter) {
		if(insightPanels.containsKey(panelId)) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if(panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			panel.setFinalViewOptions(qs, taskOptions, formatter);
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
			return panel.getLastTaskOptions();
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
	
	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}
	
	public String getBaseURL() {
		return this.baseURL;
	}
	
	public String getInsightURL() {
		StringBuilder retURL = new StringBuilder(this.baseURL);
		retURL.append("insight?engine=").append(projectId).append("&").append("id=").append(rdbmsId);
		return retURL.toString();
	}

	public String getLiveURL() {
		StringBuilder retURL = new StringBuilder(this.baseURL);
		retURL.append("insight?insightId=").append(insightId);
		return retURL.toString();
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

	/**
	 * Set the current context for the user
	 * @param context
	 * @return
	 */
	//TODO: on tomcat side, when context changes needs to be told
	//TODO: on tomcat side, when context changes needs to be told
	//TODO: on tomcat side, when context changes needs to be told
	//TODO: on tomcat side, when context changes needs to be told
	public boolean setContext(String projectId) 
	{
		// sets the context space for the user
		// also set the cmd context right here
		if(this.contextProjectId != null && this.contextProjectId.equals(projectId)) {
//			throw new IllegalArgumentException("Already in the context");
			return true;
		}
		if(this.user != null) {
			String context = null;
			Map <String, StringBuffer> varMap = this.user.getVarMap();
			if(!varMap.containsKey(projectId)) {
				// assume the context is currently the project id
				// and we will add it and get back the varname that was used
				// which will then be the actual context that is set
				context = this.user.addVarMap(projectId); 
			}
			if(varMap.containsKey(context)) {
				this.user.setContext(context);
				this.contextProjectId = projectId;
				this.contextProjectName = SecurityProjectUtils.getProjectAliasForId(projectId);
				
				contextReinitialized = true;
				// we need to find a way to serialize the insight here
				//InsightSerializer is = new InsightSerializer(this);
				//is.serializeInsight(true); // force it. the context may have changed

				
				return true;
			}
			return false;
		}
		// should we allow this if no one is logged in?
		else {
			String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
			String mountDir = AssetUtility.getProjectVersionFolder(projectName, projectId);
	
			this.cmdUtil = new CmdExecUtil(projectName, mountDir, this.user.getSocketClient(false));
			this.contextProjectId = projectId;
			return true;
		}
		
	}
	
	public CmdExecUtil getCmdUtil() {
		if(this.user != null) {
			return this.user.getCmdUtil();
		} else {
			return this.cmdUtil;
		}
	}
	
	public ITableDataFrame getFrame(String frameName) {
		return this.varStore.getFrame(frameName);
	}
	
	public boolean addVariable(Variable var) {
		// check to see if the frames are there
		// they may want to use it with a non-semoss frame ?
		List <String> varFrames = var.getFrames();
		boolean frameFound = true;
		for(String varFrame : varFrames) {
			frameFound = getFrame(varFrame) != null;
			if(!frameFound) {
				return false;
			}
		}

		NounMetadata varNoun = new NounMetadata(var, PixelDataType.VARIABLE);
		this.varStore.put(var.getName(), varNoun);
		return true;
	}
	
	public Variable getVariable(String name) {
		NounMetadata varNoun = this.varStore.get(name);
		if(varNoun == null) {
			return null;
		}
		return (Variable) varNoun.getValue();
	}

	public void removeVariable(String name) {
		this.varStore.remove(name);
	}
	
	public List<String> getAllVars() {
		return this.varStore.getDynamicVarKeys();
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
	
	public NounMetadata setInsightFilterRefresh(boolean filterRefresh) {
		NounMetadata noun = new NounMetadata(filterRefresh, PixelDataType.BOOLEAN);
		this.varStore.put(FILTER_REFRESH_KEY, noun);
		return noun;
	}
	
	public Boolean getInsightFilterRefresh() {
		return (Boolean) getVar(FILTER_REFRESH_KEY);
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
			this.pyt = PySingleton.getTranslator();			
		}		
		
		if(this.pyt == null) {
			this.pyt = user.getPyTranslator();
			if(this.pyt == null) {
				throw new NullPointerException("Could not create python translator");
			}
			// need to recreate the translator
			if(this.pyt instanceof TCPPyTranslator) {
				SocketClient nc1 = ((TCPPyTranslator)pyt).getSocketClient();
				this.pyt = new TCPPyTranslator();
				((TCPPyTranslator) this.pyt).setSocketClient(nc1);
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
	
	public void setNettyClient(SocketClient nc) {
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
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if(this.nc != null) {
			//nc.disconnect();
		}
	}

	///////////////////////////////////////// END PYTHON SPECIFIC METHODS ///////////////////////////////////////////

	public ChromeDriverUtility getChromeDriver() {
		if(this.chromeUtil == null) {
			chromeUtil = new ChromeDriverUtility();
		}
		return chromeUtil;
	}
	
	@Override
	protected void finalize() throws Throwable {
		logger.info("Insight " + this.insightId + " is being gc'd");
	}
	
	
	// query the frame and get the data
	public Object query(String sql, String srcFrameName) {
		ITableDataFrame frame = null;
		if(srcFrameName != null && !srcFrameName.isEmpty()) {
			NounMetadata noun = this.varStore.get(srcFrameName);
			if(noun == null) {
				throw new IllegalArgumentException("Unable to find frame = " + srcFrameName);
			}
			frame = (ITableDataFrame) noun.getValue();
		} else {
			frame = getCurFrame();
		}
		return frame.querySQL(sql);
	}
	
	// query the frame and get the data
	public Object queryCSV(String sql, String srcFrameName) {
		ITableDataFrame frame = null;
		if(srcFrameName != null && !srcFrameName.isEmpty()) {
			NounMetadata noun = this.varStore.get(srcFrameName);
			if(noun == null) {
				throw new IllegalArgumentException("Unable to find frame = " + srcFrameName);
			}
			frame = (ITableDataFrame) noun.getValue();
		} else {
			frame = getCurFrame();
		}
		return frame.queryCSV(sql);
	}

	// query the frame and get the data
	public Object queryJSON(String sql, String srcFrameName) {
		ITableDataFrame frame = null;
		if(srcFrameName != null && !srcFrameName.isEmpty()) {
			NounMetadata noun = this.varStore.get(srcFrameName);
			if(noun == null) {
				throw new IllegalArgumentException("Unable to find frame = " + srcFrameName);
			}
			frame = (ITableDataFrame) noun.getValue();
		} else {
			frame = getCurFrame();
		}
		return frame.queryJSON(sql);
	}
	
	public String getREnv() {
		return this.rEnvName;
	}
	
	public void setSerialized(boolean serialized)
	{
		this.user.setInsightSerialization(insightId, serialized);
	}
	
	public boolean getSerialized()
	{
		return 	this.user.getInsightSerialization(insightId);
	}
	
	public void setContextReinitialized(boolean contextReinitialized)
	{
		this.contextReinitialized = contextReinitialized;
	}
	
	public boolean getContextReinitialized()
	{
		return this.contextReinitialized;
	}
	
	public String setSQLWrapper(String sql, GenExpressionWrapper wrapper)
	{
		String id = "id" + idCount;
		idCount++;
		this.sqlWrapperMap.put(sql, wrapper);
		this.id2SQLMapper.put(id, sql);
		return id; 
	}
	
	public GenExpressionWrapper getSQLWrapper(String id)
	{
		String sql = this.id2SQLMapper.get(id);
		return this.sqlWrapperMap.get(sql);
	}

	public void removeSQLWrapper(String id)
	{
		String sql = this.id2SQLMapper.get(id);
		id2SQLMapper.remove(id);
		this.sqlWrapperMap.remove(sql);
	}
	public void replaceWrapper(String id, String sql, GenExpressionWrapper wrapper)
	{
		String origSql = this.id2SQLMapper.get(id);
		id2SQLMapper.put(id, sql);
		this.sqlWrapperMap.put(sql, wrapper);
		this.sqlWrapperMap.remove(origSql);
		
	}
}
