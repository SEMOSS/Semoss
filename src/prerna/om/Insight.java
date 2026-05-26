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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.PyTranslator;
import prerna.engine.impl.model.Room;
import prerna.project.api.IProject;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.IReactor;
import prerna.reactor.browser.PlaywrightBrowserUtil;
import prerna.reactor.export.IFormatter;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.reactor.frame.r.util.TCPRTranslator;
import prerna.reactor.insights.SetInsightConfigReactor;
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
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class Insight implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final String DEFAULT_SHEET_ID = "0";
	public static final String DEFAULT_SHEET_LABEL = "Sheet1";

	private static final Logger classLogger = LogManager.getLogger(Insight.class);
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// special VarStore key pointing to the currently active frame
	public static transient final String CUR_FRAME_KEY = "$CUR_FRAME_KEY";
	private static transient final String INSIGHT_FOLDER_KEY = "INSIGHT_FOLDER";
	public static transient final String FILTER_REFRESH_KEY = "$FILTER_REFRESH";

	// unique ID assigned by the InsightCache; varies per live instance
	protected String insightId;

	// user bound to this insight
	protected User user;
	protected String insightName;

	// set only for saved insights
	protected String rdbmsId;
	protected String projectId;
	protected String projectName;
	protected boolean cacheable = true;
	protected int cacheMinutes = -1;
	protected String cacheCron;
	protected boolean cacheEncrypt = false;
	private transient ZonedDateTime cachedDateTime = null;

	// true when this insight was opened from the old (pre-pixel) format
	protected boolean isOldInsight = false;

	// room ID when this insight is associated with an agent room
	protected String roomId;

	// ordered log of every pixel expression run against this insight
	private transient PixelList pixelList;

	// holds pixel variable assignments and named frames
	private transient VarStore varStore = new VarStore();

	// outbound messages that are deferred until the next flush
	private transient BlockingQueue<NounMetadata> delayedMessages = new ArrayBlockingQueue<NounMetadata>(1024);

	// active iterators / task handles returned by data frame operations
	private transient TaskStore taskStore;

	// temporary single-column frames used by the filter-model feature
	private transient Map<String, ITableDataFrame> cachedFitlerModelFrame = new HashMap<>();

	// UI layout: sheets are tab-level containers; panels live inside sheets
	private transient Map<String, InsightSheet> insightSheets = new LinkedHashMap<String, InsightSheet>();
	private transient Map<String, InsightPanel> insightPanels = new LinkedHashMap<String, InsightPanel>();
	private transient Map<String, Object> insightOrnament = new ConcurrentHashMap<String, Object>();

	// shared R translator for the lifetime of this insight
	private transient AbstractRJavaTranslator rJavaTranslator;
	private String rEnvName = null;

	// shared Python translator for the lifetime of this insight
	private transient PyTranslator pyTranslator;

	/*
	 * TODO: find a better way of doing this keep a list of all the files that are
	 * used to create this insight this is important so we can save those files into
	 * full databases if the insight is saved
	 */
	private transient String insightFolder;
	private transient String appFolder;
	private transient String userFolder;
	private transient List<InsightFile> loadInsightFiles = new Vector<>();
	private transient Map<String, InsightFile> exportInsightFiles = new HashMap<>();

	private transient boolean deleteFilesOnDropInsight = true;
	private transient boolean deleteREnvOnDropInsight = true;
	private transient boolean deletePythonGlobalsOnDropInsight = true;

	private transient boolean isSchedulerMode = false;
	private transient boolean isSavedInsightMode = false;

	private transient Set<String> queriedDatabaseIds = new HashSet<String>();

	// last panel touched by a reactor (used as default for getLastTaskOptions etc.)
	private String lastPanelId = null;

	// pragma settings (cache, raw, parquet, etc.) active for this insight
	private Map<String, Object> pragmap = new HashMap<>();

	// base URL used when building shareable insight links
	private String baseURL = null;

	// shell-command executor; working directory tracks the active context project
	private CmdExecUtil cmdUtil = null;

	// active project context used to resolve project-scoped reactors and assets
	private String contextProjectId = null;
	private String contextProjectName = null;

	// set to true after setContext() successfully changes the context
	private boolean contextReinitialized = false;

	// browser automation utilities
	private transient ChromeDriverUtility chromeUtil = null;
	private transient PlaywrightBrowserUtil playwrightUtil = null;

	// SQL expression wrappers keyed by an auto-generated ID
	Map<String, GenExpressionWrapper> sqlWrapperMap = new HashMap<String, GenExpressionWrapper>();
	Map<String, String> id2SQLMapper = new HashMap<String, String>();
	int idCount = 0;

	////////////////////////////////////////////////////////////////
	// CONSTRUCTORS & INITIALIZATION
	////////////////////////////////////////////////////////////////

	/**
	 * Creates a new transient (unsaved) insight with default task-queue capacity. A
	 * default sheet is added so legacy pixels that omit an explicit sheet still
	 * work correctly.
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
	 * Creates a saved insight tied to a specific project database record.
	 *
	 * @param projectId    the project this insight belongs to
	 * @param projectName  human-readable project alias
	 * @param rdbmsId      the database row ID for this saved insight
	 * @param cacheable    whether results may be cached
	 * @param cacheMinutes TTL for the cache in minutes; -1 means no expiry
	 * @param cacheCron    optional cron expression for scheduled cache refresh
	 * @param cacheEncrypt whether cached data should be encrypted at rest
	 * @param capacity     initial pixel-list capacity
	 */
	public Insight(String projectId, String projectName, String rdbmsId, boolean cacheable, int cacheMinutes,
			String cacheCron, boolean cacheEncrypt, int capacity) {
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
	 * Initializes fields that are common to all constructors: pixel list, task
	 * store, and a fresh insight ID. Also applies any server-level pragma defaults
	 * and, when chroot is enabled, symlinks the insight folder for the user.
	 *
	 * @param capacity initial capacity for the pixel list
	 */
	private void loadDefaultSettings(int capacity) {
		this.pixelList = new PixelList(capacity);
		this.taskStore = new TaskStore();
		this.insightId = GUID.v7().toUUID().toString();

		// put the pragmap
		if (Utility.getDIHelperProperty("X_CACHE") != null
				&& !Utility.getDIHelperProperty("X_CACHE").trim().isEmpty()) {
			this.pragmap.put("xCache", Utility.getDIHelperProperty("X_CACHE").trim());
		}
		// put the pragmap
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE) + "")) {
			if (this.user != null) {
				this.user.getUserSymlinkHelper().symlinkFolder(getInsightFolder());
			}
		}
	}

	////////////////////////////////////////////////////////////////
	// PIXEL EXECUTION
	////////////////////////////////////////////////////////////////

	/**
	 * Runs a single pixel expression in the current insight context.
	 *
	 * @param pixelString the pixel expression to execute
	 * @return the PixelRunner containing execution results
	 */
	public PixelRunner runPixel(String pixelString) {
		List<String> pixelList = new ArrayList<String>();
		pixelList.add(pixelString);
		return runPixel(pixelList);
	}

	/**
	 * Runs a single pixel expression using an existing PixelRunner so results
	 * accumulate alongside prior executions.
	 *
	 * @param runner      the PixelRunner to append results to
	 * @param pixelString the pixel expression to execute
	 * @return the same PixelRunner with the new result appended
	 */
	public PixelRunner runPixel(PixelRunner runner, String pixelString) {
		List<String> pixelList = new ArrayList<String>();
		pixelList.add(pixelString);
		return runPixel(runner, pixelList);
	}

	/**
	 * Runs a list of pixel expressions in sequence using a fresh PixelRunner.
	 *
	 * @param pixelList ordered list of pixel expressions
	 * @return the PixelRunner containing all results
	 */
	public PixelRunner runPixel(List<String> pixelList) {
		return runPixel(getPixelRunner(), pixelList);
	}

	/**
	 * Runs a pixel expression with a pinned project context for this thread only.
	 * <p>
	 * The provided projectId/projectName are stored in ThreadStore for the duration
	 * of the call so that concurrent executions on the same Insight (e.g. parallel
	 * MCP tool calls) each see their own context without overwriting the shared
	 * instance fields. Any SetContext / LoadApp reactor inside the pixel string
	 * will also update the ThreadStore entry rather than the instance fields, so
	 * its effect is scoped to this thread and does not persist after the call
	 * returns.
	 *
	 * @param projectId   the project to set as context for this execution
	 * @param projectName human-readable alias for the project
	 * @param pixelString the pixel expression to execute
	 * @return the PixelRunner containing execution results
	 */
	public PixelRunner runPixelWithContext(String projectId, String projectName, String pixelString) {
		ThreadStore.setContextProjectIdOverride(projectId);
		ThreadStore.setContextProjectNameOverride(projectName);
		try {
			return runPixel(pixelString);
		} finally {
			ThreadStore.clearContextProjectOverride();
		}
	}

	/**
	 * Runs a list of pixel expressions in sequence using the provided PixelRunner.
	 * Each expression is logged before execution. A {@link SemossPixelException}
	 * that signals a halt stops the loop; all other exceptions are logged and
	 * execution continues with the next expression.
	 *
	 * @param runner    the PixelRunner to append results to
	 * @param pixelList ordered list of pixel expressions
	 * @return the same PixelRunner with all results appended
	 */
	public PixelRunner runPixel(PixelRunner runner, List<String> pixelList) {
		int size = pixelList.size();
		if (size == 0) {
			// set the insight in the runner as it is used
			// to flush to FE
			runner.setInsight(this);
		} else {
			for (int i = 0; i < size; i++) {
				String pixelString = pixelList.get(i);
				classLogger.info("Pixel >>> {}", Utility.cleanLogString(pixelString));
				try {
					runner.runPixel(pixelString, this);
				} catch (SemossPixelException e) {
					classLogger.error("Pixel execution halted for insight '{}', pixel: '{}'. Reason: {}",
							this.insightId, pixelString, e.getMessage(), e);
					if (!e.isContinueThreadOfExecution()) {
						break;
					}
				} catch (Exception e) {
					classLogger.error(
							"Unexpected error during pixel execution for insight '{}', pixel: '{}'. Error: {}",
							this.insightId, pixelString, e.getMessage(), e);
				}
			}
		}
		return runner;
	}

	/**
	 * Creates and returns a fresh PixelRunner for this insight.
	 *
	 * @return a new PixelRunner instance
	 */
	public PixelRunner getPixelRunner() {
		PixelRunner runner = new PixelRunner();
		return runner;
	}

	////////////////////////////////////////////////////////////////
	// RECIPE MANAGEMENT
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the raw pixel list backing this insight's recipe.
	 *
	 * @return the PixelList
	 */
	public PixelList getPixelList() {
		return this.pixelList;
	}

	/**
	 * Replaces the pixel list entirely.
	 *
	 * @param pixelList the new PixelList
	 */
	public void setPixelList(PixelList pixelList) {
		this.pixelList = pixelList;
	}

	/**
	 * Returns the optimized (deduplicated/compressed) form of the pixel recipe. The
	 * optimizer removes redundant steps so the insight can be reproduced with fewer
	 * expressions.
	 *
	 * @return ordered list of optimized pixel expressions
	 */
	public List<String> getOptimizedPixelRecipe() {
		GetOptimizedRecipeReactor optimizer = new GetOptimizedRecipeReactor();
		List<String> recipe = optimizer.getOptimizedRecipe(this.pixelList.getPixelRecipe());
		return recipe;
	}

	/**
	 * Replaces the current pixel recipe with the provided list, clearing any
	 * previously recorded steps.
	 *
	 * @param pixelRecipe the replacement recipe
	 */
	public void setPixelRecipe(List<String> pixelRecipe) {
		this.pixelList.clear();
		this.pixelList.addPixel(pixelRecipe);
	}

	/**
	 * Re-runs the insight using the optimized recipe after closing all active
	 * frames and clearing the variable store and panels.
	 *
	 * @return the PixelRunner from the re-execution
	 */
	public PixelRunner reRunOptimizedPixelInsight() {
		Set<String> keys = this.varStore.getKeys();
		for (String key : keys) {
			NounMetadata noun = this.varStore.get(key);
			if (noun.getValue() instanceof ITableDataFrame) {
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
	 * Re-runs the full saved insight recipe, restoring parameters and preserving
	 * pixel IDs so that the frontend can correlate results with prior positions.
	 *
	 * @param appendInsightConfig whether to prepend the stored insight config pixel
	 * @return the PixelRunner from the re-execution
	 */
	public PixelRunner reRunPixelInsight(boolean appendInsightConfig) {
		return reRunPixelInsight(appendInsightConfig, false);
	}

	/**
	 * Re-runs the full saved insight recipe with optional creation of an initial
	 * panel 0. Restores parameters and preserves pixel IDs.
	 *
	 * @param appendInsightConfig whether to prepend the stored insight config pixel
	 * @param appendPanel0        whether to pre-create panel "0" before execution
	 * @return the PixelRunner from the re-execution
	 */
	public PixelRunner reRunPixelInsight(boolean appendInsightConfig, boolean appendPanel0) {
		synchronized (this) {
			// set the mode
			setRunSavedInsightMode(true);

			Map<String, NounMetadata> currentParameters = this.varStore.pullParameters();
			Map<String, NounMetadata> preAppliedParameters = this.varStore.pullPreAppliedParameters();

			// always add the insight config
			boolean hasInsightConfig = false;
			if (appendInsightConfig) {
				NounMetadata noun = varStore.get(SetInsightConfigReactor.INSIGHT_CONFIG);
				if (noun != null) {
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
			if (!this.isSavedInsight()) {
				this.insightSheets.put(DEFAULT_SHEET_ID, new InsightSheet(DEFAULT_SHEET_ID, DEFAULT_SHEET_LABEL));
			}
			// clear the panels
			this.insightPanels.clear();
			if (appendPanel0) {
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
			for (String paramKey : currentParameters.keySet()) {
				this.varStore.put(paramKey, currentParameters.get(paramKey));
			}

			// add back the preApplied parameters
			// so that we can set the value inside of them
			for (String paramKey : preAppliedParameters.keySet()) {
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
			if (hasInsightConfig) {
				size--;
			}
			for (int i = 0; i < size; i++) {
				String id = currentPixelIds.get(i);
				Map<String, Object> position = currentPixelPositions.get(i);
				Pixel p = pixelReturns.get(i);
				p.setId(id);
				if (position != null && !position.isEmpty()) {
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

	////////////////////////////////////////////////////////////////
	// PROJECT CONTEXT
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the effective context project ID for the current thread. During a
	 * {@link #runPixelWithContext} call the ThreadStore override is returned;
	 * otherwise the shared instance field is used. This means multiple concurrent
	 * scoped executions on the same Insight each see their own project without
	 * interfering with one another.
	 *
	 * @return the active context project ID, or null if none is set
	 */
	public String getContextProjectId() {
		String override = ThreadStore.getContextProjectIdOverride();
		return override != null ? override : contextProjectId;
	}

	/**
	 * Directly sets the shared context project ID field, bypassing permission
	 * checks. Prefer {@link #setContext(String)} in most situations.
	 *
	 * @param contextProjectId the project ID to set
	 */
	public void setContextProjectId(String contextProjectId) {
		this.contextProjectId = contextProjectId;
	}

	/**
	 * Returns the effective context project name for the current thread. Follows
	 * the same ThreadStore-first lookup as {@link #getContextProjectId()}.
	 *
	 * @return the active context project name, or null if none is set
	 */
	public String getContextProjectName() {
		String override = ThreadStore.getContextProjectNameOverride();
		return override != null ? override : contextProjectName;
	}

	/**
	 * Directly sets the shared context project name field, bypassing permission
	 * checks. Prefer {@link #setContext(String)} in most situations.
	 *
	 * @param contextProjectName the project name to set
	 */
	public void setContextProjectName(String contextProjectName) {
		this.contextProjectName = contextProjectName;
	}

	/**
	 * Switches the active project context after verifying that the current user has
	 * view access to the target project.
	 * <p>
	 * When called inside a {@link #runPixelWithContext} scoped execution (detected
	 * via a non-null ThreadStore override) the change is written to ThreadStore
	 * only and does not touch the shared instance fields. This keeps the context
	 * change thread-local: it is visible for the remainder of that pixel execution
	 * but does not persist to the session afterward and does not affect other
	 * concurrent threads.
	 * <p>
	 * In a normal (non-scoped) execution the instance fields are updated as before.
	 *
	 * @param projectId the project to switch to
	 * @return true if the context was successfully updated; false if the user lacks
	 *         access or the project could not be found
	 */
	// TODO: on tomcat side, when context changes needs to be told
	public boolean setContext(String projectId) {
		boolean inScopedExecution = ThreadStore.getContextProjectIdOverride() != null;

		// check against the effective context - ThreadStore override takes precedence
		String effectiveContextId = inScopedExecution ? ThreadStore.getContextProjectIdOverride()
				: this.contextProjectId;
		if (effectiveContextId != null && effectiveContextId.equals(projectId)) {
			return true;
		}

		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			// clear out the current context even if this failed
			if (inScopedExecution) {
				ThreadStore.clearContextProjectOverride();
			} else {
				this.contextProjectId = null;
				this.contextProjectName = null;
			}
			return false;
		}

		String resolvedName = SecurityProjectUtils.getProjectAliasForId(projectId);
		if (inScopedExecution) {
			// keep the context change thread-local; do not touch instance fields
			ThreadStore.setContextProjectIdOverride(projectId);
			ThreadStore.setContextProjectNameOverride(resolvedName);
		} else {
			this.contextProjectId = projectId;
			this.contextProjectName = resolvedName;
		}

		User user = getUser();
		if (user != null) {
			// if we have a chroot, mount the project for that user.
			if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
				user.getUserSymlinkHelper().symlinkProject(this.user, projectId);
			}

			String appRootFolder = AssetUtility.getProjectAssetsFolder(resolvedName, projectId);
			this.getCmdUtil().setWorkingDir(appRootFolder);
		}

		// if we have a chroot, mount the project for that user.
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			this.user.getUserSymlinkHelper().symlinkProject(this.user, projectId);
		}

		this.contextReinitialized = true;
		return true;
	}

	/**
	 * Returns whether {@link #setContext(String)} has been called successfully
	 * since the flag was last reset.
	 *
	 * @return true if the context was re-initialized
	 */
	public boolean getContextReinitialized() {
		return this.contextReinitialized;
	}

	/**
	 * Resets the context-reinitialized flag.
	 *
	 * @param contextReinitialized the new flag value
	 */
	public void setContextReinitialized(boolean contextReinitialized) {
		this.contextReinitialized = contextReinitialized;
	}

	/**
	 * Resolves a reactor class by name, checking the active context project first
	 * and falling back to the project the insight is saved in. Returns null if no
	 * matching reactor is found in either project.
	 *
	 * @param className the fully-qualified or short reactor class name
	 * @return the resolved IReactor with this insight set on it, or null
	 */
	public IReactor getReactor(String className) {
		IReactor retReac = null;

		// user has manually set the specific context
		String contextProjectId = getContextProjectId();
		if (contextProjectId != null) {
			IProject project = Utility.getProject(contextProjectId);
			retReac = project.getReactor(className);
		}

		// else try to find it the project the insight is saved in
		// loading it inside of version/classes
		if (retReac == null && this.projectId != null) {
			IProject project = Utility.getProject(this.projectId);
			retReac = project.getReactor(className);
		}

		// set the insight into the reactor
		if (retReac != null) {
			retReac.setInsight(this);
		}

		return retReac;
	}

	////////////////////////////////////////////////////////////////
	// IDENTITY & METADATA
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the unique runtime ID assigned to this insight instance.
	 *
	 * @return the insight ID
	 */
	public String getInsightId() {
		return this.insightId;
	}

	/**
	 * Overrides the insight ID. Use with caution: the ID is used as a cache key.
	 *
	 * @param insightId the new insight ID
	 */
	public void setInsightId(String insightId) {
		this.insightId = insightId;
	}

	/**
	 * Returns the display name of this insight.
	 *
	 * @return the insight name, or null if not set
	 */
	public String getInsightName() {
		return insightName;
	}

	/**
	 * Sets the display name of this insight.
	 *
	 * @param insightName the name to set
	 */
	public void setInsightName(String insightName) {
		this.insightName = insightName;
	}

	/**
	 * Returns the database row ID for this saved insight.
	 *
	 * @return the rdbms ID, or null for transient insights
	 */
	public String getRdbmsId() {
		return rdbmsId;
	}

	/**
	 * Sets the database row ID for this insight.
	 *
	 * @param rdbmsId the rdbms ID
	 */
	public void setRdbmsId(String rdbmsId) {
		this.rdbmsId = rdbmsId;
	}

	/**
	 * Returns the ID of the project this insight is saved in.
	 *
	 * @return the project ID, or null for transient insights
	 */
	public String getProjectId() {
		return projectId;
	}

	/**
	 * Sets the project ID for this insight.
	 *
	 * @param projectId the project ID
	 */
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	/**
	 * Returns the human-readable alias of the project this insight belongs to.
	 *
	 * @return the project name
	 */
	public String getProjectName() {
		return projectName;
	}

	/**
	 * Sets the project name for this insight.
	 *
	 * @param projectName the project name
	 */
	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	/**
	 * Returns true when this insight is backed by a saved record (both projectId
	 * and rdbmsId are non-null).
	 *
	 * @return true if this is a saved insight
	 */
	public boolean isSavedInsight() {
		return this.projectId != null && this.rdbmsId != null;
	}

	/**
	 * Returns the room ID when this insight is associated with an agent room.
	 *
	 * @return the room ID, or null if not in a room
	 */
	public String getRoomId() {
		return this.roomId;
	}

	/**
	 * Binds this insight to an agent room, setting the room ID and re-rooting the
	 * insight folder to the room's folder path.
	 *
	 * @param room the Room to associate with
	 */
	public void setRoomForInsight(Room room) {
		this.roomId = room.getId();
		this.insightFolder = room.getRoomFolderPath();
	}

	////////////////////////////////////////////////////////////////
	// USER
	////////////////////////////////////////////////////////////////

	/**
	 * Sets the user associated with this insight.
	 *
	 * @param user the user
	 */
	public void setUser(User user) {
		this.user = user;
	}

	/**
	 * Returns the user for this insight. If no user was explicitly set, falls back
	 * to the ThreadStore user (set during request processing).
	 *
	 * @return the user, or null if neither the instance field nor ThreadStore has
	 *         one
	 */
	public User getUser() {
		if (this.user == null) {
			return ThreadStore.getUser();
		}
		return this.user;
	}

	/**
	 * Returns the user's ID for a specific auth provider.
	 *
	 * @param provider the auth provider to look up
	 * @return the user ID string, or "-1" if no user is set
	 */
	public String getUserId(AuthProvider provider) {
		if (this.user == null) {
			return "-1";
		}
		return user.getAccessToken(provider).getId();
	}

	/**
	 * Returns the user's ID using the primary login provider. Returns "-1" for
	 * anonymous or unset users.
	 *
	 * @return the user ID string
	 */
	public String getUserId() {
		if (this.user == null || this.user.isAnonymous()) {
			return "-1";
		}
		return user.getAccessToken(user.getLogins().get(0)).getId();
	}

	////////////////////////////////////////////////////////////////
	// VARIABLE STORE
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the VarStore holding all pixel variable assignments for this insight.
	 *
	 * @return the VarStore
	 */
	public VarStore getVarStore() {
		return this.varStore;
	}

	/**
	 * Replaces the VarStore entirely.
	 *
	 * @param varStore the new VarStore
	 */
	public void setVarStore(VarStore varStore) {
		this.varStore = varStore;
	}

	/**
	 * Returns the unwrapped value stored under a VarStore key, or null if the key
	 * is not present.
	 *
	 * @param varName the variable name
	 * @return the stored value, or null
	 */
	public Object getVar(String varName) {
		Object retObject = this.varStore.get(varName);
		if (retObject != null) {
			return ((NounMetadata) retObject).getValue();
		}
		return null;
	}

	/**
	 * Registers a named variable in the VarStore after verifying that all frames
	 * referenced by the variable exist in this insight.
	 *
	 * @param var the Variable to add
	 * @return true if all referenced frames were found and the variable was stored;
	 *         false if any referenced frame is missing
	 */
	public boolean addVariable(Variable var) {
		// check to see if the frames are there
		// they may want to use it with a non-semoss frame ?
		List<String> varFrames = var.getFrames();
		boolean frameFound = true;
		for (String varFrame : varFrames) {
			frameFound = getFrame(varFrame) != null;
			if (!frameFound) {
				return false;
			}
		}

		NounMetadata varNoun = new NounMetadata(var, PixelDataType.VARIABLE);
		this.varStore.put(var.getName(), varNoun);
		return true;
	}

	/**
	 * Retrieves a typed Variable from the VarStore by name.
	 *
	 * @param name the variable name
	 * @return the Variable, or null if not present
	 */
	public Variable getVariable(String name) {
		NounMetadata varNoun = this.varStore.get(name);
		if (varNoun == null) {
			return null;
		}
		return (Variable) varNoun.getValue();
	}

	/**
	 * Removes a variable from the VarStore by name.
	 *
	 * @param name the variable name
	 */
	public void removeVariable(String name) {
		this.varStore.remove(name);
	}

	/**
	 * Returns the names of all dynamic (non-system) variables currently in the
	 * VarStore.
	 *
	 * @return list of variable names
	 */
	public List<String> getAllVars() {
		return this.varStore.getDynamicVarKeys();
	}

	/**
	 * Writes the filter-refresh flag to the VarStore and returns the resulting
	 * NounMetadata.
	 *
	 * @param filterRefresh the flag value
	 * @return the NounMetadata wrapping the flag
	 */
	public NounMetadata setInsightFilterRefresh(boolean filterRefresh) {
		NounMetadata noun = new NounMetadata(filterRefresh, PixelDataType.BOOLEAN);
		this.varStore.put(FILTER_REFRESH_KEY, noun);
		return noun;
	}

	/**
	 * Returns the current filter-refresh flag from the VarStore, or null if not
	 * set.
	 *
	 * @return the Boolean flag, or null
	 */
	public Boolean getInsightFilterRefresh() {
		return (Boolean) getVar(FILTER_REFRESH_KEY);
	}

	////////////////////////////////////////////////////////////////
	// FRAMES & QUERIES
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the currently active data frame ({@link #CUR_FRAME_KEY}), or null if
	 * no frame has been set.
	 *
	 * @return the current ITableDataFrame, or null
	 */
	public ITableDataFrame getCurFrame() {
		Object frame = getDataMaker();
		if (frame != null) {
			return (ITableDataFrame) frame;
		}
		return null;
	}

	/**
	 * Returns the data frame stored under the given name in the VarStore.
	 *
	 * @param frameName the VarStore key for the frame
	 * @return the ITableDataFrame, or null if not present
	 */
	public ITableDataFrame getFrame(String frameName) {
		return this.varStore.getFrame(frameName);
	}

	/**
	 * Sets the active data maker (frame) under the {@link #CUR_FRAME_KEY} slot in
	 * the VarStore, marking it as the current frame.
	 *
	 * @param datamaker the IDataMaker to set as current
	 */
	public void setDataMaker(IDataMaker datamaker) {
		this.varStore.put(CUR_FRAME_KEY, new NounMetadata(datamaker, PixelDataType.FRAME, PixelOperationType.FRAME));
	}

	/**
	 * Returns the active IDataMaker from the VarStore, or null if no frame is set.
	 *
	 * @return the IDataMaker
	 */
	public IDataMaker getDataMaker() {
		NounMetadata curFrameNoun = this.varStore.get(CUR_FRAME_KEY);
		if (curFrameNoun != null) {
			return ((IDataMaker) curFrameNoun.getValue());
		}
		return null;
	}

	/**
	 * Executes a SQL query against the named frame, or the current frame if no name
	 * is provided.
	 *
	 * @param sql          the SQL to execute
	 * @param srcFrameName the frame to query; null or empty uses the current frame
	 * @return the query result object
	 * @throws IllegalArgumentException if the named frame cannot be found
	 */
	public Object query(String sql, String srcFrameName) {
		ITableDataFrame frame = null;
		if (srcFrameName != null && !srcFrameName.isEmpty()) {
			NounMetadata noun = this.varStore.get(srcFrameName);
			if (noun == null) {
				throw new IllegalArgumentException("Unable to find frame = " + srcFrameName);
			}
			frame = (ITableDataFrame) noun.getValue();
		} else {
			frame = getCurFrame();
		}
		return frame.querySQL(sql);
	}

	/**
	 * Executes a SQL query against the named frame and returns the result as CSV.
	 *
	 * @param sql          the SQL to execute
	 * @param srcFrameName the frame to query; null or empty uses the current frame
	 * @return the CSV result
	 * @throws IllegalArgumentException if the named frame cannot be found
	 */
	public Object queryCSV(String sql, String srcFrameName) {
		ITableDataFrame frame = null;
		if (srcFrameName != null && !srcFrameName.isEmpty()) {
			NounMetadata noun = this.varStore.get(srcFrameName);
			if (noun == null) {
				throw new IllegalArgumentException("Unable to find frame = " + srcFrameName);
			}
			frame = (ITableDataFrame) noun.getValue();
		} else {
			frame = getCurFrame();
		}
		return frame.queryCSV(sql);
	}

	/**
	 * Executes a SQL query against the named frame and returns the result as JSON.
	 *
	 * @param sql          the SQL to execute
	 * @param srcFrameName the frame to query; null or empty uses the current frame
	 * @return the JSON result
	 * @throws IllegalArgumentException if the named frame cannot be found
	 */
	public Object queryJSON(String sql, String srcFrameName) {
		ITableDataFrame frame = null;
		if (srcFrameName != null && !srcFrameName.isEmpty()) {
			NounMetadata noun = this.varStore.get(srcFrameName);
			if (noun == null) {
				throw new IllegalArgumentException("Unable to find frame = " + srcFrameName);
			}
			frame = (ITableDataFrame) noun.getValue();
		} else {
			frame = getCurFrame();
		}
		return frame.queryJSON(sql);
	}

	/**
	 * Caches a single-column filter-model frame under the given key.
	 *
	 * @param uniqueKey the cache key
	 * @param tempFrame the frame to cache
	 */
	public void addCachedFitlerModelFrame(String uniqueKey, ITableDataFrame tempFrame) {
		this.cachedFitlerModelFrame.put(uniqueKey, tempFrame);
	}

	/**
	 * Returns the cached filter-model frame for the given key, or null if not
	 * present.
	 *
	 * @param uniqueKey the cache key
	 * @return the cached ITableDataFrame, or null
	 */
	public ITableDataFrame getCachedFitlerModelFrame(String uniqueKey) {
		return this.cachedFitlerModelFrame.get(uniqueKey);
	}

	/**
	 * Returns the full map of cached filter-model frames.
	 *
	 * @return map of cache key to ITableDataFrame
	 */
	public Map<String, ITableDataFrame> getCachedFilterModelFrame() {
		return this.cachedFitlerModelFrame;
	}

	/**
	 * Records that a database was queried during this insight's execution. Used for
	 * auditing and dependency tracking.
	 *
	 * @param databaseId the database engine ID that was queried
	 */
	public void addQueriedDatabases(String databaseId) {
		// this is a set
		this.queriedDatabaseIds.add(databaseId);
	}

	/**
	 * Returns all database IDs that have been queried during this insight session.
	 *
	 * @return set of database engine IDs
	 */
	public Set<String> getQueriedDatabaseIds() {
		return this.queriedDatabaseIds;
	}

	////////////////////////////////////////////////////////////////
	// PANELS & SHEETS
	////////////////////////////////////////////////////////////////

	/**
	 * Returns all insight panels keyed by panel ID.
	 *
	 * @return map of panel ID to InsightPanel
	 */
	public Map<String, InsightPanel> getInsightPanels() {
		return this.insightPanels;
	}

	/**
	 * Replaces all insight panels.
	 *
	 * @param insightPanels the new panels map
	 */
	public void setInsightPanels(Map<String, InsightPanel> insightPanels) {
		this.insightPanels = insightPanels;
	}

	/**
	 * Returns the insight panel with the given ID, or null if not found.
	 *
	 * @param panelId the panel ID
	 * @return the InsightPanel, or null
	 */
	public InsightPanel getInsightPanel(String panelId) {
		return this.insightPanels.get(panelId);
	}

	/**
	 * Registers a new panel in this insight.
	 *
	 * @param insightPanel the panel to add
	 */
	public void addNewInsightPanel(InsightPanel insightPanel) {
		this.insightPanels.put(insightPanel.getPanelId(), insightPanel);
	}

	/**
	 * Sets the final view options (query struct, task options, and formatter) on
	 * the named panel and records it as the last-touched panel.
	 *
	 * @param panelId     the target panel ID
	 * @param qs          the query struct defining what was visualized
	 * @param taskOptions the task/visualization options
	 * @param formatter   the output formatter
	 * @throws NullPointerException if the panel does not exist
	 */
	public void setFinalViewOptions(String panelId, SelectQueryStruct qs, TaskOptions taskOptions,
			IFormatter formatter) {
		if (insightPanels.containsKey(panelId)) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if (panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			panel.setFinalViewOptions(qs, taskOptions, formatter);
		}
		this.lastPanelId = panelId;
	}

	/**
	 * Stores the last query struct on the named panel.
	 *
	 * @param lastQs  the query struct to store
	 * @param panelId the target panel ID
	 * @throws NullPointerException if the panel does not exist
	 */
	public void setLastQS(SelectQueryStruct lastQs, String panelId) {
		if (panelId != null) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if (panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			panel.setLastQs(lastQs);
		}
	}

	/**
	 * Returns the last query struct from the named panel, or null if the panel does
	 * not exist.
	 *
	 * @param panelId the panel ID
	 * @return the last SelectQueryStruct, or null
	 */
	public SelectQueryStruct getLastQS(String panelId) {
		if (panelId != null && insightPanels.containsKey(panelId)) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if (panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			return panel.getLastQs();
		}
		return null;
	}

	/**
	 * Returns the last task options from the most recently touched panel.
	 *
	 * @return the TaskOptions, or null
	 */
	public TaskOptions getLastTaskOptions() {
		return getLastTaskOptions(lastPanelId);
	}

	/**
	 * Returns the last task options from the named panel, or null if the panel does
	 * not exist.
	 *
	 * @param panelId the panel ID
	 * @return the TaskOptions, or null
	 */
	public TaskOptions getLastTaskOptions(String panelId) {
		if (panelId != null && insightPanels.containsKey(panelId)) {
			InsightPanel panel = this.insightPanels.get(panelId);
			if (panel == null) {
				throw new NullPointerException("Panel " + panelId + " does not exist");
			}
			return panel.getLastTaskOptions();
		}
		return null;
	}

	/**
	 * Records the ID of the most recently touched panel.
	 *
	 * @param panelId the panel ID
	 */
	public void setLastPanelId(String panelId) {
		this.lastPanelId = panelId;
	}

	/**
	 * Returns the ID of the most recently touched panel.
	 *
	 * @return the last panel ID, or null
	 */
	public String getLastPanelId() {
		return this.lastPanelId;
	}

	/**
	 * Returns all insight sheets keyed by sheet ID.
	 *
	 * @return map of sheet ID to InsightSheet
	 */
	public Map<String, InsightSheet> getInsightSheets() {
		return this.insightSheets;
	}

	/**
	 * Replaces all insight sheets.
	 *
	 * @param insightSheets the new sheets map
	 */
	public void setInsightSheets(Map<String, InsightSheet> insightSheets) {
		this.insightSheets = insightSheets;
	}

	/**
	 * Returns the insight sheet with the given ID, or null if not found.
	 *
	 * @param sheetId the sheet ID
	 * @return the InsightSheet, or null
	 */
	public InsightSheet getInsightSheet(String sheetId) {
		return this.insightSheets.get(sheetId);
	}

	/**
	 * Registers a new sheet in this insight.
	 *
	 * @param insightSheet the sheet to add
	 */
	public void addNewInsightSheet(InsightSheet insightSheet) {
		this.insightSheets.put(insightSheet.getSheetId(), insightSheet);
	}

	////////////////////////////////////////////////////////////////
	// TASK STORE
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the TaskStore holding all active iterator / task handles for frames
	 * in this insight.
	 *
	 * @return the TaskStore
	 */
	public TaskStore getTaskStore() {
		return this.taskStore;
	}

	/**
	 * Replaces the TaskStore.
	 *
	 * @param taskStore the new TaskStore
	 */
	public void setTaskStore(TaskStore taskStore) {
		this.taskStore = taskStore;
	}

	////////////////////////////////////////////////////////////////
	// CACHING
	////////////////////////////////////////////////////////////////

	/**
	 * Returns whether this insight's results may be cached.
	 *
	 * @return true if caching is enabled
	 */
	public boolean isCacheable() {
		return this.cacheable;
	}

	/**
	 * Sets whether this insight's results may be cached.
	 *
	 * @param cacheable true to enable caching
	 */
	public void setCacheable(boolean cacheable) {
		this.cacheable = cacheable;
	}

	/**
	 * Returns the cache time-to-live in minutes. -1 means no expiry.
	 *
	 * @return cache TTL in minutes
	 */
	public int getCacheMinutes() {
		return cacheMinutes;
	}

	/**
	 * Sets the cache time-to-live in minutes.
	 *
	 * @param cacheMinutes TTL in minutes; -1 for no expiry
	 */
	public void setCacheMinutes(int cacheMinutes) {
		this.cacheMinutes = cacheMinutes;
	}

	/**
	 * Returns the cron expression used for scheduled cache refresh, or null if not
	 * set.
	 *
	 * @return the cache cron expression
	 */
	public String getCacheCron() {
		return cacheCron;
	}

	/**
	 * Sets the cron expression for scheduled cache refresh.
	 *
	 * @param cacheCron a valid cron expression
	 */
	public void setCacheCron(String cacheCron) {
		this.cacheCron = cacheCron;
	}

	/**
	 * Returns whether cached data is encrypted at rest.
	 *
	 * @return true if cache encryption is enabled
	 */
	public boolean isCacheEncrypt() {
		return this.cacheEncrypt;
	}

	/**
	 * Sets whether cached data should be encrypted at rest.
	 *
	 * @param cacheEncrypt true to encrypt
	 */
	public void setCacheEncrypt(boolean cacheEncrypt) {
		this.cacheEncrypt = cacheEncrypt;
	}

	/**
	 * Returns the timestamp of the last time this insight's cache was populated.
	 *
	 * @return the cached date-time, or null if never cached
	 */
	public ZonedDateTime getCachedDateTime() {
		return cachedDateTime;
	}

	/**
	 * Records the timestamp when the cache was last populated.
	 *
	 * @param cachedDateTime the timestamp to store
	 */
	public void setCachedDateTime(ZonedDateTime cachedDateTime) {
		this.cachedDateTime = cachedDateTime;
	}

	////////////////////////////////////////////////////////////////
	// RUNTIME TRANSLATORS (R, PYTHON, BROWSER)
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the shared R translator for this insight, creating it on first
	 * access. The logger is sourced from the provided class name.
	 *
	 * @param className the class name to use for the translator's logger
	 * @return the AbstractRJavaTranslator
	 */
	public AbstractRJavaTranslator getRJavaTranslator(String className) {
		Logger logger = LogManager.getLogger(className);
		return getRJavaTranslator(logger);
	}

	/**
	 * Returns the shared R translator for this insight, creating it on first access
	 * using the provided logger.
	 *
	 * @param logger the logger to pass to the translator factory
	 * @return the AbstractRJavaTranslator
	 */
	public AbstractRJavaTranslator getRJavaTranslator(Logger logger) {
		if (this.rJavaTranslator == null) {
			this.rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(this, logger);

			// set the netty client if the translator is TCP R translator
			if (this.rJavaTranslator instanceof TCPRTranslator) {
				// do this so that the netty client is initialized
				// getPyTranslator();
				// now set the netty client
				((TCPRTranslator) this.rJavaTranslator).setClient(this.user.getPythonSocketClient(true));
				this.rJavaTranslator.setInsight(this);
				this.rJavaTranslator.startR();
			}
		}
		return this.rJavaTranslator;
	}

	/**
	 * Replaces the R translator and records the R environment name from it.
	 *
	 * @param rJavaTranslator the new translator
	 */
	public void setRJavaTranslator(AbstractRJavaTranslator rJavaTranslator) {
		this.rJavaTranslator = rJavaTranslator;
		this.rEnvName = rJavaTranslator.env;
	}

	/**
	 * Returns true if an R translator has been created for this insight.
	 *
	 * @return true if R is instantiated
	 */
	public boolean rInstantiated() {
		return this.rJavaTranslator != null;
	}

	/**
	 * Returns the R environment name bound to this insight, or null if R has not
	 * been initialized.
	 *
	 * @return the R environment name
	 */
	public String getREnv() {
		return this.rEnvName;
	}

	/**
	 * Returns the shared Python translator for this insight. If the translator is
	 * null or its underlying socket is disconnected, a new one is created from the
	 * user's Python socket client.
	 *
	 * @return the PyTranslator
	 */
	public PyTranslator getPyTranslator() {
		if (this.pyTranslator == null || this.pyTranslator.getSocketClient() == null
				|| !this.pyTranslator.getSocketClient().isConnected()) {
			SocketClient sc = user.getPythonSocketClient(true);
			this.pyTranslator = new PyTranslator(sc, this);
		}
		return this.pyTranslator;
	}

	/**
	 * Returns the Chrome WebDriver utility for this insight, creating it on first
	 * access.
	 *
	 * @return the ChromeDriverUtility
	 */
	public ChromeDriverUtility getChromeDriver() {
		if (this.chromeUtil == null) {
			chromeUtil = new ChromeDriverUtility();
		}
		return chromeUtil;
	}

	/**
	 * Returns the Playwright browser utility bound to this insight, or null if
	 * Playwright has not been initialized.
	 *
	 * @return the PlaywrightBrowserUtil, or null
	 */
	public PlaywrightBrowserUtil getPlaywrightUtil() {
		return this.playwrightUtil;
	}

	/**
	 * Sets the Playwright browser utility for this insight.
	 *
	 * @param pbu the PlaywrightBrowserUtil to bind
	 */
	public void setPlaywrightUtil(PlaywrightBrowserUtil pbu) {
		this.playwrightUtil = pbu;
	}

	////////////////////////////////////////////////////////////////
	// SHELL EXECUTION
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the shell-command executor for this insight, creating it on first
	 * access. The initial working directory is the active context project's asset
	 * folder, or the chroot root if no project context is set.
	 *
	 * @return the CmdExecUtil
	 * @throws NullPointerException if no user is bound to this insight
	 */
	public CmdExecUtil getCmdUtil() {
		if (getUser() == null) {
			throw new NullPointerException("No user defined within the insight to get the shell utilities");
		}
		if (this.cmdUtil == null) {
			// if first time, set the working directory if we have it
			if (this.contextProjectId != null) {
				String appRootFolder = AssetUtility.getProjectAssetsFolder(this.contextProjectName,
						this.contextProjectId);
				this.cmdUtil = new CmdExecUtil(this.user, this.insightId, appRootFolder);
			} else if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
				this.cmdUtil = new CmdExecUtil(this.user, this.insightId, "/");
			}
		}
		return this.cmdUtil;
	}

	////////////////////////////////////////////////////////////////
	// FOLDERS & FILES
	////////////////////////////////////////////////////////////////

	/**
	 * Returns the filesystem path of this insight's working folder, creating the
	 * directory if it does not yet exist. For unsaved insights the folder is under
	 * the session cache directory; for saved insights it lives under the project
	 * version folder.
	 *
	 * @return the absolute insight folder path
	 */
	public String getInsightFolder() {
		if (this.insightFolder == null) {
			// account for unsaved insights vs. saved insights
			if (!isSavedInsight()) {
				String sessionId = ThreadStore.getSessionId();
				sessionId = InsightUtility.getFolderDirSessionId(sessionId);
				this.insightFolder = Utility.getInsightCacheDir() + DIR_SEPARATOR + sessionId + DIR_SEPARATOR
						+ this.insightId;
			} else {
				this.insightFolder = AssetUtility.getProjectVersionFolder(this.projectName, this.projectId)
						+ DIR_SEPARATOR + this.rdbmsId;
			}
		}

		// make the folder if it doesn't already exist
		File f = new File(Utility.normalizePath(this.insightFolder));
		if (!f.exists() || !f.isDirectory()) {
			f.mkdirs();
		}

		return this.insightFolder;
	}

	/**
	 * Overrides the insight folder path. Use when migrating or re-rooting the
	 * insight (e.g. when binding to a room).
	 *
	 * @param insightFolder the new folder path
	 */
	public void setInsightFolder(String insightFolder) {
		this.insightFolder = insightFolder;
	}

	/**
	 * Returns the project asset folder for this insight. Returns null for transient
	 * (unsaved) insights. Creates the folder if it does not exist.
	 *
	 * @return the absolute app-asset folder path, or null for transient insights
	 */
	public String getAppFolder() {
		if (this.appFolder == null) {
			// account for unsaved insights vs. saved insights
			if (!isSavedInsight()) {
				return null;
			} else {
				// grab from db folder... technically shouldn't be binding on db + we allow
				// multiple locations
				// need to grab from engine
				this.appFolder = AssetUtility.getProjectAssetsFolder(this.projectName, this.projectId);
				// if this folder does not exist create it and git init it
				File file = new File(appFolder);
				if (!file.exists()) {
					file.mkdir();
					// GitRepoUtils.init(appFolder);
				}
			}
		}

		return this.appFolder;
	}

	/**
	 * Overrides the app folder path.
	 *
	 * @param appFolder the new app folder path
	 */
	public void setAppFolder(String appFolder) {
		this.appFolder = appFolder;
	}

	/**
	 * Returns the versioned asset folder for the current user's personal asset
	 * project.
	 *
	 * @return the absolute user-asset folder path
	 */
	public String getUserFolder() {
		AuthProvider provider = user.getPrimaryLogin();
		String projectId = user.getAssetProjectId(provider);
		this.userFolder = AssetUtility.getUserAssetVersionFolder("Asset", projectId);
		return userFolder;
	}

	/**
	 * Resolves a (possibly relative) file path to an absolute path anchored at the
	 * insight folder. Paths beginning with {@code INSIGHT_FOLDER} are expanded to
	 * the real folder path. Relative paths are prefixed with the insight folder and
	 * the platform directory separator.
	 *
	 * @param filePath the path to resolve
	 * @return the absolute path
	 */
	public String getAbsoluteInsightFolderPath(String filePath) {
		// is this one that starts with INSIGHT_FOLDER
		if (filePath.startsWith(Insight.INSIGHT_FOLDER_KEY)) {
			filePath = Pattern.compile(Matcher.quoteReplacement(Insight.INSIGHT_FOLDER_KEY)).matcher(filePath)
					.replaceFirst(Matcher.quoteReplacement(getInsightFolder()));
		} else {
			// make sure this is not relative
			// if it is
			// turn to absolute based on the insight folder location
			if (!(new File(filePath).exists())) {
				String filePrefix = getInsightFolder();
				if (filePath.startsWith("\\") || filePath.startsWith("/")) {
					filePath = filePrefix + filePath;
				} else {
					filePath = filePrefix + DIR_SEPARATOR + filePath;
				}
			}
		}

		return filePath;
	}

	/**
	 * Registers a file export under a unique key so it can be retrieved later by
	 * the frontend or a download reactor.
	 *
	 * @param uniqueKey    the key to register under
	 * @param fileLocation the InsightFile descriptor
	 */
	public void addExportFile(String uniqueKey, InsightFile fileLocation) {
		this.exportInsightFiles.put(uniqueKey, fileLocation);
	}

	/**
	 * Returns the absolute filesystem path for an export file registered under the
	 * given key.
	 *
	 * @param uniqueKey the registration key
	 * @return the absolute file path
	 * @throws IllegalArgumentException if the key is not found
	 */
	public String getExportFileLocation(String uniqueKey) {
		InsightFile insightFile = this.exportInsightFiles.get(uniqueKey);
		if (insightFile == null) {
			throw new IllegalArgumentException(
					"The unique key '" + uniqueKey + "' is an incorrect identifier for the file");
		}
		String fileLocation = insightFile.getFilePath();
		return getAbsoluteInsightFolderPath(fileLocation);
	}

	/**
	 * Returns all registered export files keyed by their registration key.
	 *
	 * @return map of key to InsightFile
	 */
	public Map<String, InsightFile> getExportInsightFiles() {
		return this.exportInsightFiles;
	}

	/**
	 * Adds a file to the list of files that were loaded into this insight and that
	 * should be bundled when the insight is saved.
	 *
	 * @param fileMeta the InsightFile descriptor to add
	 */
	public void addLoadInsightFile(InsightFile fileMeta) {
		this.loadInsightFiles.add(fileMeta);
	}

	/**
	 * Replaces the entire list of load-insight files.
	 *
	 * @param insightFiles the new list
	 */
	public void setLoadInsightFiles(List<InsightFile> insightFiles) {
		this.loadInsightFiles = insightFiles;
	}

	/**
	 * Returns the list of files that were loaded into this insight.
	 *
	 * @return list of InsightFile descriptors
	 */
	public List<InsightFile> getLoadInsightFiles() {
		return this.loadInsightFiles;
	}

	/**
	 * Returns whether temporary files created by this insight should be deleted
	 * when the insight is dropped.
	 *
	 * @return true if files should be deleted on drop
	 */
	public boolean isDeleteFilesOnDropInsight() {
		return this.deleteFilesOnDropInsight;
	}

	/**
	 * Sets whether temporary files should be deleted when the insight is dropped.
	 *
	 * @param deleteFilesOnDropInsight true to delete files on drop
	 */
	public void setDeleteFilesOnDropInsight(boolean deleteFilesOnDropInsight) {
		this.deleteFilesOnDropInsight = deleteFilesOnDropInsight;
	}

	////////////////////////////////////////////////////////////////
	// INSIGHT CONFIG & URLS
	////////////////////////////////////////////////////////////////

	/**
	 * Sets the pragma map controlling low-level execution options such as cache
	 * strategy, raw mode, and parquet output.
	 *
	 * @param pragmap the pragma map to apply
	 */
	public void setPragmap(Map<String, Object> pragmap) {
		this.pragmap = pragmap;
	}

	/**
	 * Returns the active pragma map.
	 *
	 * @return the pragma map
	 */
	public Map<String, Object> getPragmap() {
		return this.pragmap;
	}

	/**
	 * Clears all entries from the pragma map.
	 */
	public void clearPragmap() {
		this.pragmap.clear();
	}

	/**
	 * Sets the base URL used when constructing shareable insight links.
	 *
	 * @param baseURL the base URL (including trailing slash if needed)
	 */
	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}

	/**
	 * Returns the base URL used when constructing shareable links.
	 *
	 * @return the base URL
	 */
	public String getBaseURL() {
		return this.baseURL;
	}

	/**
	 * Builds and returns a live URL for this insight using its runtime insight ID.
	 *
	 * @return the live insight URL string
	 */
	public String getLiveURL() {
		StringBuilder retURL = new StringBuilder(this.baseURL);
		retURL.append("insight?insightId=").append(insightId);
		return retURL.toString();
	}

	/**
	 * Looks up a property by first checking the VarStore and then falling back to
	 * the DIHelper server properties.
	 *
	 * @param propName the property name
	 * @return the property value, or null if not found in either location
	 */
	public String getProperty(String propName) {
		Object retObject = this.varStore.get(propName);
		if (retObject != null) {
			return ((NounMetadata) retObject).getValue().toString();
		}

		return Utility.getDIHelperProperty(propName);
	}

	/**
	 * Sets the ornament map, which holds arbitrary key-value metadata for the
	 * insight (e.g. UI hints or workflow annotations).
	 *
	 * @param insightOrnament the ornament map to store
	 */
	public void setInsightOrnament(Map<String, Object> insightOrnament) {
		this.insightOrnament = insightOrnament;
	}

	/**
	 * Returns the ornament map for this insight.
	 *
	 * @return the insight ornament map
	 */
	public Map<String, Object> getInsightOrnament() {
		return this.insightOrnament;
	}

	////////////////////////////////////////////////////////////////
	// MODE FLAGS
	////////////////////////////////////////////////////////////////

	/**
	 * Enables or disables saved-insight re-run mode. When true, certain reactors
	 * behave differently to preserve deterministic replay behavior.
	 *
	 * @param isSavedInsightMode true to enable saved-insight mode
	 */
	public void setRunSavedInsightMode(boolean isSavedInsightMode) {
		this.isSavedInsightMode = isSavedInsightMode;
	}

	/**
	 * Returns whether this insight is currently executing in saved-insight re-run
	 * mode.
	 *
	 * @return true if in saved-insight mode
	 */
	public boolean isSavedInsightMode() {
		return this.isSavedInsightMode;
	}

	/**
	 * Returns whether this insight is executing as part of a scheduled job.
	 *
	 * @return true if in scheduler mode
	 */
	public boolean isSchedulerMode() {
		return isSchedulerMode;
	}

	/**
	 * Sets whether this insight is executing as part of a scheduled job.
	 *
	 * @param isSchedulerMode true to enable scheduler mode
	 */
	public void setSchedulerMode(boolean isSchedulerMode) {
		this.isSchedulerMode = isSchedulerMode;
	}

	/**
	 * Returns whether the R environment should be torn down when this insight is
	 * dropped.
	 *
	 * @return true if R env should be deleted on drop
	 */
	public boolean isDeleteREnvOnDropInsight() {
		return this.deleteREnvOnDropInsight;
	}

	/**
	 * Sets whether the R environment should be torn down when this insight is
	 * dropped.
	 *
	 * @param deleteREnvOnDropInsight true to delete the R env on drop
	 */
	public void setDeleteREnvOnDropInsight(boolean deleteREnvOnDropInsight) {
		this.deleteREnvOnDropInsight = deleteREnvOnDropInsight;
	}

	/**
	 * Returns whether Python globals should be cleared when this insight is
	 * dropped.
	 *
	 * @return true if Python globals should be deleted on drop
	 */
	public boolean isDeletePythonGlobalsOnDropInsight() {
		return this.deletePythonGlobalsOnDropInsight;
	}

	/**
	 * Sets whether Python globals should be cleared when this insight is dropped.
	 *
	 * @param deletePythonGlobalsOnDropInsight true to clear globals on drop
	 */
	public void setDeletePythonGlobalsOnDropInsight(boolean deletePythonGlobalsOnDropInsight) {
		this.deletePythonGlobalsOnDropInsight = deletePythonGlobalsOnDropInsight;
	}

	/**
	 * Marks this insight as serialized or not in the user's insight-serialization
	 * registry.
	 *
	 * @param serialized true if the insight has been serialized
	 */
	public void setSerialized(boolean serialized) {
		this.user.setInsightSerialization(insightId, serialized);
	}

	/**
	 * Returns whether this insight has been serialized, according to the user's
	 * insight-serialization registry.
	 *
	 * @return true if the insight is marked as serialized
	 */
	public boolean getSerialized() {
		return this.user.getInsightSerialization(insightId);
	}

	////////////////////////////////////////////////////////////////
	// MESSAGING
	////////////////////////////////////////////////////////////////

	/**
	 * Enqueues a message to be delivered to the frontend on the next flush.
	 * Messages are held in a bounded blocking queue; if the queue is full the call
	 * will block until space is available.
	 *
	 * @param noun the NounMetadata message to enqueue
	 */
	public void addDelayedMessage(NounMetadata noun) {
		this.delayedMessages.add(noun);
	}

	/**
	 * Drains and returns all pending delayed messages. The internal queue is empty
	 * after this call returns.
	 *
	 * @return list of pending NounMetadata messages
	 */
	public List<NounMetadata> getDelayedMessages() {
		List<NounMetadata> messages = new Vector<NounMetadata>();
		NounMetadata noun = null;
		while ((noun = delayedMessages.poll()) != null) {
			messages.add(noun);
		}
		return messages;
	}

	////////////////////////////////////////////////////////////////
	// SQL WRAPPERS
	////////////////////////////////////////////////////////////////

	/**
	 * Stores a SQL expression wrapper and returns the auto-generated ID that can be
	 * used to retrieve it later.
	 *
	 * @param sql     the SQL expression being wrapped
	 * @param wrapper the GenExpressionWrapper to store
	 * @return the generated ID for this wrapper
	 */
	public String setSQLWrapper(String sql, GenExpressionWrapper wrapper) {
		String id = "id" + idCount;
		idCount++;
		this.sqlWrapperMap.put(sql, wrapper);
		this.id2SQLMapper.put(id, sql);
		return id;
	}

	/**
	 * Retrieves the SQL expression wrapper associated with the given ID.
	 *
	 * @param id the wrapper ID returned by {@link #setSQLWrapper}
	 * @return the GenExpressionWrapper, or null if not found
	 */
	public GenExpressionWrapper getSQLWrapper(String id) {
		String sql = this.id2SQLMapper.get(id);
		return this.sqlWrapperMap.get(sql);
	}

	/**
	 * Removes the SQL expression wrapper associated with the given ID.
	 *
	 * @param id the wrapper ID to remove
	 */
	public void removeSQLWrapper(String id) {
		String sql = this.id2SQLMapper.get(id);
		id2SQLMapper.remove(id);
		this.sqlWrapperMap.remove(sql);
	}

	/**
	 * Replaces the SQL and wrapper associated with an existing ID. The old SQL
	 * entry is removed from the wrapper map and the new one is inserted.
	 *
	 * @param id      the existing wrapper ID
	 * @param sql     the replacement SQL expression
	 * @param wrapper the replacement GenExpressionWrapper
	 */
	public void replaceWrapper(String id, String sql, GenExpressionWrapper wrapper) {
		String origSql = this.id2SQLMapper.get(id);
		id2SQLMapper.put(id, sql);
		this.sqlWrapperMap.put(sql, wrapper);
		this.sqlWrapperMap.remove(origSql);
	}

	////////////////////////////////////////////////////////////////
	// LEGACY (OldInsight support)
	////////////////////////////////////////////////////////////////

	/**
	 * Marks this insight as using the pre-pixel (OldInsight / playsheet) format.
	 * This flag gates certain backwards-compatible code paths.
	 *
	 * @param isOldInsight true if this is an old-format insight
	 */
	public void setIsOldInsight(boolean isOldInsight) {
		this.isOldInsight = isOldInsight;
	}

	/**
	 * Returns whether this insight uses the pre-pixel (OldInsight) format.
	 *
	 * @return true if this is an old-format insight
	 */
	public boolean isOldInsight() {
		return this.isOldInsight;
	}

	/**
	 * Returns the data maker name for old-format insights. Always returns "H2Frame"
	 * when no frame is set.
	 *
	 * @return the data maker name
	 * @deprecated Only exists to support OldInsight (playsheet) execution paths.
	 */
	@Deprecated
	public String getDataMakerName() {
		NounMetadata curFrameNoun = this.varStore.get(CUR_FRAME_KEY);
		if (curFrameNoun != null) {
			return ((IDataMaker) curFrameNoun.getValue()).getDataMakerName();
		}
		return "H2Frame";
	}

	/**
	 * Hook for OldInsight (playsheet IDataMaker) subclasses to return web-rendered
	 * output. Always returns null in the base Insight class.
	 *
	 * @return null (overridden in OldInsight subclass)
	 */
	public Map<String, Object> getWebData() {
		return null;
	}
}
