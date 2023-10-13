package prerna.util.insight;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

import net.snowflake.client.jdbc.internal.apache.commons.io.FilenameUtils;
import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.date.SemossDate;
import prerna.ds.nativeframe.NativeFrame;
import prerna.om.Insight;
import prerna.om.InsightFile;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.om.Variable;
import prerna.query.parsers.ParamStruct;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.reactor.CalcVarReactor;
import prerna.reactor.PixelPlanner;
import prerna.reactor.export.CollectPivotReactor;
import prerna.reactor.export.FormattingUtility;
import prerna.reactor.export.IFormatter;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.insights.SetInsightConfigReactor;
import prerna.reactor.job.JobReactor;
import prerna.reactor.task.AutoTaskOptionsHelper;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.gson.FrameCacheHelper;
import prerna.util.gson.InsightPanelAdapter;
import prerna.util.gson.InsightSheetAdapter;
import prerna.util.gson.NumberAdapter;
import prerna.util.gson.SemossDateAdapter;

public class InsightUtility {

	private static final Gson GSON =  new GsonBuilder()
			.disableHtmlEscaping()
			.registerTypeAdapter(Double.class, new NumberAdapter())
			.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
			.create();
	
	protected static final Logger classLogger = LogManager.getLogger(InsightUtility.class.getName());
	public static final String PANEL_VIEW_VISUALIZATION = "visualization";
	
	public static final String OUTPUT_TYPE = "output";
	public static final String MAP_OUTPUT = "map";
	public static final String STRING_OUTPUT = "string";
	
	private InsightUtility() {
		
	}
	
	/**
	 * Used to transfer important properties when creating an insight within an insight
	 * @param origInsight
	 * @param newInsight
	 */
	public static void transferDefaultVars(Insight origInsight, Insight newInsight) {
		if(origInsight == null) {
			return;
		}
		String[] keys = new String[]{JobReactor.JOB_KEY, JobReactor.SESSION_KEY, JobReactor.INSIGHT_KEY};
		for(String key : keys) {
			if(origInsight.getVarStore().containsKey(key)) {
				newInsight.getVarStore().put(key, origInsight.getVarStore().get(key));
			}
		}
		newInsight.setBaseURL(origInsight.getBaseURL());
		newInsight.setSchedulerMode(origInsight.isSchedulerMode());
		newInsight.setUser(origInsight.getUser());
		// r
		if(origInsight.rInstantiated()) {
			newInsight.setRJavaTranslator(origInsight.getRJavaTranslator(classLogger));
		}
		// py
		newInsight.setTupleSpace(origInsight.getTupleSpace());
	}
	
	/**
	 * User to transfer the insight id / saved components for identification of the insight
	 * @param origInsight
	 * @param newInsight
	 */
	public static void transferInsightIdentifiers(Insight origInsight, Insight newInsight) {
		newInsight.setInsightId(origInsight.getInsightId());
		newInsight.setProjectId(origInsight.getProjectId());
		newInsight.setProjectName(origInsight.getProjectName());
		newInsight.setRdbmsId(origInsight.getRdbmsId());
		newInsight.setInsightName(origInsight.getInsightName());
	}
	
	/**
	 * Useful method to determine if the frame is the same using the FrameCacheHelper class
	 * @param frames
	 * @param frame
	 * @return
	 */
	public static FrameCacheHelper findSameFrame(List<FrameCacheHelper> frames, ITableDataFrame frame) {
		int size = frames.size();
		for(int i = 0; i < size; i++) {
			if(frames.get(i).sameFrame(frame)) {
				return frames.get(i);
			}
		}
		
		return null;
	}
	
	/**
	 * Get a subset of the session Id to use in file locations to do file structure length restrictions
	 * @param sessionId
	 * @return
	 */
	public static String getFolderDirSessionId(String sessionId) {
		if(sessionId == null) {
			classLogger.warn("SESSION ID is null");
			return "null";
		}
		if(sessionId.length() > 32) {
			return sessionId.substring(0, 32);
		}
		return sessionId;
	}
	
	/**
	 * Set the insight panel to be a visualization
	 * @param insight
	 * @param panelId
	 */
	public static void setPanelForVisualization(Insight insight, String panelId) {
		InsightPanel insightPanel = insight.getInsightPanel(panelId);
		if(insightPanel != null) {
			if(!PANEL_VIEW_VISUALIZATION.equals(insightPanel.getPanelView())) {
				insightPanel.setPanelView(PANEL_VIEW_VISUALIZATION);
			}
		}
	}
	
	/**
	 * Get the parameters within the insight
	 * @param insight
	 * @return
	 */
	public static List<ParamStruct> getInsightParams(Insight insight) {
		List<ParamStruct> params = new Vector<>();
		VarStore varStore = insight.getVarStore();
		List<String> paramKeys = varStore.getInsightParameterKeys();
		for(String paramKey : paramKeys) {
			NounMetadata noun = varStore.get(paramKey);
			params.add((ParamStruct) noun.getValue());
		}
		
		return params;
	}
	
	/**
	 * 	
	 * @param planner
	 * @param key
	 * @return
	 */
	public static NounMetadata removeVaraible(PixelPlanner planner, String key) {
		InMemStore<String, NounMetadata> varStore = planner.getVarStore();
		return removeVaraible(varStore, key);
	}

	/**
	 * 
	 * @param varStore
	 * @param key
	 * @return
	 */
	public static NounMetadata removeVaraible(InMemStore<String, NounMetadata> varStore, String key) {
		NounMetadata noun = varStore.remove(key);
		if(noun == null) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.REMOVE_VARIABLE);
		}
		
		PixelDataType nType = noun.getNounType();
		if(nType == PixelDataType.FRAME) {
			ITableDataFrame dm = (ITableDataFrame) noun.getValue();
			dm.close();

			// find all other names pointing to this frame and remove it
			// this is because caching might load this frame
			List<String> keysToRemove = new ArrayList<String>();
			Iterator<String> curKeys = varStore.getKeys().iterator();
			while(curKeys.hasNext()) {
				String k = curKeys.next();
				// can use == since i am trying to see if same reference
				if(varStore.get(k).getValue() == dm) {
					keysToRemove.add(k);
				}
			}
			for(String k : keysToRemove) {
				varStore.remove(k);
			}
		} else if(nType == PixelDataType.TASK) {
			// get the task object
			ITask task = (ITask) noun.getValue();
			// close it
			try {
				task.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.REMOVE_VARIABLE);
	}
	
	/**
	 * Replaces all nouns with a specific value to point to a new noun
	 * @param varStore
	 * @param oldValue
	 * @param newNoun
	 */
	public static void replaceNounValue(InMemStore<String, NounMetadata> varStore, Object oldValue, NounMetadata newNoun) {
		List<String> keysToSub = new ArrayList<String>();
		Iterator<String> curKeys = varStore.getKeys().iterator();
		while(curKeys.hasNext()) {
			String k = curKeys.next();
			// can use == since i am trying to see if same reference
			if(varStore.get(k).getValue() == oldValue) {
				keysToSub.add(k);
			}
		}
		for(String k : keysToSub) {
			varStore.put(k, newNoun);
		}
	}
	
	/**
	 * Will remove the variable only if the key is pointing to a frame
	 * @param varStore
	 * @param key
	 * @return
	 */
	public static NounMetadata removeFrameVaraible(InMemStore<String, NounMetadata> varStore, String key) {
		// only remove if the frame variable is in fact a frame
		NounMetadata noun = varStore.get(key);
		if(noun == null) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.REMOVE_VARIABLE);
		}
		
		PixelDataType nType = noun.getNounType();
		if(nType == PixelDataType.FRAME) {
			removeVaraible(varStore, key);
			return new NounMetadata(key, PixelDataType.CONST_STRING, PixelOperationType.REMOVE_FRAME, PixelOperationType.REMOVE_VARIABLE);
		}
		
		return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.REMOVE_VARIABLE);
	}
	
	/**
	 * 
	 * @param insight
	 * @param taskId
	 * @return
	 */
	public static ITask removeTask(Insight insight, String taskId) {
		// get the task object
		ITask task = insight.getTaskStore().getTask(taskId);
		// remove the task id
		insight.getTaskStore().removeTask(taskId);
		// return the task object so we know what was removed
		return task;
	}
	
	/**
	 * Clear an insight
	 * @param insight
	 * @return
	 */
	public static NounMetadata clearInsight(final Insight insight, boolean noOpType) {
		synchronized(insight) {
			classLogger.info("Start clearning insight " + Utility.cleanLogString(insight.getInsightId()));

			// drop all the tasks that are currently running
			TaskStore taskStore = insight.getTaskStore();
			taskStore.clearAllTasks();
			classLogger.debug("Successfully cleared all stored tasks for the insight");
	
			Map<String, ITableDataFrame> filterCaches = insight.getCachedFilterModelFrame();
			for(String key : filterCaches.keySet()) {
				try {
					filterCaches.get(key).close();
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			
			// drop all the frame connections
			VarStore varStore = insight.getVarStore();
			List<String> keys = varStore.getFrameKeysCopy();
			Set<ITableDataFrame> allCreatedFrames = varStore.getAllCreatedFrames();
			
			// find all the vars which are frames
			// and drop them
			for(String key : keys) {
				NounMetadata noun = varStore.get(key);
				try {
					ITableDataFrame dm = (ITableDataFrame) noun.getValue();
					dm.close();
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			for(ITableDataFrame dm : allCreatedFrames) {
				if(!dm.isClosed()) {
					try {
						classLogger.info("There are untracked frames in this insight. Frame with name = " + Utility.cleanLogString(dm.getOriginalName()) );
						dm.close();
					} catch(Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			classLogger.debug("Successfully removed all frames from insight");
			
			// clear insight
			insight.getVarStore().clear();
			classLogger.debug("Successfully removed all variables from varstore");
	
			// if you are a scheduler mode
			// we will not delete the files
			if( !insight.isSchedulerMode() ) {
				Map<String, InsightFile> fileExports = insight.getExportInsightFiles();
				if (fileExports != null && !fileExports.isEmpty()) {
					for (String fileKey : fileExports.keySet()) {
						InsightFile insightFile = fileExports.get(fileKey);
						if(insightFile.isDeleteOnInsightClose()) {
							try {
								File f = new File(Utility.normalizePath(insightFile.getFilePath()));
								f.delete();
								classLogger.debug("Successfully deleted export file used in insight " + f.getName());
							} catch(Exception e) {
								classLogger.error(Constants.STACKTRACE, e);
							}
						}
					}
				}
			}
			
			// if R is instantiated
			// remove all the variables
			// this will happen in your environment
			if(insight.rInstantiated() && insight.isDeleteREnvOnDropInsight()) {
				try {
					AbstractRJavaTranslator rJava = insight.getRJavaTranslator(classLogger);
					rJava.runR("rm(list=ls())");
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			
			classLogger.info("Successfully cleared insight " + Utility.cleanLogString(insight.getInsightId()));
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("suppress", noOpType);
			return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.CLEAR_INSIGHT);
		}
	}
	
	public static NounMetadata dropInsight(final Insight insight) {
		synchronized(insight) {
			classLogger.info("Droping insight " + insight.getInsightId());
	
			// i will first grab all the files used then delete them
			// only if this is not a saved insight + not a copied insight used for preview
			if(insight.isDeleteFilesOnDropInsight() && !insight.isSavedInsight()) {
				List<InsightFile> fileData = insight.getLoadInsightFiles();
				if (fileData != null && !fileData.isEmpty()) {
					for (int fileIdx = 0; fileIdx < fileData.size(); fileIdx++) {
						InsightFile file = fileData.get(fileIdx);
						File f = new File(file.getFilePath());
						f.delete();
						classLogger.debug("Successfully deleted File used in insight " + file.getFilePath());
					}
				}
			}
			
			// now i will clear
			classLogger.info("Clear insight for drop");
			clearInsight(insight, true);
	
			classLogger.debug("Removing from insight store");
			String insightId = insight.getInsightId();
			InsightStore.getInstance().remove(insightId);
	
			String sessionId = ThreadStore.getSessionId();
			if(sessionId != null) {
				Set<String> insightIdsForSesh = InsightStore.getInstance().getInsightIDsForSession(sessionId);
				if(insightIdsForSesh != null) {
					insightIdsForSesh.remove(insightId);
				}
			}
			
			// if R is instantiated
			// remove the environment
			if(insight.rInstantiated() && insight.isDeleteREnvOnDropInsight()) {
				try {
					AbstractRJavaTranslator rJava = insight.getRJavaTranslator(classLogger);
					rJava.removeEnv();
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			// if Python is instantiated
			// remove the watcher
			if(insight.isDeletePythonTupleOnDropInsight()) {
				insight.dropPythonTupleSpace();
			}
			
//			NounMetadata sessionNoun = insight.getVarStore().get(JobReactor.SESSION_KEY);
//			if(sessionNoun != null) {
//				String sessionId = sessionNoun.getValue().toString();
//				Set<String> insightIdsForSesh = InsightStore.getInstance().getInsightIDsForSession(sessionId);
//				if(insightIdsForSesh != null) {
//					insightIdsForSesh.remove(insightId);
//				}
//			}
			
			classLogger.info("Successfully dropped insight " + insight.getInsightId());
			// also remove from the user object as an open insight
			if(insight.isSavedInsight() && insight.getUser() != null) {
				insight.getUser().removeOpenInsight(insight.getProjectId(), insight.getRdbmsId(), insightId);
			}
			return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DROP_INSIGHT);
		}
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	// Insight State Methods
	
	/**
	 * Give the FE all the data needed to get to the same UI for the existing insight
	 * @param in
	 */
	public static PixelRunner recreateInsightState(Insight in) {
		List<String> recipe = PixelUtility.getCachedInsightRecipe(in);
		
		Insight rerunInsight = new Insight();
		rerunInsight.setVarStore(in.getVarStore());
		rerunInsight.setUser(in.getUser());
		InsightUtility.transferDefaultVars(in, rerunInsight);
		InsightUtility.transferInsightIdentifiers(in, rerunInsight);
		
		// set in thread
		ThreadStore.setInsightId(in.getInsightId());
		ThreadStore.setSessionId(in.getVarStore().get(JobReactor.SESSION_KEY).getValue() + "");
		ThreadStore.setUser(in.getUser());
		
		try {
			// add a copy of all the insight sheets
			Map<String, InsightSheet> sheets = in.getInsightSheets();
			for(String sheetId : sheets.keySet()) {
				InsightSheetAdapter adapter = new InsightSheetAdapter();
				StringWriter writer = new StringWriter();
				JsonWriter jWriter = new JsonWriter(writer);
				adapter.write(jWriter, sheets.get(sheetId));
				String sheetStr = writer.toString();
				InsightSheet sheetClone = adapter.fromJson(sheetStr);
				rerunInsight.addNewInsightSheet(sheetClone);
			}
			
			// add a copy of all the insight panels
			Map<String, InsightPanel> panels = in.getInsightPanels();
			for(String panelId : panels.keySet()) {
				InsightPanelAdapter adapter = new InsightPanelAdapter();
				StringWriter writer = new StringWriter();
				JsonWriter jWriter = new JsonWriter(writer);
				adapter.write(jWriter, panels.get(panelId));
				String panelStr = writer.toString();
				InsightPanel panelClone = adapter.fromJson(panelStr);
				rerunInsight.addNewInsightPanel(panelClone);
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		PixelRunner pixelRunner = new PixelRunner();
		// add all the frame headers to the payload first
		try {
			VarStore vStore = in.getVarStore();
			List<String> keys = vStore.getFrameKeysCopy();
			for(String k : keys) {
				NounMetadata noun = vStore.get(k);
				PixelDataType type = noun.getNounType();
				if(type == PixelDataType.FRAME) {
					try {
						ITableDataFrame frame = (ITableDataFrame) noun.getValue();
						pixelRunner.addResult("CACHED_FRAME_HEADERS", 
								new NounMetadata(frame.getFrameHeadersObject(), PixelDataType.CUSTOM_DATA_STRUCTURE, 
										PixelOperationType.FRAME_HEADERS), true);
					} catch(Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
						// ignore
					}
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		// now rerun the recipe and append to the runner
		pixelRunner = rerunInsight.runPixel(pixelRunner, recipe);
		NounMetadata insightConfig = in.getVarStore().get(SetInsightConfigReactor.INSIGHT_CONFIG);
		if(insightConfig != null) {
			pixelRunner.addResult("META | GetInsightConfig()", insightConfig, true);
		}
		return pixelRunner;
	}
	
	/**
	 * Get the recipe to generate the end state of the FE UI
	 * @param insight
	 * @param outputType
	 * @return
	 */
	public static List<String> getInsightUIStateSteps(Insight insight, String outputType) {
		outputType = outputType.toLowerCase();
		List<String> pixelSteps = new Vector<String>();

		// we will be doing this for every sheet and every panel
		for(String sheetId : insight.getInsightSheets().keySet()) {
			pixelSteps.add("AddSheet(" + sheetId + ");");
		}
		for(String sheetId : insight.getInsightSheets().keySet()) {
			// we will just serialize the insight sheet
			InsightSheet sheet = insight.getInsightSheet(sheetId);
			NounMetadata noun = getSheetState(sheet, outputType);
			
			// turn the serialization into a Map object
			Object serialization = noun.getValue();
			if(MAP_OUTPUT.equals(outputType)) {
				pixelSteps.add("SetSheetState(" + GSON.toJson(serialization) + ");");
			} else {
				pixelSteps.add("SetSheetState(\"" + serialization + "\");");
			}
		}
		
		// repeat for each panel
		for(String panelId : insight.getInsightPanels().keySet()) {
			pixelSteps.add("AddPanel(" + panelId + ");");
		}
		for(String panelId : insight.getInsightPanels().keySet()) {
			InsightPanel panel = insight.getInsightPanel(panelId);
			NounMetadata noun = getPanelState(panel, outputType);

			// turn the serialization into a Map object
			Object serialization = noun.getValue();
			if(MAP_OUTPUT.equals(outputType)) {
				pixelSteps.add("SetPanelState(" + GSON.toJson(serialization) + ");");
			} else {
				pixelSteps.add("SetPanelState(\"" + serialization + "\");");
			}
		}
		
		return pixelSteps;
	}
	
	/**
	 * Get the sheet state as a JSON or String
	 * @param sheet
	 * @param outputType
	 * @return
	 */
	public static NounMetadata getSheetState(InsightSheet sheet, String outputType) {
		// we will just serialize the insight sheet
		InsightSheetAdapter adapter = new InsightSheetAdapter();
		String serialization = null;
		try {
			serialization = adapter.toJson(sheet);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Exeption occurred generate the sheet state with error: " + e.getMessage());
		}
		
		// turn the serialization into a Map object
		if(MAP_OUTPUT.equals(outputType.toLowerCase())) {
			HashMap<String, Object> json = GSON.fromJson(serialization, HashMap.class);
			return new NounMetadata(json, PixelDataType.MAP);
		}
		return new NounMetadata(serialization, PixelDataType.CONST_STRING);
	}
	
	/**
	 * Get the map state as a JSON or String
	 * @param panel
	 * @param outputType
	 * @return
	 */
	public static NounMetadata getPanelState(InsightPanel panel, String outputType) {
		// we will just serialize the insight panel
		InsightPanelAdapter adapter = new InsightPanelAdapter();
		String serialization = null;
		try {
			serialization = adapter.toJson(panel);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Exeption occurred generate the panel state with error: " + e.getMessage());
		}
		
		// turn the serialization into a Map object
		if(MAP_OUTPUT.equals(outputType)) {
			HashMap<String, Object> json = GSON.fromJson(serialization, HashMap.class);
			return new NounMetadata(json, PixelDataType.MAP);
		}
		return new NounMetadata(serialization, PixelDataType.CONST_STRING);
	}
	
	/**
	 * Get all the frames to their current set of headers
	 * @param varStore
	 * @return
	 */
	public static Map<String, Map<String, Object>> getAllFrameHeaders(VarStore varStore) {
		Map<String, Map<String, Object>> retMap = new HashMap<>();
		Iterator<String> frameKeys = varStore.getFrameKeysCopy().iterator();
		while(frameKeys.hasNext()) {
			String frameName = frameKeys.next();
			NounMetadata noun = varStore.get(frameName);
			if(noun != null && noun.getValue() instanceof ITableDataFrame) {
				ITableDataFrame frame = (ITableDataFrame) noun.getValue();
				if(!retMap.containsKey(frame.getOriginalName())) {
					Map<String, Object> headers = frame.getFrameHeadersObject();
					retMap.put(frame.getOriginalName(), headers);
				}
			} else {
				classLogger.info("You are grabbing frame headers but the noun doesn't refer to a frame... very vey weird....");
			}
		}

		return retMap;
	}
	
	/**
	 * Generate a task based on the QueryStruct
	 * @param qs
	 * @param insight
	 * @return
	 */
	public static BasicIteratorTask constructTaskFromQs(Insight insight, SelectQueryStruct qs) {
		fillQsReferencesAndMergeOptions(insight, qs);

		BasicIteratorTask task = new BasicIteratorTask(qs);
		// add the task to the store
		insight.getTaskStore().addTask(task);
		return task;
	}
	
	/**
	 * Fill and move over the QS state stored in the insight via frame/panels to the QS
	 * @param qs
	 * @param insight
	 * @return
	 */
	public static void fillQsReferencesAndMergeOptions(Insight insight, SelectQueryStruct qs) {
		// handle some defaults
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// first, do a basic check
		if(qsType != QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY && qsType != QUERY_STRUCT_TYPE.RAW_FRAME_QUERY
				&& qsType != QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY) {
			// it is not a hard query
			// we need to make sure there is at least a selector
			if(qs.getSelectors().isEmpty()) {
				throw new IllegalArgumentException("There are no selectors in the query to return.  "
						+ "There must be at least one selector for the query to execute.");
			}
		}

		// fill in the references in the QS
		fillQueryStructReferences(insight, qs);
		// just need to set some default behavior based on the pixel generation
		if(qsType == QUERY_STRUCT_TYPE.FRAME || qsType == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			ITableDataFrame frame = qs.getFrame();
			// if we are not overriding implicit filters - add them
			if(!qs.isOverrideImplicit()) {
				qs.setFrameImplicitFilters(frame.getFrameFilters());
			}

			// if the frame is native and there are other
			// things to blend - we need to do that
			if(frame instanceof NativeFrame) {
				qs.setBigDataEngine( ((NativeFrame) frame).getQueryStruct().getBigDataEngine());
			}
		}

		// set the pragmap before I can build the task
		// the idea is this needs to be passed into querystruct and later iterator
		// unless we start keeping a reference of querystruct in the iterator
		// adds it to the qs
		if(qs.getPragmap() != null && insight.getPragmap() != null) {
			qs.getPragmap().putAll(insight.getPragmap());
		} else if(insight.getPragmap() != null) {
			qs.setPragmap(insight.getPragmap());
		}
	}
	
	/**
	 * Sets the reference for frames and panels in the insight if not present
	 * @param insight
	 * @param qs
	 */
	public static void fillQueryStructReferences(Insight insight, AbstractQueryStruct qs) {
		// set the frame reference based on the insight state
		ITableDataFrame frame = qs.getFrame();
		if(frame == null) {
			// see if the frame name exists
			if(qs.getFrameName() != null) {
				frame = (ITableDataFrame) insight.getVar(qs.getFrameName());
			}
			// default to base frame
			if(frame == null) {
				frame = (ITableDataFrame) insight.getDataMaker();
			}
			qs.setFrame(frame);
		}
		
		// set the panel state
		List<String> pIds = qs.getPanelIdList();
		if(pIds != null && !pIds.isEmpty()) {
			List<InsightPanel> panels = qs.getPanelList();
			if(panels == null) {
				panels = new Vector<>(pIds.size());
			}
			if(pIds.size() != panels.size()) {
				boolean insightPanelsExist = true;
				for(String pId : pIds) {
					// this might not exist
					// when the insight
					InsightPanel panel = insight.getInsightPanel(pId);
					if(panel == null) {
						insightPanelsExist = false;
						break;
					}
					panels.add(insight.getInsightPanel(pId));
				}
				if(insightPanelsExist) {
					qs.setPanelList(panels);
				}
			}
		}
	}
	
	/**
	 * Find an image in the directory
	 * 
	 * @param baseDir
	 * @return
	 */
	public static File[] findImageFile(String baseDir) {
		FileFilter imageExtensionFilter = new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				String filePath = pathname.getAbsolutePath();
				if(FilenameUtils.getBaseName(filePath).equals("image")) {
					String ext = FilenameUtils.getExtension(filePath);
					if(ext.equalsIgnoreCase("png") 
						|| ext.equalsIgnoreCase("jpeg")
						|| ext.equalsIgnoreCase("jpg")
						|| ext.equalsIgnoreCase("gif")
						|| ext.equalsIgnoreCase("svg") ) {
						return true;
					}
				}
				
				return false;
			}
		};
		File baseFolder = new File(Utility.normalizePath(baseDir));
		return baseFolder.listFiles(imageExtensionFilter);
	}
	
	/**
	 * Find an image in the directory with a given name
	 * 
	 * @param baseDir
	 * @param baseName
	 * @return
	 */
	public static File[] findImageFile(String baseDir, String baseName) {
		FileFilter imageExtensionFilter = new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				String filePath = pathname.getAbsolutePath();
				if(FilenameUtils.getBaseName(filePath).equals(baseName)) {
					String ext = FilenameUtils.getExtension(filePath);
					if(ext.equalsIgnoreCase("png") 
						|| ext.equalsIgnoreCase("jpeg")
						|| ext.equalsIgnoreCase("jpg")
						|| ext.equalsIgnoreCase("gif")
						|| ext.equalsIgnoreCase("svg") ) {
						return true;
					}
				}
				
				return false;
			}
		};
		File baseFolder = new File(baseDir);
		return baseFolder.listFiles(imageExtensionFilter);
	}
	
	/**
	 * Find an image in the directory
	 * 
	 * @param baseDir
	 * @return
	 */
	public static File[] findImageFile(File baseFolder) {
		FileFilter imageExtensionFilter = new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				String filePath = pathname.getAbsolutePath();
				if(FilenameUtils.getBaseName(filePath).equals("image")) {
					String ext = FilenameUtils.getExtension(filePath);
					if(ext.equalsIgnoreCase("png") 
						|| ext.equalsIgnoreCase("jpeg")
						|| ext.equalsIgnoreCase("jpg")
						|| ext.equalsIgnoreCase("gif")
						|| ext.equalsIgnoreCase("svg") ) {
						return true;
					}
				}
				
				return false;
			}
		};
		return baseFolder.listFiles(imageExtensionFilter);
	}
	
	/**
	 * All insight panels that have a temp filter model referencing a frame should be reset
	 * @param insight
	 * @param frame
	 */
	public static void clearPanelTempFilterModel(Insight insight, ITableDataFrame frame) {
		Map<String, InsightPanel> insightPanels = insight.getInsightPanels();
		for(String key : insightPanels.keySet()) {
			InsightPanel panel = insightPanels.get(key);
			if(panel.getTempFitlerModelFrame() == frame) {
				panel.getTempFilterModelGrf().clear();
			}
		}
	}
	
	/**
	 * Calculate and return the dynamic var values
	 * @param insight
	 * @param dynamicVarNames
	 * @return
	 */
	public static Map<String, Object> calculateDynamicVars(Insight insight, List<Object> dynamicVarNames) {
		Map<String, Object> varValue = new HashMap<>();

		StringBuffer pyDeleter = new StringBuffer("del(");
		StringBuffer rDeleter = new StringBuffer("rm(");
		
		Map <String, String> oldNew = new HashMap<>();
		
		// get all the frames processed first
		for(int varIndex =0; varIndex < dynamicVarNames.size();varIndex++) {
			Object val = dynamicVarNames.get(varIndex);
			String name = null;
			Variable var = null;
			if(val instanceof String) {
				name = (String) val;
				var = insight.getVariable(name);
			} else if(val instanceof Variable) {
				var = (Variable) val;
				name = ((Variable) val).getName();
			} else {
				throw new IllegalArgumentException("Input " + val + " is not a valid variable");
			}
			// get the variable
			// get the frames
			// get the language
			// compare to see if the frame and language are the same 
			// TODO else we need to move it like how the pivot is
			// get the frame from insight for frame name
			// do a query all and create a query struct
			// make the call to the frame to create a secondary variable
			// string replace old frame with new
			// run the calculation
			// give the response
			//List <String> frames = var.getFrames();
			
			// get the variable
			// only for testing
			/*
			Variable var = new Variable();
			var.setExpression("'Sum of ages is now set to.. {}'.format(frame_d['age'].astype(int).sum())");
			var.setName("age_sum");
			var.addFrame("frame_d");
			var.setLanguage(Variable.LANGUAGE.PYTHON);
			*/
			
			//this.insight.getVariable(name);
						
			// get the frames
			List <String> frameNames = var.getFrames();
			// get the language
			// compare to see if the frame and language are the same 
			// TODO else we need to move it like how the pivot is
			// get the frame from insight for frame name
			
			for(int frameIndex = 0;frameIndex < frameNames.size();frameIndex++)
			{
				String thisFrameName = frameNames.get(frameIndex);
				ITableDataFrame frame = insight.getFrame(thisFrameName);
				
				if(!oldNew.containsKey(thisFrameName)) // if not already processed. Generate one
				{
					// query the frame
					// make the call to the frame to create a secondary variable
					try {
						// forcing it to be pandas frame for now
						String newName = frame.createVarFrame();
						oldNew.put(thisFrameName, newName);	
						if(frame.getFrameType() == DataFrameTypeEnum.PYTHON) {
							pyDeleter.append(newName).append(", ");
						} else if(frame.getFrameType() == DataFrameTypeEnum.R) {
							rDeleter.append(newName).append(", ");
						}
					} catch (Exception e)  {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// replace the frames and execute
		for(int varIndex = 0; varIndex < dynamicVarNames.size(); varIndex++) {
			Object val = dynamicVarNames.get(varIndex);
			String name = null;
			Variable var = null;
			if(val instanceof String) {
				name = (String) val;
				var = insight.getVariable(name);
			} else if(val instanceof Variable) {
				var = (Variable) val;
				name = ((Variable) val).getName();
			} else {
				throw new IllegalArgumentException("Input " + val + " is not a valid variable");
			}
			
			List <String> frameNames = var.getFrames();
			String expression = var.getExpression();
			// now that the frames are created
			// replace and run
			// string replace old frame with new
			for(int frameIndex = 0;frameIndex < frameNames.size();frameIndex++) {
				String thisFrameName = frameNames.get(frameIndex);
				String newName = oldNew.get(thisFrameName);				
				expression = expression.replace(thisFrameName, newName);
			}
			
			// calc the var			
			// run the calculation
			// give the response
			if(var.getLanguage() == Variable.LANGUAGE.PYTHON) {
				Object value = insight.getPyTranslator().runScript(expression);
				varValue.put(var.getName(), value);
			}
			if(var.getLanguage() == Variable.LANGUAGE.R) {
				Object value = insight.getRJavaTranslator(CalcVarReactor.class.getName()).runRAndReturnOutput(expression);
				varValue.put(var.getName(), value);
			}
		}	
		
		// need something to delete all the interim frame variables
		if(pyDeleter.indexOf(",") > 0) {// atleast one is filled up
			insight.getPyTranslator().runEmptyPy(pyDeleter.substring(0, pyDeleter.length() - 2) + ")");
		}
		
		if(rDeleter.indexOf(",") > 0) { // atleast one is filled up
			insight.getRJavaTranslator(CalcVarReactor.class.getName()).executeEmptyR(rDeleter.substring(0, pyDeleter.length() - 2) + ")");
		}
		
		return varValue;
	}
	
	/**
	 * Recalculate the var html view
	 * @param insight
	 * @param panel
	 * @return
	 */
	public static String recalculateHtmlViews(Insight insight, InsightPanel panel) {
		String html = panel.getPanelActiveViewOptions();

		List<String> allDynamicVars = insight.getVarStore().getDynamicVarKeys();
		if(allDynamicVars.isEmpty()) {
			panel.setRenderedViewOptions(html, new ArrayList<>());
			return html;
		}
		
		List<Object> dynamicVarNames = new ArrayList<>();

		String regex = "\\{\\{([A-Z|a-z])\\w+\\}\\}";
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);    
		Matcher matcher = pattern.matcher(html);
		while(matcher.find()) {
			String varName = matcher.group();
			String cleanedVarName = varName.substring(2, varName.length() - 2);
			// make sure the var exists
			if(allDynamicVars.contains(cleanedVarName)) {
				dynamicVarNames.add(cleanedVarName);
			}
		}
		
		if(!dynamicVarNames.isEmpty()) {
			// this calculates for all vars if no names are passed
			// so need the isEmpty check above
			Map<String, Object> dynamicVarValues = InsightUtility.calculateDynamicVars(insight, dynamicVarNames);
			for(String dynamicVarName : dynamicVarValues.keySet()) {
				Object dynamicValue = dynamicVarValues.get(dynamicVarName);
				if(dynamicValue == null) {
					continue;
				}
				dynamicValue = dynamicValue.toString().substring(4);
				Variable variable = (Variable) insight.getVarStore().get(dynamicVarName).getValue();
				if(variable.getFormat() != null) {
					// Checking for Formats
					Object formattedData = FormattingUtility.formatDataValues(dynamicValue, "DOUBLE", variable.getFormat().toString(), null);
					html = html.replace("{{" + dynamicVarName + "}}", formattedData.toString());
				} else {
					html = html.replace("{{" + dynamicVarName + "}}", dynamicValue + "");
				}
			}
		}
		
		panel.setRenderedViewOptions(html, dynamicVarNames);
		return html;
	}
	
	/**
	 * Add panel refresh for panel filtering
	 * @param insight
	 * @param frame
	 * @param filterNoun
	 * @param logger
	 */
	public static void addInsightPanelRefreshFromPanelFilter(Insight insight, InsightPanel panel, NounMetadata filterNoun, Logger logger) {
		List<NounMetadata> taskOutput = new ArrayList<>();
		List<NounMetadata> additionalMessages = new ArrayList<>();
		InsightUtility.refreshPanelTasks(insight, panel, panel.getNumCollect(), taskOutput, additionalMessages, logger);
		filterNoun.addAllAdditionalReturn(taskOutput);
		filterNoun.addAllAdditionalReturn(additionalMessages);
		InsightUtility.refreshViewFromPanelFilter(insight, panel, filterNoun, logger);
	}
	
	/**
	 * Add panel refresh for frame filtering
	 * @param insight
	 * @param frame
	 * @param filterNoun
	 * @param logger
	 */
	public static void addInsightPanelRefreshFromFrameFilter(Insight insight, ITableDataFrame frame, NounMetadata filterNoun, Logger logger) {
		List<NounMetadata> taskOutput = new ArrayList<>();
		List<NounMetadata> additionalMessages = new ArrayList<>();
		InsightUtility.refreshInsightPanelTasksFromFrameFilter(insight, frame, taskOutput, additionalMessages, logger);
		filterNoun.addAllAdditionalReturn(taskOutput);
		filterNoun.addAllAdditionalReturn(additionalMessages);
		InsightUtility.refreshViewFromFrameFilter(insight, frame, filterNoun, logger);
	}
	
	/**
	 * Get the updated tasks as a result of a frame filter
	 * @param insight
	 * @param frame
	 * @param taskOutput
	 * @param additionalMessages
	 * @param logger
	 */
	public static void refreshInsightPanelTasksFromFrameFilter(Insight insight, ITableDataFrame frame, List<NounMetadata> taskOutput, List<NounMetadata> additionalMessages, Logger logger) {
		List<InsightPanel> affectedPanels = getInsightPanelsUsingFrame(insight, frame);
		for(InsightPanel panel : affectedPanels) {
			InsightUtility.refreshPanelTasks(insight, panel, panel.getNumCollect(), taskOutput, additionalMessages, logger);
		}
	}
	
	/**
	 * Get the insight panels that are using a specific frame for the view
	 * @param insight
	 * @param frame
	 * @return
	 */
	public static List<InsightPanel> getInsightPanelsUsingFrame(Insight insight, ITableDataFrame frame) {
		List<InsightPanel> affectedPanels = new ArrayList<>();
		
		Map<String, InsightPanel> insightPanelsMap = insight.getInsightPanels();
		for(String panelId : insightPanelsMap.keySet()) {
			InsightPanel panel = insightPanelsMap.get(panelId);
			if(!panel.getPanelView().equalsIgnoreCase("visualization")) {
				continue;
			}
			
			Map<String, SelectQueryStruct> allQsOnPanel = panel.getLayerQueryStruct();
			LAYER_LOOP : for(String layerId : allQsOnPanel.keySet()) {
				SelectQueryStruct qs = allQsOnPanel.get(layerId);
				QUERY_STRUCT_TYPE qsType = qs.getQsType();
				if(qsType == QUERY_STRUCT_TYPE.FRAME || qsType == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
					if(qs.getFrame() == frame) {
						affectedPanels.add(panel);
						break LAYER_LOOP;
					}
				}
			}
		}
		
		return affectedPanels;
	}
	
	/**
	 * Refresh the tasks running on a panel
	 * @param insight
	 * @param panel
	 * @param panelCollect
	 * @param taskOutput
	 * @param additionalMessages
	 * @param logger
	 */
	public static void refreshPanelTasks(Insight insight, InsightPanel panel, int panelCollect, List<NounMetadata> taskOutput, List<NounMetadata> additionalMessages, Logger logger) {
		String panelId = panel.getPanelId();
		Map<String, SelectQueryStruct> lQs = panel.getLayerQueryStruct();
		Map<String, TaskOptions> lTaskOption = panel.getLayerTaskOption();
		Map<String, IFormatter> lFormatter = panel.getLayerFormatter();
		
		if(lQs != null && lTaskOption != null) {
			Set<String> layers = lQs.keySet();
			LAYER_LOOP : for(String layerId : layers) {
				SelectQueryStruct qs = lQs.get(layerId);
				// reset the panel specific objects so we can pick up the latest state
				qs.setPanelList(new ArrayList<InsightPanel>());
				qs.setPanelIdList(new ArrayList<String>());
				qs.setPanelOrderBy(new ArrayList<IQuerySort>());
				// add the panel
				qs.addPanel(panel);
				qs.resetPanelState();
				TaskOptions taskOptions = lTaskOption.get(layerId);
				IFormatter formatter = lFormatter.get(layerId);
				
				if(qs != null && taskOptions != null) {
					logger.info("Found task for panel = " + Utility.cleanLogString(panelId));
					// this will ensure we are using the latest panel and frame filters on refresh
					BasicIteratorTask task = InsightUtility.constructTaskFromQs(insight, qs);
					task.setFormat(formatter);
					try {
						task.setLogger(logger);
						task.toOptimize(true);
						task.setTaskOptions(taskOptions);
						task.setNumCollect(panelCollect);
						task.optimizeQuery(panelCollect);
					} catch(Exception e) {
						logger.info("Previous query on panel " + panelId + " does not work");
						InsightUtility.classLogger.error(Constants.STACKTRACE, e);
						// see if the frame at least exists
						ITableDataFrame queryFrame = qs.getFrame();
						if(queryFrame == null || queryFrame.isClosed()) {
							additionalMessages.add(NounMetadata.getErrorNounMessage("Attempting to refresh panel id " + panelId 
									+ " but the frame creating the visualization no longer exists"));
							continue LAYER_LOOP;
						}
						
						NounMetadata warning = NounMetadata.getWarningNounMessage("Attempting to refresh panel id " + panelId 
								+ " but the underlying data creating the visualization no longer exists "
								+ "or is now incompatible with the view. Displaying a grid of the data.");
						
						SelectQueryStruct allQs = queryFrame.getMetaData().getFlatTableQs(true);
						allQs.setFrame(queryFrame);
						allQs.setQsType(QUERY_STRUCT_TYPE.FRAME);
						allQs.setQueryAll(true);
						task = new BasicIteratorTask(allQs);
						taskOptions = new TaskOptions(AutoTaskOptionsHelper.generateGridTaskOptions(allQs, panelId));
						try {
							task.setLogger(logger);
							task.toOptimize(true);
							task.setTaskOptions(taskOptions);
							task.setNumCollect(panelCollect);
							task.optimizeQuery(panelCollect);
							additionalMessages.add(warning);
						} catch (Exception e1) {
							// at this point - no luck :/
							InsightUtility.classLogger.error(Constants.STACKTRACE, e);
							additionalMessages.add(NounMetadata.getErrorNounMessage("Attemptingt to refresh panel id " + panelId 
										+ " but the underlying data creating the visualization no longer exists "
										+ " or is now incompatible with the view. Displaying a grid of the data "
										+ " errors with the following message: " + e.getMessage()));
							continue LAYER_LOOP;
						}
					}
					
					// is this a pivot?
					Set<String> taskPanelIds = taskOptions.getPanelIds();
					String layout = taskOptions.getLayout(taskPanelIds.iterator().next());
					if(layout.equals("PivotTable")) {
						CollectPivotReactor pivot = new CollectPivotReactor();
						pivot.In();
						pivot.setInsight(insight);
						pivot.setNounStore(taskOptions.getCollectStore());
						GenRowStruct grs = taskOptions.getCollectStore().makeNoun(PixelDataType.TASK.getKey());
						grs.clear();
						grs.add(new NounMetadata(task, PixelDataType.TASK));
						taskOutput.add(pivot.execute());
					} else {
						taskOutput.add(new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
					}
				}
			}
		}
	}
	
	/**
	 * Add panel view refresh for panel filtering
	 * @param insight
	 * @param frame
	 * @param filterNoun
	 * @param logger
	 */
	public static void refreshViewFromPanelFilter(Insight insight, InsightPanel panel, NounMetadata filterNoun, Logger logger) {
		if(!panel.getPanelView().equalsIgnoreCase("text-editor")
				|| panel.getDynamicVars().isEmpty()) {
			return;
		}
		
		Map<String, String> returnMap = new HashMap<String, String>();
		returnMap.put("panelId", panel.getPanelId());
		returnMap.put("view", panel.getPanelView());
		// grab the options for this view
		returnMap.put("options", panel.getPanelActiveViewOptions());
		String renderedViewOptions = InsightUtility.recalculateHtmlViews(insight, panel);
		returnMap.put("renderedOptions", renderedViewOptions);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_VIEW);
		filterNoun.addAdditionalReturn(noun);
	}
	
	/**
	 * Add panel view refresh for frame filtering
	 * @param insight
	 * @param frame
	 * @param filterNoun
	 * @param logger
	 */
	public static void refreshViewFromFrameFilter(Insight insight, ITableDataFrame frame, NounMetadata filterNoun, Logger logger) {
		List<String> affectedVars = InsightUtility.getDynamicVarsUsingFrame(insight, frame);
		
		Map<String, InsightPanel> insightPanelsMap = insight.getInsightPanels();
		for(String panelId : insightPanelsMap.keySet()) {
			InsightPanel panel = insightPanelsMap.get(panelId);
			if(!panel.getPanelView().equalsIgnoreCase("text-editor")
					|| panel.getDynamicVars().isEmpty()) {
				continue;
			}
			
			if(!Collections.disjoint(panel.getDynamicVars(), affectedVars)) {
				InsightUtility.refreshViewFromPanelFilter(insight, panel, filterNoun, logger);
			}
		}
	}
	
	/**
	 * Get the variable names that are utilizing a specific frame
	 * @param insight
	 * @param frame
	 * @return
	 */
	public static List<String> getDynamicVarsUsingFrame(Insight insight, ITableDataFrame frame) {
		List<String> affectedVars = new ArrayList<>();
		
		List<String> dynamicVarKeys = insight.getVarStore().getDynamicVarKeys();
		for(String dynamicVarKey : dynamicVarKeys) {
			Variable variable = insight.getVariable(dynamicVarKey);
			if(variable.getFrames().contains(frame.getName())) {
				affectedVars.add(dynamicVarKey);
			}
		}
		
		return affectedVars;
	}
	
	/**
	 * Get the QS for the frame with frame filters applied.  If panel is not null then add in the panel filters as well.
	 * @param frame
	 * @param panel
	 * @return
	 */
	public static SelectQueryStruct getFilteredQsForFrame(ITableDataFrame frame, InsightPanel panel) {
		SelectQueryStruct allDataQs = frame.getMetaData().getFlatTableQs(true);
		// if panel is passed - add in those filters
		if(panel != null) {
			allDataQs.addExplicitFilter(panel.getPanelFilters(), true);
		}
		// always add in frame filters
		allDataQs.addExplicitFilter(frame.getFrameFilters(), true);
		return allDataQs;
	}
	
}
