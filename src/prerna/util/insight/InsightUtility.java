package prerna.util.insight;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

import net.snowflake.client.jdbc.internal.apache.commons.io.FilenameUtils;
import prerna.algorithm.api.ITableDataFrame;
import prerna.date.SemossDate;
import prerna.ds.nativeframe.NativeFrame;
import prerna.om.Insight;
import prerna.om.InsightFile;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.query.parsers.ParamStruct;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.insights.SetInsightConfigReactor;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.util.Constants;
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
	
	protected static final Logger logger = LogManager.getLogger(InsightUtility.class.getName());
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
			newInsight.setRJavaTranslator(origInsight.getRJavaTranslator(logger));
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
			logger.warn("SESSION ID is null");
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
			task.cleanUp();
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
			logger.info("Start clearning insight " + insight.getInsightId());

			// drop all the tasks that are currently running
			TaskStore taskStore = insight.getTaskStore();
			taskStore.clearAllTasks();
			logger.debug("Successfully cleared all stored tasks for the insight");
	
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
					logger.error(Constants.STACKTRACE, e);
				}
			}
			for(ITableDataFrame dm : allCreatedFrames) {
				if(!dm.isClosed()) {
					try {
						logger.info("There are untracked frames in this insight. Frame with name = " + dm.getName() );
						dm.close();
					} catch(Exception e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			logger.debug("Successfully removed all frames from insight");
			
			// clear insight
			insight.getVarStore().clear();
			logger.debug("Successfully removed all variables from varstore");
	
			// if you are a scheduler mode
			// we will not delete the files
			if( !insight.isSchedulerMode() ) {
				Map<String, InsightFile> fileExports = insight.getExportInsightFiles();
				if (fileExports != null && !fileExports.isEmpty()) {
					for (String fileKey : fileExports.keySet()) {
						InsightFile insightFile = fileExports.get(fileKey);
						if(insightFile.isDeleteOnInsightClose()) {
							try {
								File f = new File(insightFile.getFilePath());
								f.delete();
								logger.debug("Successfully deleted export file used in insight " + f.getName());
							} catch(Exception e) {
								logger.error(Constants.STACKTRACE, e);
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
					AbstractRJavaTranslator rJava = insight.getRJavaTranslator(logger);
					rJava.runR("rm(list=ls())");
				} catch(Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			
			logger.info("Successfully cleared insight " + insight.getInsightId());
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("suppress", noOpType);
			return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.CLEAR_INSIGHT);
		}
	}
	
	public static NounMetadata dropInsight(final Insight insight) {
		synchronized(insight) {
			logger.info("Droping insight " + insight.getInsightId());
	
			// i will first grab all the files used then delete them
			// only if this is not a saved insight + not a copied insight used for preview
			if(insight.isDeleteFilesOnDropInsight() && !insight.isSavedInsight()) {
				List<InsightFile> fileData = insight.getLoadInsightFiles();
				if (fileData != null && !fileData.isEmpty()) {
					for (int fileIdx = 0; fileIdx < fileData.size(); fileIdx++) {
						InsightFile file = fileData.get(fileIdx);
						File f = new File(file.getFilePath());
						f.delete();
						logger.debug("Successfully deleted File used in insight " + file.getFilePath());
					}
				}
			}
			
			// now i will clear
			logger.info("Clear insight for drop");
			clearInsight(insight, true);
	
			logger.debug("Removing from insight store");
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
					AbstractRJavaTranslator rJava = insight.getRJavaTranslator(logger);
					rJava.removeEnv();
				} catch(Exception e) {
					logger.error(Constants.STACKTRACE, e);
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
			
			logger.info("Successfully dropped insight " + insight.getInsightId());
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
			logger.error(Constants.STACKTRACE, e);
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
						logger.error(Constants.STACKTRACE, e);
						// ignore
					}
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
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
			e.printStackTrace();
			throw new IllegalArgumentException("Exeption occured generate the sheet state with error: " + e.getMessage());
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
			e.printStackTrace();
			throw new IllegalArgumentException("Exeption occured generate the panel state with error: " + e.getMessage());
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
			ITableDataFrame frame = (ITableDataFrame) noun.getValue();
			if(!retMap.containsKey(frame.getName())) {
				Map<String, Object> headers = frame.getFrameHeadersObject();
				retMap.put(frame.getName(), headers);
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
		File baseFolder = new File(baseDir);
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
	
}
