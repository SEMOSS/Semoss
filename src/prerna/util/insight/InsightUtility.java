package prerna.util.insight;

import java.io.File;
import java.io.IOException;
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

import prerna.algorithm.api.ITableDataFrame;
import prerna.date.SemossDate;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.FileMeta;
import prerna.sablecc2.reactor.job.JobReactor;
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
		newInsight.setTupleSpace(origInsight.getTupleSpace());
		newInsight.setUser(origInsight.getUser());
		if(origInsight.rInstantiated()) {
			newInsight.setRJavaTranslator(origInsight.getRJavaTranslator(logger));
		}
		newInsight.setTupleSpace(origInsight.getTupleSpace());
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
	public static NounMetadata clearInsight(Insight insight, boolean noOpType) {
		synchronized(insight) {
			logger.info("Start clearning insight " + insight.getInsightId());

			// drop all the tasks that are currently running
			TaskStore taskStore = insight.getTaskStore();
			taskStore.clearAllTasks();
			logger.debug("Successfully cleared all stored tasks for the insight");
	
			// drop all the frame connections
			VarStore varStore = insight.getVarStore();
			Set<String> keys = varStore.getKeys();
			// find all the vars which are frames
			// and drop them
			for(String key : keys) {
				NounMetadata noun = varStore.get(key);
				PixelDataType nType = noun.getNounType();
				if(nType == PixelDataType.FRAME) {
					ITableDataFrame dm = (ITableDataFrame) noun.getValue();
					dm.close();
				}
			}
			logger.debug("Successfully removed all frames from insight");
			
			// clear insight
			insight.getVarStore().clear();
			logger.debug("Successfully removed all variables from varstore");
	
			Map<String, String> fileExports = insight.getExportFiles();
			if (fileExports != null && !fileExports.isEmpty()) {
				for (String fileKey : fileExports.keySet()){
					File f = new File(fileExports.get(fileKey));
					f.delete();
					logger.debug("Successfully deleted export file used in insight " + f.getName());
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
					e.printStackTrace();
				}
			}
			
			logger.info("Successfully cleared insight " + insight.getInsightId());
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("suppress", noOpType);
			return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.CLEAR_INSIGHT);
		}
	}
	
	public static NounMetadata dropInsight(Insight insight) {
		synchronized(insight) {
			logger.info("Droping insight " + insight.getInsightId());
	
			// i will first grab all the files used then delete them
			// only if this is not a saved insight + not a copied insight used for preview
			if(insight.isDeleteFilesOnDropInsight() && !insight.isSavedInsight()) {
				List<FileMeta> fileData = insight.getFilesUsedInInsight();
				if (fileData != null && !fileData.isEmpty()) {
					for (int fileIdx = 0; fileIdx < fileData.size(); fileIdx++) {
						FileMeta file = fileData.get(fileIdx);
						File f = new File(file.getFileLoc());
						f.delete();
						logger.debug("Successfully deleted File used in insight " + file.getFileLoc());
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
					e.printStackTrace();
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
		Set<String> frameKeys = varStore.getFrameKeys();
		for(String fKey : frameKeys) {
			NounMetadata noun = varStore.get(fKey);
			if(noun == null) {
				logger.info("why is the varstore not synced up???");
			} else {
				ITableDataFrame frame = (ITableDataFrame) noun.getValue();
				if(!retMap.containsKey(frame.getName())) {
					Map<String, Object> headers = frame.getFrameHeadersObject();
					retMap.put(frame.getName(), headers);
				}
			}
		}
		
		return retMap;
	}
	
}
