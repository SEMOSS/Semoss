package prerna.util.insight;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
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

public class InsightUtility {

	protected static final Logger LOGGER = LogManager.getLogger(InsightUtility.class.getName());

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
		newInsight.setPy(origInsight.getPy());
		newInsight.setUser(origInsight.getUser());
		if(origInsight.rInstantiated()) {
			newInsight.setRJavaTranslator(origInsight.getRJavaTranslator(LOGGER));
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
				if(varStore.get(k) == noun) {
					keysToRemove.add(k);
				}
			}
			for(String k : keysToRemove) {
				varStore.remove(k);
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
	public static NounMetadata clearInsight(Insight insight) {
		synchronized(insight) {
			LOGGER.info("Start clearning insight " + insight.getInsightId());

			// drop all the tasks that are currently running
			TaskStore taskStore = insight.getTaskStore();
			taskStore.clearAllTasks();
			LOGGER.debug("Successfully cleared all stored tasks for the insight");
	
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
			LOGGER.debug("Successfully removed all frames from insight");
			
			// clear insight
			insight.getVarStore().clear();
			LOGGER.debug("Successfully removed all variables from varstore");
	
			Map<String, String> fileExports = insight.getExportFiles();
			if (fileExports != null && !fileExports.isEmpty()) {
				for (String fileKey : fileExports.keySet()){
					File f = new File(fileExports.get(fileKey));
					f.delete();
					LOGGER.debug("Successfully deleted export file used in insight " + f.getName());
				}
			}
			
			// if R is instantiated
			// remove all the variables
			// this will happen in your environment
			if(insight.rInstantiated()) {
				try {
					AbstractRJavaTranslator rJava = insight.getRJavaTranslator(LOGGER);
					rJava.runR("rm(list=ls())");
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
			LOGGER.info("Successfully cleared insight " + insight.getInsightId());
			return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.CLEAR_INSIGHT);
		}
	}
	
	public static NounMetadata dropInsight(Insight insight) {
		synchronized(insight) {
			LOGGER.info("Droping insight " + insight.getInsightId());
	
			// i will first grab all the files used then delete them
			// only if this is not a saved insight + not a copied insight used for preview
			if(insight.isDeleteFilesOnDropInsight() && !insight.isSavedInsight()) {
				List<FileMeta> fileData = insight.getFilesUsedInInsight();
				if (fileData != null && !fileData.isEmpty()) {
					for (int fileIdx = 0; fileIdx < fileData.size(); fileIdx++) {
						FileMeta file = fileData.get(fileIdx);
						File f = new File(file.getFileLoc());
						f.delete();
						LOGGER.debug("Successfully deleted File used in insight " + file.getFileLoc());
					}
				}
			}
			
			// now i will clear
			LOGGER.info("Clear insight for drop");
			clearInsight(insight);
	
			LOGGER.debug("Removing from insight store");
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
			if(insight.rInstantiated()) {
				try {
					AbstractRJavaTranslator rJava = insight.getRJavaTranslator(LOGGER);
					rJava.removeEnv();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
//			NounMetadata sessionNoun = insight.getVarStore().get(JobReactor.SESSION_KEY);
//			if(sessionNoun != null) {
//				String sessionId = sessionNoun.getValue().toString();
//				Set<String> insightIdsForSesh = InsightStore.getInstance().getInsightIDsForSession(sessionId);
//				if(insightIdsForSesh != null) {
//					insightIdsForSesh.remove(insightId);
//				}
//			}
			
			LOGGER.info("Successfully dropped insight " + insight.getInsightId());
			return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DROP_INSIGHT);
		}
	}
}
