package prerna.util.insight;

import java.io.File;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.job.JobReactor;

public class InsightUtility {

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
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		
		PixelDataType nType = noun.getNounType();
		if(nType == PixelDataType.FRAME) {
			ITableDataFrame dm = (ITableDataFrame) noun.getValue();
			dm.close();

			// if it is the current frame
			// also remove it
			if(dm == varStore.get(Insight.CUR_FRAME_KEY)) {
				varStore.remove(Insight.CUR_FRAME_KEY);
			} else if(key.equals(Insight.CUR_FRAME_KEY)) {
				// if we are removing curframekey
				// also remove if it is added twice with its alias
				String alias = dm.getTableName();
				if(alias != null && !alias.isEmpty()) {
					varStore.remove(alias);
				}
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
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
		// drop all the tasks that are currently running
		TaskStore taskStore = insight.getTaskStore();
		taskStore.clearAllTasks();
//		logger.info("Successfully cleared all stored Tasks for the insight");

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
//		logger.info("Successfully removed all frames from insight");
		// need to keep session key
		NounMetadata sessionKey = varStore.get(JobReactor.SESSION_KEY);
		// clear insight
		insight.getVarStore().clear();
		// add session key
		insight.getVarStore().put(JobReactor.SESSION_KEY, sessionKey);
		//		logger.info("Successfully removed all variables from varstore");

		// also delete any files that were used
//		List<FileMeta> fileData = insight.getFilesUsedInInsight();
//		if (fileData != null && !fileData.isEmpty()) {
//			for (int fileIdx = 0; fileIdx < fileData.size(); fileIdx++) {
//				FileMeta file = fileData.get(fileIdx);
//				File f = new File(file.getFileLoc());
//				f.delete();
//				logger.info("Successfully deleted File used in insight " + file.getFileLoc());
//			}
//		}
		
		Map<String, String> fileExports = insight.getExportFiles();
		if (fileExports != null && !fileExports.isEmpty()) {
			for (String fileKey : fileExports.keySet()){
				File f = new File(fileExports.get(fileKey));
				f.delete();
//				logger.info("Successfully deleted File used in insight " + f.getName());
			}
		}
		
//		logger.info("Successfully cleared insight");
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.CLEAR_INSIGHT);
	}
	
	public static NounMetadata dropInsight(Insight insight) {
		clearInsight(insight);
		
		String insightId = insight.getInsightId();
		InsightStore.getInstance().remove(insightId);

		NounMetadata sessionNoun = insight.getVarStore().get(JobReactor.SESSION_KEY);;
		if(sessionNoun != null) {
			String sessionId = sessionNoun.getValue().toString();
			Set<String> insightIdsForSesh = InsightStore.getInstance().getInsightIDsForSession(sessionId);
			if(insightIdsForSesh != null) {
				insightIdsForSesh.remove(insightId);
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.CLEAR_INSIGHT);
	}
}
