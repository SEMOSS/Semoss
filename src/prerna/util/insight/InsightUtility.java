package prerna.util.insight;

import java.io.File;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
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
		String[] keys = new String[]{JobReactor.JOB_KEY, JobReactor.SESSION_KEY, JobReactor.INSIGHT_KEY};
		for(String key : keys) {
			if(origInsight.getVarStore().containsKey(key)) {
				newInsight.getVarStore().put(key, origInsight.getVarStore().get(key));
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
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		
		PixelDataType nType = noun.getNounType();
		if(nType == PixelDataType.FRAME) {
			ITableDataFrame dm = (ITableDataFrame) noun.getValue();
			//TODO: expose a delete on the frame to hide this crap
			// drop the existing tables/connections if present
			if(dm instanceof H2Frame) {
				H2Frame frame = (H2Frame)dm;
				frame.dropTable();
				if(!frame.isInMem()) {
					frame.dropOnDiskTemporalSchema();
				}
			} else if(dm instanceof RDataTable) {
				RDataTable frame = (RDataTable)dm;
				frame.executeRScript("gc(" + frame.getTableName() + ");");
			}
			
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
		String insightId = insight.getInsightId();

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
//				dm.setLogger(logger);
				//TODO: expose a delete on the frame to hide this crap
				// drop the existing tables/connections if present
				if(dm instanceof H2Frame) {
					H2Frame frame = (H2Frame)dm;
					frame.dropTable();
					if(!frame.isInMem()) {
						frame.dropOnDiskTemporalSchema();
					}
				} else if(dm instanceof RDataTable) {
					RDataTable frame = (RDataTable) dm;
					frame.closeConnection();
				}
			}
		}
//		logger.info("Successfully removed all frames from insight");
		
		insight.getVarStore().clear();
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
	
}
