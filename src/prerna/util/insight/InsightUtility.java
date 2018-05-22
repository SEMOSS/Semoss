package prerna.util.insight;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.PixelPlanner;

public class InsightUtility {

	private InsightUtility() {
		
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
	

}
