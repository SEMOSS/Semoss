package prerna.reactor.export;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.usertracking.UserTrackerFactory;

public class CollectReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	private int limit = 0;
	
	public CollectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.INCLUDE_META_KEY.getKey()};
	}
	
	public NounMetadata execute() {
		this.task = getTask();		
		
		this.limit = getTotalToCollect();
		this.task.setNumCollect(this.limit);
		try {
			buildTask();
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(e.getMessage());
		}
		
		// tracking
		if (this.task instanceof BasicIteratorTask) {
			try {
				// NEW TRACKER
				if(this.task.getTaskOptions() != null && !this.task.getTaskOptions().isEmpty()) {
					UserTrackerFactory.getInstance().trackVizWidget(this.insight, this.task.getTaskOptions(), ((BasicIteratorTask) task).getQueryStruct());
				} else {
					UserTrackerFactory.getInstance().trackQueryData(this.insight, ((BasicIteratorTask) task).getQueryStruct());
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		PixelOperationType retOpType = PixelOperationType.TASK_DATA;
		// this is the second place I need to change
		TaskOptions ornamnetOptions = genOrnamentTaskOptions();
		if(ornamnetOptions != null || (task.getTaskOptions() != null && task.getTaskOptions().isOrnament()) ) {
			this.task.setTaskOptions(ornamnetOptions);
			retOpType = PixelOperationType.PANEL_ORNAMENT_DATA;
		}
		
		// this is causing more confusion and breaks facet
//		if(this.task.getTaskOptions() == null) {
//			// I am setting the panel id on the task options and getting it from here
//			TaskOptions taskOptions = this.insight.getLastTaskOptions();
//			if(taskOptions != null) {
//				Set<String> pIds = taskOptions.getPanelIds();
//				String panelId = pIds.iterator().next();
//				String layout = taskOptions.getLayout(panelId);
//				
//				if(this.task instanceof BasicIteratorTask) {
//					SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
//					if(!qs.getSelectors().isEmpty()) {
//						TaskOptions newTOptions = AutoTaskOptionsHelper.getAutoOptions(qs, panelId, layout);
//						this.task.setTaskOptions(newTOptions);
//					}
//				}
//				
//				if(this.task.getTaskOptions() == null) {
//					this.task.setTaskOptions(this.insight.getLastTaskOptions());
//				}
//			}
//		}
		
		return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, retOpType);
	}
	
	@Override
	protected void buildTask() throws Exception {
		// if the task was already passed in
		// we do not need to optimize/recreate the iterator
		if(this.task.isOptimized()) {
			this.task.optimizeQuery(this.limit);
		}
	}
	
	private TaskOptions genOrnamentTaskOptions() {
		if(this.subAdditionalReturn != null && this.subAdditionalReturn.size() == 1) {
			NounMetadata noun = this.subAdditionalReturn.get(0);
			if(noun.getNounType() == PixelDataType.ORNAMENT_MAP) {
				// we will use this map as task options
				TaskOptions options = new TaskOptions((Map<String, Object>) noun.getValue());
				options.setOrnament(true);
				return options;
			}
		}
		return null;
	}
	
	//returns how much do we need to collect
	private int getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getNoun(keysToGet[1]);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}
		
		// try the cur row
		List<Object> allNumericInputs = this.curRow.getAllNumericColumns();
		if(allNumericInputs != null && !allNumericInputs.isEmpty()) {
			return ((Number) allNumericInputs.get(0)).intValue();
		}
		
		// default to 500
		return 500;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null && !outputs.isEmpty()) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The number to collect";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
