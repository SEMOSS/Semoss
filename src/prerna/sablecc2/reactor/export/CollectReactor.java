package prerna.sablecc2.reactor.export;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
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
		buildTask();
		
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
		
		return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA); //return the data
	}
	
	@Override
	protected void buildTask() {
		this.task.optimizeQuery(this.limit);
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
		if(outputs != null) return outputs;
		
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
