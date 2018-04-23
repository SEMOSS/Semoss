package prerna.sablecc2.reactor.export;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;

public class CollectReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	private int limit = 0;
	
	public CollectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.INCLUDE_META_KEY.getKey()};
	}
	
	public NounMetadata execute() {
		this.limit = getTotalToCollect();
		this.task = getTask();
		buildTask();
		return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA); //return the data
//		Object data = this.task.collect(this.limit, collectMeta());
//		NounMetadata result = new NounMetadata(data, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
//		return result;
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
	
	//return if we should get the metadata for the task
	private boolean collectMeta() {
		// try the key
		GenRowStruct includeMetaGrs = store.getNoun(keysToGet[2]);
		if(includeMetaGrs != null && !includeMetaGrs.isEmpty()) {
			return (boolean) includeMetaGrs.get(0);
		}
		
		// try the cur row
		List<NounMetadata> booleanNouns = this.curRow.getNounsOfType(PixelDataType.BOOLEAN);
		if(booleanNouns != null && !booleanNouns.isEmpty()) {
			return (boolean) booleanNouns.get(0).getValue();
		}
		
		return true;
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
