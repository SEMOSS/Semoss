package prerna.reactor.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class CollectMetaReactor extends AbstractReactor {

	// These are all caps because we UpperCase the user input
	private static final String TASK_OPTIONS = "TASKOPTIONS";
	private static final String HEADER_INFO = "HEADERINFO";
	private static final String SORT_INFO = "SORTINFO";
	private static final String FILTER_INFO = "FILTERINFO";

	public CollectMetaReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.VALUES.getKey()};
	}

	public NounMetadata execute() {
		ITask job = getTask();

		Map<String, Object> metaData = new HashMap<String, Object>(3);
		// get all the strings that were passed
		// and figure out which pieces of metadata to return
		List<NounMetadata> passedInStrings = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		int size = passedInStrings.size();
		for(int i = 0; i < size; i++) {
			String valToRetrieve = passedInStrings.get(i).getValue().toString().trim().toUpperCase();
			if(TASK_OPTIONS.equals(valToRetrieve)) {
				metaData.put("taskOptions", job.getTaskOptions());
			} else if(HEADER_INFO.equals(valToRetrieve)) {
				metaData.put("headerInfo", job.getHeaderInfo());
			} else if(SORT_INFO.equalsIgnoreCase(valToRetrieve)) {
				metaData.put("sortInfo", job.getSortInfo());
			} else if(FILTER_INFO.equalsIgnoreCase(valToRetrieve)) {
				metaData.put("filterInfo", job.getFilterInfo());
			}
		}
		metaData.put("taskId", job.getId());

		NounMetadata result = new NounMetadata(metaData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_METADATA);

		return result;
	}

	/**
	 * Get the task to collect from
	 * @return
	 */
	private ITask getTask() {
		ITask task = null;

		// look in the store
		GenRowStruct taskGrs = this.store.getNoun(PixelDataType.TASK.getKey());
		if(taskGrs != null && !taskGrs.isEmpty()) {
			task = (ITask) taskGrs.get(0);
			return task;
		}
		
		// look in curRow
		List<Object> tasks = curRow.getValuesOfType(PixelDataType.TASK);
		if(tasks == null || tasks.size() == 0) {
			task = (ITask) tasks.get(0);
		}
		return task;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;

		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_METADATA);
		outputs.add(output);
		return outputs;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VALUES.getKey())) {
			return "The string values: TASKOPTIONS, HEADERINFO, or SORTINFO. Entering these strings as input will add the task options, "
					+ "header info, or sort info to the metadata";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}