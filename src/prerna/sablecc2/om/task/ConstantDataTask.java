package prerna.sablecc2.om.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.task.options.TaskOptions;

public class ConstantDataTask extends AbstractTask {

	private transient Object outputData;
	private Map<String, Object> formatMap;
	/**
	 * Collect data from an iterator
	 * Or return defined outputData
	 */
	@Override
	public Map<String, Object> collect(boolean meta) {
		Map<String, Object> collectedData = new HashMap<String, Object>(7);
		collectedData.put("data", outputData);
		if(meta) {
			collectedData.put("format", getFormatMap());
			TaskOptions thisTaskOptions = getTaskOptions();
			if(thisTaskOptions != null) {
				collectedData.put("taskOptions", thisTaskOptions.getOptions());
			} else {
				collectedData.put("taskOptions", new HashMap<String, Object>());
			}
			collectedData.put("headerInfo", getHeaderInfo());
			collectedData.put("sortInfo", getSortInfo());
			collectedData.put("filterInfo", getFilterInfo());
		}
		collectedData.put("taskId", this.id);
		collectedData.put("numCollected", this.numCollect);
		return collectedData;
	}
	
	public void setOutputData(Object outputData) {
		this.outputData = outputData;
	}
	
	public Object getOutputData() {
		return this.outputData;
	}
	
	private Map<String, Object> getFormatMap() {
		if(this.formatMap == null) {
			formatMap = new HashMap<String, Object>();
			formatMap.put("type", "Custom Task Output");
		}
		return formatMap;
	}
	
	public void setFormatMap(Map<String, Object> formatMap) {
		this.formatMap = formatMap;
	}
	
	/*
	 * Bottom methods are not important
	 * This is just so I can have operations
	 * Return data as if it was a "Task"
	 */
	
	@Override
	public List<Object[]> flushOutIteratorAsGrid() {
		return null;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public IHeadersDataRow next() {
		return null;
	}

	@Override
	public void cleanUp() {
		this.outputData = null;
	}

	@Override
	public void reset() {
		// do nothing
	}

}
