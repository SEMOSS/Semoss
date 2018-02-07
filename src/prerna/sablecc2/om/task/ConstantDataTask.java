package prerna.sablecc2.om.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;

public class ConstantDataTask extends AbstractTask {

	private transient Object outputData;
	private Map<String, Object> formatMap;
	/**
	 * Collect data from an iterator
	 * Or return defined outputData
	 */
	@Override
	public Map<String, Object> collect(int num, boolean meta) {
		Map<String, Object> collectedData = new HashMap<String, Object>(7);
		collectedData.put("data", outputData);
		if(meta) {
			collectedData.put("format", getFormatMap());
			collectedData.put("taskOptions", getTaskOptions());
			collectedData.put("headerInfo", getHeaderInfo());
			collectedData.put("sortInfo", getSortInfo());
			collectedData.put("filterInfo", getFilterInfo());
		}
		collectedData.put("taskId", this.id);
		collectedData.put("numCollected", num);
		return collectedData;
	}
	
	public void setOutputObject(Object outputData) {
		this.outputData = outputData;
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
