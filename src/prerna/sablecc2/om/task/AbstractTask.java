package prerna.sablecc2.om.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.reactor.export.FormatFactory;
import prerna.sablecc2.reactor.export.Formatter;
import prerna.sablecc2.reactor.export.TableFormatter;

public abstract class AbstractTask implements ITask {

	protected String id;
	// this holds the options object for the FE
	protected transient Map<String, Object> taskOptions; 
	// this holds the header info for the FE
	protected transient List<Map<String, Object>> headerInfo;
	//this holds the sort info for the FE
	protected transient List<Map<String, Object>> sortInfo;
	// this holds the formatter to perform any viz specific transformations
	// of the data
	protected transient Formatter formatter = null;
	// logger
	protected transient Logger logger;
	// protected
	protected long internalOffset = 0;
	
	public AbstractTask() {
		this.taskOptions = new HashMap<>();
		this.sortInfo = new ArrayList<Map<String, Object>>();
		this.headerInfo = new ArrayList<Map<String, Object>>();
		// just default to a table formatter
		this.formatter = new TableFormatter();
	}
	
	/**
	 * Collect data from an iterator
	 * Or return defined outputData
	 */
	@Override
	public Map<String, Object> collect(int num, boolean meta) {
		Map<String, Object> collectedData = new HashMap<String, Object>(7);
		collectedData.put("data", getData(num));
		if(meta) {
			collectedData.put("format", getFormatMap());
			collectedData.put("taskOptions", getTaskOptions());
			collectedData.put("headerInfo", this.headerInfo);
			collectedData.put("sortInfo", this.sortInfo);
		}
		collectedData.put("taskId", this.id);
		collectedData.put("numCollected", num);
		return collectedData;
	}
	
	/**
	 * Returns structure based on the formatter: Example for Table
	 * {
	 * 	data : List of Arrays of Data,
	 * 	headers: headers of my data 
	 * }
	 */
	@Override
	public Object getData(int num) {
		this.formatter.clear();
		boolean collectAll = false;
		if(num == -1) {
			collectAll = true;
		}
		int count = 0;
		// recall, a task is also an iterator!
		while(this.hasNext() && (collectAll || count < num)) {
			IHeadersDataRow next = this.next();
			this.formatter.addData(next);
			count++;
		}
		this.internalOffset += count;
		return formatter.getFormattedData();
	}
	
	@Override
	public List<Object[]> flushOutIteratorAsGrid() {
		TableFormatter format = new TableFormatter();
		while(hasNext()) {
			IHeadersDataRow next = next();
			format.addData(next);
		}
		return format.getData();
	}
	
	// MOST IMPORTANT IS THE ID
	@Override
	public void setId(String newId) {
		this.id = newId;
	}
	
	@Override
	public String getId() {
		return this.id;
	}
	
	// OTHER GETTERS 
	
	@Override
	public Map<String, Object> getTaskOptions() {
		return this.taskOptions;
	}
	
	@Override
	public List<Map<String, Object>> getHeaderInfo() {
		return this.headerInfo;
	}
	
	@Override
	public List<Map<String, Object>> getSortInfo() {
		return this.sortInfo;
	}
	
	@Override
	public Formatter getFormatter() {
		return this.formatter;
	}
	
	private Map<String, Object> getFormatMap() {
		Map<String, Object> formatMap = new HashMap<String, Object>();
		formatMap.put("type", this.formatter.getFormatType());
		formatMap.put("options", this.formatter.getOptionsMap());
		return formatMap;
	}
	
	// OTHER SETTESR

	@Override
	public void setTaskOptions(Map<String, Object> taskOptions) {
		this.taskOptions = taskOptions;
	}
	
	@Override
	public void setFormat(String format) {
		this.formatter = FormatFactory.getFormatter(format);
	}
	
	@Override
	public void setFormatOptions(Map<String, Object> optionValues) {
		this.formatter.setOptionsMap(optionValues);
	}
	
	@Override
	public void setHeaderInfo(List<Map<String, Object>> headerInfo) {
		this.headerInfo = headerInfo;
	}
	
	@Override
	public void setSortInfo(List<Map<String, Object>> sortInfo) {
		this.sortInfo = sortInfo;
	}
	
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	@Override
	public void optimizeQuery(int limit) {
		// this does nothing by default
		// only makes sense for BasicIterator
		// since we modify the QS to only return this many values
	}
	
	// JUST TO MAKE IT EASIER TO DEBUG
	
	@Override
	public String toString() {
		return this.id;
	}

}
