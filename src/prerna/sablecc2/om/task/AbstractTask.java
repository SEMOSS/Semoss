package prerna.sablecc2.om.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.export.FormatFactory;
import prerna.reactor.export.IFormatter;
import prerna.reactor.export.TableFormatter;
import prerna.sablecc2.om.task.options.TaskOptions;

public abstract class AbstractTask implements ITask {

	protected String id;
	// this holds the options object for the FE
	protected transient TaskOptions taskOptions; 
	// this holds the header info for the FE
	protected transient List<Map<String, Object>> headerInfo;
	// this holds the sort info for the FE
	protected transient List<Map<String, Object>> sortInfo;
	// this holds explicitly defined filters on the qs
	protected transient List<Map<String, Object>> filterInfo;
	
	// this holds the formatter to perform any viz specific transformations
	// of the data
	protected transient IFormatter formatter = null;
	// logger
	protected transient Logger classLogger;
	// internal offset
	protected long internalOffset = 0;
	// num to collect
	protected int numCollect = 500;
	// size of the task
	protected long numRows = 0;
	// optimized query
	// so we know the limit is a fake one
	protected boolean isOptimize = false;
	
	private boolean meta = true;
	
	public AbstractTask() {
		this.sortInfo = new ArrayList<Map<String, Object>>();
		this.headerInfo = new ArrayList<Map<String, Object>>();
		// just default to a table formatter
		this.formatter = new TableFormatter();
	}
	
	/////////////////////////////////////
	
	/*
	 * abstract methods block
	 */
	
	/**
	 * Need to track a log of the sources
	 * @return
	 */
	public abstract List<Map<String, String>> getSource();
	
	////////////////////////////////////
	
	/**
	 * Collect data from an iterator
	 * Or return defined outputData
	 * @throws Exception 
	 */
	@Override
	public Map<String, Object> collect(boolean meta) throws Exception {
		Map<String, Object> collectedData = new HashMap<String, Object>(10);
		collectedData.put("data", getData());
		if(meta) {
			collectedData.put("headerInfo", this.getHeaderInfo());
			if(this.taskOptions != null && !this.taskOptions.isEmpty()) {
				collectedData.put("format", getFormatMap());
				collectedData.put("taskOptions", this.taskOptions.getOptions());
				collectedData.put("sortInfo", this.sortInfo);
				collectedData.put("filterInfo", this.filterInfo);
				long numRows = TaskUtility.getNumRows(this);
				if(numRows > 0) {
					collectedData.put("numRows", numRows);
				}
			}
		}
		collectedData.put("sources", getSource());
		collectedData.put("taskId", this.id);
		collectedData.put("numCollected", this.numCollect);
		return collectedData;
	}
	
	@Override
	public Map<String, Object> getMetaMap() {
		Map<String, Object> collectedData = new HashMap<String, Object>(10);
		collectedData.put("headerInfo", this.getHeaderInfo());
		collectedData.put("numCollected", this.numCollect);
		if(this.taskOptions != null && !this.taskOptions.isEmpty()) {
			collectedData.put("format", getFormatMap());
			collectedData.put("taskOptions", this.taskOptions.getOptions());
			collectedData.put("sortInfo", this.sortInfo);
			collectedData.put("filterInfo", this.filterInfo);
			long numRows = 0;
			try {
				numRows = TaskUtility.getNumRows(this);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(numRows > 0) {
				collectedData.put("numRows", numRows);
			}
		}
		collectedData.put("sources", getSource());
		return collectedData;
	}
	
	/**
	 * Returns structure based on the formatter: Example for Table
	 * {
	 * 	data : List of Arrays of Data,
	 * 	headers: headers of my data 
	 * }
	 */
	public Object getData() {
		this.formatter.clear();
		boolean collectAll = false;
		if(this.numCollect == -1) {
			collectAll = true;
		}
		int count = 0;
		// recall, a task is also an iterator!
		while(this.hasNext() && (collectAll || count < this.numCollect)) {
			IHeadersDataRow next = this.next();
			this.formatter.addData(next);
			count++;
		}
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
	public TaskOptions getTaskOptions() {
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
	public IFormatter getFormatter() {
		return this.formatter;
	}
	
	protected Map<String, Object> getFormatMap() {
		Map<String, Object> formatMap = new HashMap<String, Object>();
		formatMap.put("type", this.formatter.getFormatType());
		formatMap.put("options", this.formatter.getOptionsMap());
		return formatMap;
	}
	
	// OTHER SETTESR

	@Override
	public void setTaskOptions(TaskOptions taskOptions) {
		this.taskOptions = taskOptions;
	}
	
	@Override
	public void setFormat(String format) {
		this.formatter = FormatFactory.getFormatter(format);
	}
	
	@Override
	public void setFormat(IFormatter formatter) {
		this.formatter = formatter;
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
	public void setFilterInfo(GenRowFilters grf) {
		this.filterInfo = grf.getFormatedFilters();
	}
	
	@Override
	public List<Map<String, Object>> getFilterInfo() {
		return this.filterInfo;
	}
	
	@Override
	public void setSortInfo(List<Map<String, Object>> sortInfo) {
		this.sortInfo = sortInfo;
	}
	
	@Override
	public void setLogger(Logger logger) {
		this.classLogger = logger;
	}
	
	@Override
	public int getNumCollect() {
		return this.numCollect;
	}
	
	@Override
	public void setNumCollect(int numCollect) {
		this.numCollect = numCollect;
	}
	
	@Override
	public void setMeta(boolean meta) {
		this.meta  = meta;
	}
	
	@Override
	public boolean getMeta() {
		return this.meta;
	}
	
	@Override
	public void optimizeQuery(int limit) throws Exception {
		// this does nothing by default
		// only makes sense for BasicIterator
		// since we modify the QS to only return this many values
	}
	
	@Override
	public boolean isOptimized() {
		return this.isOptimize;
	}
	
	@Override
	public void toOptimize(boolean toOptimize) {
		this.isOptimize = toOptimize;
	}
	
	// JUST TO MAKE IT EASIER TO DEBUG
	
	// dummy method to avoid errors on creating cache
	@Override
	public RawCachedWrapper createCache() throws Exception {
		return null;
	}
	
	// dummy
	@Override
	public String getPragma(String key) {
		return null;
	}

	@Override
	public String toString() {
		return this.id;
	}
	
	public long getNumRows() {
		return this.numRows;
	}

	public void setNumRows(long numRows) {
		this.numRows = numRows;
	}
	
	public long getInternalOffset() {
		return this.internalOffset;
	}
	
	public void setInternalOffset(long internalOffset) {
		this.internalOffset = internalOffset;
	}
}
