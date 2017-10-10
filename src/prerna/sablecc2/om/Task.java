package prerna.sablecc2.om;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.sablecc2.reactor.export.FormatFactory;
import prerna.sablecc2.reactor.export.Formatter;
import prerna.sablecc2.reactor.export.TableFormatter;
import prerna.util.Utility;

public class Task {

	private String id;
	private transient Iterator<IHeadersDataRow> iterator;
	private transient Map<String, Object> taskOptions; //this holds the options object for the FE
	private transient Formatter formatter = null;;
	private transient List<Map<String, Object>> headerInfo;
	private transient List<Map<String, Object>> sortInfo;
	private transient Object outputData;
	
	public Task() {
		this.taskOptions = new HashMap<>();
	}
	
	public Task(Iterator<IHeadersDataRow> iterator) {
		this.iterator = iterator;
		this.taskOptions = new HashMap<>();
		this.formatter = new TableFormatter();
	}
	
	/**
	 * 
	 * @param num
	 * @return
	 */
	public Map<String, Object> collect(int num, boolean meta) {
		Map<String, Object> collectedData = new HashMap<String, Object>(3);
		if(outputData == null) {
			collectedData.put("data", getData(num));
			if(meta) {
				collectedData.put("format", this.formatter.getFormatType());
			}
		} else {
			collectedData.put("data", outputData);
			if(meta) {
				collectedData.put("format", "Custom Job Output");
			}
		}
		if(meta) {
			collectedData.put("taskOptions", getTaskOptions());
			collectedData.put("headerInfo", this.headerInfo);
			collectedData.put("sortInfo", this.sortInfo);
		}
		collectedData.put("taskId", this.id);
		collectedData.put("numCollected", num);
		return collectedData;
	}

	/**
	 * Returns structure in this format:
	 * 		{
	 * 			"dataKey": {
	 * 				data : List of Arrays of Data,
	 * 				headers: headers of my data 
	 * 			}
	 * 		}
	 */
	private Object getData(int num) {
		this.formatter.clear();
		int count = 0;
		while(this.iterator.hasNext() && count < num) {
			Object next = iterator.next();
			if(next instanceof IHeadersDataRow) {
				IHeadersDataRow nextData = (IHeadersDataRow)next;
				this.formatter.addData(nextData);
			} else {
				//i don't know what to do :(
				throw new IllegalArgumentException("Unsupported format for output...");
			}
			count++;
		}
		
		return formatter.getFormattedData();
	}
	
	public Iterator getIterator() {
		return this.iterator;
	}
	
	public Map<String, Object> getTaskOptions() {
		return this.taskOptions;
	}
	
	public List<Map<String, Object>> getHeaderInfo() {
		return this.headerInfo;
	}
	
	public List<Map<String, Object>> getSortInfo() {
		return this.sortInfo;
	}
	
	/****************** SETTERS ******************************/
	
	public void setTaskOptions(Map<String, Object> taskOptions) {
		this.taskOptions = taskOptions;
	}
	
	public void setFormat(String format) {
		this.formatter = FormatFactory.getFormatter(format);
	}
	
	public void setFormatOptions(Map<String, Object> optionValues) {
		this.formatter.setOptionsMap(optionValues);
	}

	public void setId(String newId) {
		this.id = newId;
	}
	
	public String getId() {
		return this.id;
	}
	
	public void setOutputObject(Object outputData) {
		this.outputData = outputData;
	}

	public void setHeaderInfo(List<Map<String, Object>> headerInfo) {
		this.headerInfo = headerInfo;
		//TODO: so bad :(
		//need to create a proper iterate object that will get this info
		//instead of how it is set up which takes it from the QS
		if(this.headerInfo != null) {
			String[] types = null;
			if(this.iterator instanceof RawRDBMSSelectWrapper) {
				types = ((RawRDBMSSelectWrapper) this.iterator).getTypes();
			}
			if(types != null) {
				for(int i = 0; i < headerInfo.size(); i++) {
					headerInfo.get(i).put("type", Utility.getCleanDataType(types[i]));
				}
			}
		}
	}
	
	public void setSortInfo(List<Map<String, Object>> sortInfo) {
		this.sortInfo = sortInfo;
	}

	/****************** END SETTERS **************************/
	
	@Override
	public String toString() {
		return this.id;
	}
	
	public List<Object[]> flushOutIteratorAsGrid() {
		TableFormatter format = new TableFormatter();
		while(iterator.hasNext()) {
			Object next = iterator.next();
			if(next instanceof IHeadersDataRow) {
				IHeadersDataRow nextData = (IHeadersDataRow)next;
				format.addData(nextData);
			} else {
				//i don't know what to do :(
				throw new IllegalArgumentException("Unsupported format for output...");
			}
		}
		
		return format.getData();
	}
}
