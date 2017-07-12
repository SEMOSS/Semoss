package prerna.sablecc2.om;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.query.interpreters.QueryStruct2;
import prerna.sablecc2.reactor.export.FormatFactory;
import prerna.sablecc2.reactor.export.Formatter;
import prerna.sablecc2.reactor.export.TableFormatter;

public class Job {

	private String id;
	private final Iterator iterator;
	private Map<String, Object> viewOptions; //this holds the options object for the FE
	private Formatter formatter = null;;
	private List<Map<String, Object>> headerInfo;
	
	public Job(Iterator iterator, QueryStruct2 queryStruct) {
		this.iterator = iterator;
		this.viewOptions = new HashMap<>();
		this.formatter = new TableFormatter();
	}
	
	/**
	 * 
	 * @param num
	 * @return
	 */
	public Map<String, Object> collect(int num, boolean meta) {
		Map<String, Object> collectedData = new HashMap<String, Object>(3);
		collectedData.put("data", getData(num));
		if(meta) {
			collectedData.put("viewOptions", getViewOptions());
			collectedData.put("headerInfo", this.headerInfo);
		}
		collectedData.put("jobId", this.id);
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
		int count = 0;
		while(iterator.hasNext() && count < num) {
			Object next = iterator.next();
			if(next instanceof IHeadersDataRow) {
				IHeadersDataRow nextData = (IHeadersDataRow)next;
				formatter.addData(nextData);
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
	
	public Map<String, Object> getViewOptions() {
		return viewOptions;
	}
	
	public List<Map<String, Object>> getHeaderInfo() {
		return headerInfo;
	}
	
	/****************** SETTERS ******************************/
	
	public void setViewOptions(Map<String, Object> viewOptions) {
		this.viewOptions = viewOptions;
	}
	
	public void setFormat(String format) {
		this.formatter = FormatFactory.getFormatter(format);
	}

	public void setId(String newId) {
		this.id = newId;
	}
	
	public String getId() {
		return this.id;
	}

	public void setHeaderInfo(List<Map<String, Object>> headerInfo) {
		this.headerInfo = headerInfo;		
	}

	/****************** END SETTERS **************************/
}
