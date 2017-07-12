package prerna.sablecc2.om;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.query.interpreters.QueryStruct2;
import prerna.sablecc2.reactor.export.FormatFactory;
import prerna.sablecc2.reactor.export.Formatter;

public class Job {

	private String id;
	private final Iterator iterator;
	private Map<String, List<Object>> options; //this holds the options object for the FE
	private Formatter formatter;
	private List<Map<String, Object>> headerInfo;
	
	public Job(Iterator iterator, QueryStruct2 queryStruct) {
		this.iterator = iterator;
		this.options = new HashMap<>();
	}
	
	/**
	 * 
	 * @param num
	 * @return
	 */
	public Object collect(int num) {
		Map<String, Object> collectedData = new HashMap<String, Object>(3);
		collectedData.put("data", getData(num));
		collectedData.put("viewOptions", getOptions());
		collectedData.put("headerInfo", getHeaderInfo());
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
	
	/**
	 * Returns structure in this format:
	 * 		{
	 * 			"optionsKey": {
	 * 				key : "value",
	 * 				key2: "value2"
	 * 			}
	 * 		}
	 * @return 
	 */
	private Map<String, List<Object>> getOptions() {
		options.remove(PkslDataTypes.JOB.toString());
		options.remove("all");
		return options;
	}
	
	public Iterator getIterator() {
		return this.iterator;
	}
	
	public List<Map<String, Object>> getHeaderInfo() {
		return headerInfo;
	}
	
	/****************** SETTERS ******************************/
	
	public void setOptions(Map<String, List<Object>> options) {
		this.options.clear();
		this.options.putAll(options);
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
