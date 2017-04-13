package prerna.sablecc2.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.ds.querystruct.QueryStruct2;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.reactor.export.FormatFactory;
import prerna.sablecc2.reactor.export.Formatter;

public class Job {

	private final Iterator iterator;
	private QueryStruct2 queryStruct; //this is the query struct that was used to generate this job
	private Map<String, List<Object>> options; //this holds the options object for the FE
	private Map<String, Map<String, String>> targets; //map of maps
	private List<Formatter> formatters;
	//need to specify a format type
	
	public Job(Iterator iterator, QueryStruct2 queryStruct) {
		this.iterator = iterator;
		this.queryStruct = queryStruct;
		setDefaults();
	}
	
	private void setDefaults() {
		targets = new HashMap<>();
		options = new HashMap<>();
		formatters = new ArrayList<>();
	}
	
	public Object collect() {
		return null;
	}
	
	public Iterator getIterator() {
		return this.iterator;
	}
	
	/**
	 * 
	 * @param num
	 * @return
	 */
	public Object collect(int num) {
		
		Map<String, Object> collectedData = new HashMap<String, Object>(3);
		
		collectedData.put("data", getData(num));
		collectedData.put("options", getOptions());
		collectedData.put("targets", getTargets());
		
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
	private Map<String, Object> getData(int num) {
		int count = 0;
		List<Formatter> formatters = getFormatters();
		String[] headers = null;
		while(iterator.hasNext() && count < num) {
			
			Object next = iterator.next();
			
			if(next instanceof IHeadersDataRow) {
				IHeadersDataRow nextData = (IHeadersDataRow)next;
				
				if(headers == null) {
					headers = nextData.getHeaders();
				}
				
				for(Formatter formatter : formatters) {
					formatter.addData(nextData);
				}
			} else{
				//i don't know what to do :(
			}
			
			count++;
		}
		
		Map<String, Object> retMap = new HashMap<>(1);
		for(Formatter formatter : formatters) {
			Map<String, Object> formattedData = new HashMap<>(2);
			formattedData.put("values", formatter.getFormattedData());
			formattedData.put("headers", headers);
			retMap.put(formatter.getIdentifier(), formattedData);
			formatter.clear();
		}
		return retMap;
	}
	
	/**
	 * Returns structure in this format:
	 * 		{
	 * 			"optionsKey": {
	 * 				key : "value",
	 * 				key2: "value2"
	 * 			}
	 * 		}
	 */
	private Object getOptions() {
		options.remove(PkslDataTypes.JOB.toString());
		options.remove("all");
		
		String optionsName = (String)options.remove("optionsName").get(0);
		
		Map<String, Object> retMap = new HashMap<>();
		retMap.put(optionsName, options);
		return retMap;
	}
	
	/**
	 * 
	 * Returns structure in this format:
	 * 		{
	 * 			"bar": {
	 * 				data : "dataKey",
	 * 				options: "optionsKey"
	 * 			}
	 * 		}
	 */
	private Object getTargets() {
		return this.targets;
	}
	
	private List<Formatter> getFormatters() {
		if(this.formatters == null || this.formatters.isEmpty()) {
			List<Formatter> formatters = new ArrayList<>(1);
			formatters.add(FormatFactory.getFormatter("table"));
			return formatters;
		}
		return this.formatters;
	}
	
	/****************** SETTERS ******************************/
	
	public void setOptions(Map<String, List<Object>> options) {
		this.options.clear();
		this.options.putAll(options);
	}
	
	public void addOption(String key, Object option) {
		options.putIfAbsent(key, new ArrayList<>());
		options.get(key).add(option);
	}
	
	public void addOutput(String target, String formatKey, String optionsKey) {
		Map<String, String> targetMap = new HashMap<>(2);
		targetMap.put("data", formatKey);
		targetMap.put("options", optionsKey);
		this.targets.put(target, targetMap);
	}
	
	public void setOutput(String target, String formatKey, String optionsKey) {
		this.targets = new HashMap<>();
		addOutput(target, formatKey, optionsKey);
	}
	
	public void addFormat(String format, String name) {
		Formatter formatter = FormatFactory.getFormatter(format);
		formatter.setIdentifier(name);
		this.formatters.add(formatter);
	}
	
	public void setFormat(String format, String name) {
		this.formatters.clear();
		addFormat(format, name);
	}
	
	/****************** END SETTERS **************************/
}
