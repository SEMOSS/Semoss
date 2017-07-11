package prerna.sablecc2.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.query.interpreters.QueryStruct2;
import prerna.sablecc2.reactor.export.FormatFactory;
import prerna.sablecc2.reactor.export.Formatter;

public class Job {

	private String id;
	private final Iterator iterator;
	private QueryStruct2 queryStruct; //this is the query struct that was used to generate this job
	private Map<String, List<Object>> options; //this holds the options object for the FE
	private Map<String, Map<String, String>> targets; //map of maps
	private List<Formatter> formatters;
	
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
	private List<Object> getData(int num) {
		int count = 0;
		List<Formatter> formatters = getFormatters();
		while(iterator.hasNext() && count < num) {
			Object next = iterator.next();
			
			if(next instanceof IHeadersDataRow) {
				IHeadersDataRow nextData = (IHeadersDataRow)next;
				for(Formatter formatter : formatters) {
					formatter.addData(nextData);
				}
			} else {
				//i don't know what to do :(
				throw new IllegalArgumentException("Unsupported format for output...");
			}
			count++;
		}
		
		List<Object> data = new Vector<Object>();
		for(Formatter formatter : formatters) {
			data.add(formatter.getFormattedData());
			formatter.clear();
		}
		return data;
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
		return options;
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
		if(formatKey != null) {
			targetMap.put("data", formatKey);
		}
		if(optionsKey != null) {
			targetMap.put("options", optionsKey);
		}
		this.targets.put(target, targetMap);
	}
	
	public void setOutput(String target, String formatKey, String optionsKey) {
		this.targets = new HashMap<>();
		addOutput(target, formatKey, optionsKey);
	}
	
	public void addFormat(String format) {
		Formatter formatter = FormatFactory.getFormatter(format);
		this.formatters.add(formatter);
	}
	
	public void setFormat(String format) {
		this.formatters.clear();
		addFormat(format);
	}

	public void setId(String newId) {
		this.id = newId;
	}
	
	public String getId() {
		return this.id;
	}
	
	/****************** END SETTERS **************************/
}
