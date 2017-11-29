package prerna.engine.impl.r;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.QueryStruct;
import prerna.poi.main.helper.CSVFileHelper;

public class RCsvFileWrapper {

	// set the filepath for the csv file
	private String filePath;
	
	// store the headers used
	private String[] headersArr;
	// store the data types for each header
	private String[] dataTypesArr;
	// store the user defined headers if modified
	private Map<String, String> newHeaders;
	
	// store map of headers to data types
	private Map<String, SemossDataType> dataTypes;
	
	// store the produced query to execute in order to apply
	// the qs onto the dataframe
	private String rScript;
	
	// store the script to modify the header names
	private String modHeadersRVec;
	
	public RCsvFileWrapper(String filePath) {
		this.filePath = filePath;
	}
	
	public void composeRScript(QueryStruct qs, Map<String, String> dataTypeMap, List<String> headerNames, Map<String, String> newHeaders) {
		int size = dataTypeMap.keySet().size();
		this.headersArr = headerNames.toArray(new String[]{});
		this.dataTypesArr = new String[size];
		this.dataTypes = new Hashtable<String, SemossDataType>();
		this.newHeaders = newHeaders;
		
		int counter = 0;
		for(String header : headersArr) {
			dataTypesArr[counter] = dataTypeMap.get(header);
			dataTypes.put(header, SemossDataType.convertStringToDataType(dataTypesArr[counter]));
			
			counter++;
		}
		
		// the qs is passed from AbstractApiReactor
		RInterpreter rI = new RInterpreter();
		rI.setQueryStruct(qs);
		rI.setColDataTypes(dataTypes);
		this.rScript = rI.composeQuery();
	}
	
	/**
	 * Need to take into consideration that our loading modifies the excels
	 * AND the user can modify those modifications
	 */
	public void composeRChangeHeaderNamesScript(Map<String, String> newHeaders) {
		CSVFileHelper helper = new CSVFileHelper();
		helper.parse(this.filePath);
		String[] autoCleanHeaders = helper.getAllCSVHeaders();
		
		StringBuilder changeHeaderNames = new StringBuilder();
		changeHeaderNames.append("c(");
		for(int i = 0; i < autoCleanHeaders.length; i++) {
			// autoCleanHeaders is our clean version of the header
			String newHeader = autoCleanHeaders[i];
			// map contains old csv header to user defined header
			if(newHeaders != null && newHeaders.containsKey(newHeader)) {
				newHeader = newHeaders.get(newHeader);
			}
			if( (i+1) == autoCleanHeaders.length ) {
				changeHeaderNames.append("\"").append(newHeader).append("\"");
			} else {
				changeHeaderNames.append("\"").append(newHeader).append("\", ");
			}
		}
		changeHeaderNames.append(")");
		this.modHeadersRVec = changeHeaderNames.toString();
		
		helper.clear();
	}
	
	public String getFilePath() {
		return this.filePath;
	}
	
	public String getRScript() {
		return this.rScript;
	}
	
	public String getModHeadersRVec() {
		return this.modHeadersRVec;
	}
	
	public String[] getHeaders() {
		return this.headersArr;
	}
	
	public String[] getTypes() {
		return this.dataTypesArr;
	}
	
	public Map<String, SemossDataType> getDataTypes() {
		return this.dataTypes;
	}

}
