package prerna.engine.impl.r;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.IMetaData;
import prerna.ds.QueryStruct;
import prerna.poi.main.helper.CSVFileHelper;

public class RFileWrapper {

	// set the filepath for the csv file
	private String filePath;
	
	// store the headers used
	private String[] headersArr;
	// store the data types for each header
	private String[] dataTypesArr;
	
	// store map of headers to data types
	private Map<String, IMetaData.DATA_TYPES> dataTypes;
	
	// store the produced query to execute in order to apply
	// the qs onto the dataframe
	private String rScript;
	
	// store the script to modify the header names
	private String modHeadersRVec;
	
	public RFileWrapper(String filePath) {
		this.filePath = filePath;
	}
	
	public void composeRScript(QueryStruct qs, Map<String, String> dataTypeMap, List<String> headerNames) {
		int size = dataTypeMap.keySet().size();
		headersArr = headerNames.toArray(new String[]{});
		dataTypesArr = new String[size];
		dataTypes = new Hashtable<String, IMetaData.DATA_TYPES>();
		
		int counter = 0;
		for(String header : headersArr) {
			dataTypesArr[counter] = dataTypeMap.get(header);
			dataTypes.put(header, IMetaData.convertToDataTypeEnum(dataTypesArr[counter]));
			
			counter++;
		}
		
		// the qs is passed from AbstractApiReactor
		RInterpreter rI = new RInterpreter();
		rI.setQueryStruct(qs);
		rI.setColDataTypes(dataTypes);
		this.rScript = rI.composeQuery();
	}
	

	public void composeRChangeHeaderNamesScript() {
		CSVFileHelper helper = new CSVFileHelper();
		helper.parse(this.filePath);
		String[] newHeaders = helper.getAllCSVHeaders();
		
		StringBuilder changeHeaderNames = new StringBuilder();
		changeHeaderNames.append("c(");
		for(int i = 0; i < newHeaders.length; i++) {
			if( (i+1) == newHeaders.length ) {
				changeHeaderNames.append("\"").append(newHeaders[i]).append("\"");
			} else {
				changeHeaderNames.append("\"").append(newHeaders[i]).append("\", ");
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
	
	public Map<String, IMetaData.DATA_TYPES> getDataTypes() {
		return this.dataTypes;
	}

}
