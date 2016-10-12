package prerna.engine.impl.r;

import java.util.Hashtable;
import java.util.Map;

import prerna.algorithm.api.IMetaData;
import prerna.ds.QueryStruct;

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
	
	public RFileWrapper(String filePath) {
		this.filePath = filePath;
	}
	
	public void composeRScript(QueryStruct qs, Map<String, String> dataTypeMap) {
		int size = dataTypeMap.keySet().size();
		headersArr = new String[size];
		dataTypesArr = new String[size];
		dataTypes = new Hashtable<String, IMetaData.DATA_TYPES>();
		
		int counter = 0;
		for(String header : dataTypeMap.keySet()) {
			headersArr[counter] = header;
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
	
	public String getFilePath() {
		return this.filePath;
	}
	
	public String getRScript() {
		return this.rScript;
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
