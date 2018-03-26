package prerna.query.querystruct;

import java.util.HashMap;
import java.util.Map;

public class CsvQueryStruct extends QueryStruct2 {

	public enum ORIG_SOURCE {FILE_UPLOAD, API_CALL}
	
	private ORIG_SOURCE source = ORIG_SOURCE.FILE_UPLOAD;
	private String csvFilePath;
	private Map<String, String> columnTypes = new HashMap<String, String>();
	private Map<String, String> newHeaderNames = new HashMap<String, String>();
	private char delimiter = ',';
	
	public CsvQueryStruct() {
		this.setQsType(QUERY_STRUCT_TYPE.CSV_FILE);
	}
	
	public String getCsvFilePath() {
		return csvFilePath;
	}

	public void setCsvFilePath(String csvFilePath) {
		this.csvFilePath = csvFilePath;
	}
	
	public void setSource(ORIG_SOURCE source) {
		this.source = source;
	}
	
	public ORIG_SOURCE getSource() {
		return this.source;
	}
	
	public void setSelectorsAndTypes(String[] colNames, String[] colTypes) {
		this.columnTypes = new HashMap<String, String>();
		int numCols = colNames.length;
		for(int i = 0; i < numCols; i++) {
			this.addSelector("CSVFile", colNames[i]);
			this.columnTypes.put(colNames[i], colTypes[i]);
		}
	}
	
	public void setColumnTypes(Map<String, String> columnTypes) {
		this.columnTypes = columnTypes;
	}
	
	public Map<String, String> getColumnTypes() {
		return this.columnTypes;
	}
	
	public Map<String, String> getNewHeaderNames() {
		return newHeaderNames;
	}

	public void setNewHeaderNames(Map<String, String> newHeaderNames) {
		this.newHeaderNames = newHeaderNames;
	}

	public char getDelimiter() {
		return delimiter;
	}
	
	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}
	
	@Override
	public QueryStruct2 getNewBaseQueryStruct() {
		CsvQueryStruct newQs = new CsvQueryStruct();
		newQs.setQsType(this.getQsType());
		newQs.setCsvFilePath(this.getCsvFilePath());
		newQs.setColumnTypes(this.getColumnTypes());
		return newQs;
	}
}
