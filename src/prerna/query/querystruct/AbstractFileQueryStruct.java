package prerna.query.querystruct;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractFileQueryStruct extends SelectQueryStruct {

	public enum ORIG_SOURCE {FILE_UPLOAD, API_CALL}
	
	protected ORIG_SOURCE source = ORIG_SOURCE.FILE_UPLOAD;

	protected String filePath;
	protected Map<String, String> newHeaderNames = new HashMap<String, String>();
	protected Map<String, String> columnTypes = new HashMap<String, String>();
	protected Map<String, String> additionalTypes = new HashMap<String, String>();
	
	/**
	 * Utility to set the column types for a file
	 * @param colNames
	 * @param colTypes
	 */
	public void setSelectorsAndTypes(String[] colNames, String[] colTypes) {
		this.columnTypes = new HashMap<String, String>();
		int numCols = colNames.length;
		for(int i = 0; i < numCols; i++) {
			this.addSelector("FILE", colNames[i]);
			this.columnTypes.put(colNames[i], colTypes[i]);
		}
	}
	
	/*
	 * Setters and Getters
	 */
	
	public ORIG_SOURCE getSource() {
		return source;
	}
	
	public void setSource(ORIG_SOURCE source) {
		this.source = source;
	}
	
	public String getFilePath() {
		return filePath;
	}
	
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	public Map<String, String> getNewHeaderNames() {
		return newHeaderNames;
	}
	
	public void setNewHeaderNames(Map<String, String> newHeaderNames) {
		this.newHeaderNames = newHeaderNames;
	}

	public Map<String, String> getColumnTypes() {
		return columnTypes;
	}
	
	public void setColumnTypes(Map<String, String> columnTypes) {
		this.columnTypes = columnTypes;
	}
	
	public Map<String, String> getAdditionalTypes() {
		return additionalTypes;
	}
	
	public void setAdditionalTypes(Map<String, String> additionalTypes) {
		this.additionalTypes = additionalTypes;
	}
	
	@Override
	public Map<String, String> getSourceMap() {
		Map<String, String> sourceMap = new HashMap<String, String>();
		sourceMap.put("type", this.qsType.toString());
		sourceMap.put("id", this.filePath);
		return sourceMap;
	}
}
