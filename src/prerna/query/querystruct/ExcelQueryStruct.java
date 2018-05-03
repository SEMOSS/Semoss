package prerna.query.querystruct;

import java.util.HashMap;
import java.util.Map;

public class ExcelQueryStruct extends SelectQueryStruct {

	private String excelFilePath;
	private Map<String, String> columnTypes = new HashMap<String, String>();
	private Map<String, String> newHeaderNames = new HashMap<String, String>();
	private String sheetName;
	
	public ExcelQueryStruct() {
		this.setQsType(QUERY_STRUCT_TYPE.EXCEL_FILE);
	}
	
	public String getExcelFilePath() {
		return excelFilePath;
	}

	public void setExcelFilePath(String excelFilePath) {
		this.excelFilePath = excelFilePath;
	}

	public Map<String, String> getColumnTypes() {
		return columnTypes;
	}

	public void setColumnTypes(Map<String, String> columnTypes) {
		this.columnTypes = columnTypes;
	}

	public Map<String, String> getNewHeaderNames() {
		return newHeaderNames;
	}

	public void setNewHeaderNames(Map<String, String> newHeaderNames) {
		this.newHeaderNames = newHeaderNames;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}
	
	@Override
	public SelectQueryStruct getNewBaseQueryStruct() {
		ExcelQueryStruct newQs = new ExcelQueryStruct();
		newQs.setQsType(this.getQsType());
		newQs.setExcelFilePath(this.getExcelFilePath());
		newQs.setColumnTypes(this.getColumnTypes());
		newQs.setSheetName(this.getSheetName());
		return newQs;
	}
}
