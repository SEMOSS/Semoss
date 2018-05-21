package prerna.query.querystruct;

public class ExcelQueryStruct extends AbstractFileQueryStruct {

	private String sheetName;
	
	public ExcelQueryStruct() {
		this.setQsType(QUERY_STRUCT_TYPE.EXCEL_FILE);
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
		newQs.setFilePath(this.getFilePath());
		newQs.setColumnTypes(this.getColumnTypes());
		newQs.setSheetName(this.getSheetName());
		return newQs;
	}
}
