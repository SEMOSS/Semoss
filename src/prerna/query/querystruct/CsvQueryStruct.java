package prerna.query.querystruct;

public class CsvQueryStruct extends AbstractFileQueryStruct {

	private char delimiter = ',';
	
	public CsvQueryStruct() {
		this.setQsType(QUERY_STRUCT_TYPE.CSV_FILE);
	}
	
	public char getDelimiter() {
		return delimiter;
	}
	
	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}
	
	@Override
	public SelectQueryStruct getNewBaseQueryStruct() {
		CsvQueryStruct newQs = new CsvQueryStruct();
		newQs.setQsType(this.getQsType());
		newQs.setFilePath(this.getFilePath());
		newQs.setColumnTypes(this.getColumnTypes());
		return newQs;
	}
}
