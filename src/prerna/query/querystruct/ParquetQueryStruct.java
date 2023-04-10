package prerna.query.querystruct;

public class ParquetQueryStruct extends AbstractFileQueryStruct {
	
	public ParquetQueryStruct() {
		this.setQsType(QUERY_STRUCT_TYPE.PARQUET_FILE);
	}
	
	@Override
	public SelectQueryStruct getNewBaseQueryStruct() {
		ParquetQueryStruct newQs = new ParquetQueryStruct();
		newQs.setQsType(this.getQsType());
		newQs.setFilePath(this.getFilePath());
		newQs.setColumnTypes(this.getColumnTypes());
		return newQs;
	}
}
