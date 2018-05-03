package prerna.query.querystruct;

import java.util.HashMap;
import java.util.Map;

public class LambdaQueryStruct extends SelectQueryStruct {

	private Map<String, String> columnTypes = new HashMap<String, String>();

	public LambdaQueryStruct() {
		this.setQsType(QUERY_STRUCT_TYPE.LAMBDA);
	}
	
	public void setColumnTypes(Map<String, String> columnTypes) {
		this.columnTypes = columnTypes;
	}
	
	public Map<String, String> getColumnTypes() {
		return this.columnTypes;
	}
	
	@Override
	public SelectQueryStruct getNewBaseQueryStruct() {
		LambdaQueryStruct newQs = new LambdaQueryStruct();
		newQs.setQsType(this.getQsType());
		newQs.setColumnTypes(this.getColumnTypes());
		return newQs;
	}
}
