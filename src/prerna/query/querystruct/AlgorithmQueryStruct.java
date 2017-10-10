package prerna.query.querystruct;

import java.util.HashMap;
import java.util.Map;

public class AlgorithmQueryStruct extends QueryStruct2 {

	private Map<String, String> columnTypes = new HashMap<String, String>();

	public AlgorithmQueryStruct() {
		this.setQsType(QUERY_STRUCT_TYPE.ALGORITHM);
	}
	
	public void setColumnTypes(Map<String, String> columnTypes) {
		this.columnTypes = columnTypes;
	}
	
	public Map<String, String> getColumnTypes() {
		return this.columnTypes;
	}
	
	@Override
	public QueryStruct2 getNewBaseQueryStruct() {
		AlgorithmQueryStruct newQs = new AlgorithmQueryStruct();
		newQs.setQsType(this.getQsType());
		newQs.setColumnTypes(this.getColumnTypes());
		return newQs;
	}
}
