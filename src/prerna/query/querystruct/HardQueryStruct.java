package prerna.query.querystruct;

public class HardQueryStruct extends QueryStruct2 {

	private String hardQuery;
	
	public HardQueryStruct() {
		
	}
	
	public void setQuery(String query) {
		this.hardQuery = query;
	}
	
	public String getQuery() {
		return this.hardQuery;
	}
	
	@Override
	public QueryStruct2 getNewBaseQueryStruct() {
		HardQueryStruct newQs = new HardQueryStruct();
		newQs.setQsType(getQsType());
		newQs.setEngineName(getEngineName());
		return newQs;
	}
}
