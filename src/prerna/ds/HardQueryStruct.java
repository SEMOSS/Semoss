package prerna.ds;

import prerna.ds.querystruct.QueryStruct2;

public class HardQueryStruct extends QueryStruct2{

	private String hardQuery;
	
	public HardQueryStruct() {
		
	}
	
	public void setQuery(String query) {
		this.hardQuery = query;
	}
	
	public String getQuery() {
		return this.hardQuery;
	}
}
