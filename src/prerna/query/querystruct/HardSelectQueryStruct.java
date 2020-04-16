package prerna.query.querystruct;

public class HardSelectQueryStruct extends SelectQueryStruct {

	private String hardQuery = null;
	
	// set new default for hard query struct
	{
		this.qsType = QUERY_STRUCT_TYPE.RAW_FRAME_QUERY;
	}
	
	public HardSelectQueryStruct() {
		
	}
	
	public void setQuery(String query) {
		this.hardQuery = query;
	}
	
	public String getQuery() {
		return this.hardQuery;
	}
	
	@Override
	public SelectQueryStruct getNewBaseQueryStruct() {
		HardSelectQueryStruct newQs = new HardSelectQueryStruct();
		newQs.setQsType(getQsType());
		newQs.setEngineId(getEngineId());
		return newQs;
	}

}
