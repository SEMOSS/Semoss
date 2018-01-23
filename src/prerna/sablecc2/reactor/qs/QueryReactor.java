package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.ReactorKeysEnum;

public class QueryReactor extends AbstractQueryStructReactor {
	
	public QueryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_KEY.getKey()};
	}
	
	@Override
	protected QueryStruct2 createQueryStruct() {
		//grab the query
		String query = (String) curRow.get(0);
		
		//create a new query struct
		HardQueryStruct hardQs = new HardQueryStruct();
		if(this.qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.ENGINE) {
			hardQs.setEngineName(qs.getEngineName());
			hardQs.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		} else {
			hardQs.setFrame(qs.getFrame());
			hardQs.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY);
		}
		hardQs.setQuery(query);
		
		//override it with new query struct
		this.qs = hardQs;
		return this.qs;
	}
	
}
