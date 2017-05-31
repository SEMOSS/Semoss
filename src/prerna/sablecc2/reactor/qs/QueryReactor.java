package prerna.sablecc2.reactor.qs;

import prerna.ds.querystruct.HardQueryStruct;
import prerna.ds.querystruct.QueryStruct2;

public class QueryReactor extends QueryStructReactor {
	
	@Override
	QueryStruct2 createQueryStruct() {
		
		//grab the query
		String query = (String)curRow.get(0);
		
		//create a new query struct
		HardQueryStruct hardQs = new HardQueryStruct();
		hardQs.setEngineName(qs.getEngineName());
		hardQs.setQuery(query);
		
		//override it with new query struct
		this.qs = hardQs;
		return this.qs;
	}
	
}
