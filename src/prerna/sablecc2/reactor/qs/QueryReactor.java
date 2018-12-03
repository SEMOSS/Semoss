package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Utility;

public class QueryReactor extends AbstractQueryStructReactor {
	
	public QueryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_KEY.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		//grab the query
		String query = Utility.decodeURIComponent(this.curRow.get(0).toString());

		//create a new query struct
		HardSelectQueryStruct hardQs = new HardSelectQueryStruct();
		if(this.qs instanceof SelectQueryStruct) {
			SelectQueryStruct sQs = ((SelectQueryStruct) qs);
			if(sQs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
				hardQs.setEngine(qs.getEngine());
				hardQs.setEngineId(qs.getEngineId());
				hardQs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
			} else {
				hardQs.setFrame(qs.getFrame());
				hardQs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY);
			}
		}
		hardQs.setQuery(query);
		
		//override it with new query struct
		this.qs = hardQs;
		return this.qs;
	}
	
}
