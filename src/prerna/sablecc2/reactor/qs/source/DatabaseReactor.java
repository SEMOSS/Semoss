package prerna.sablecc2.reactor.qs.source;

import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class DatabaseReactor extends AbstractQueryStructReactor {

	public DatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	protected QueryStruct2 createQueryStruct() {
		//get the selectors
		String engineName = (String) this.curRow.get(0);
		this.qs.setEngineName(engineName);
		// need to account if this is a hard query struct
		if(this.qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY || 
				this.qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			this.qs.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		} else {
			this.qs.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.ENGINE);
		}
		return this.qs;
	}
}
