package prerna.sablecc2.reactor.qs.source;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class DatabaseReactor extends AbstractQueryStructReactor {

	public DatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		// get the selectors
		this.organizeKeys();
		String engineName = this.keyValue.get(this.keysToGet[0]);
		this.qs.setEngineName(engineName);
		// need to account if this is a hard query struct
		if(this.qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY || 
				this.qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		} else {
			this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
		}
		return this.qs;
	}
}
