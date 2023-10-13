package prerna.reactor.qs;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
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
		organizeKeys();
		//grab the query
		String query = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[0]));

		//create a new query struct
		HardSelectQueryStruct hardQs = null;
		if(this.qs instanceof HardSelectQueryStruct) {
			// we already have some form of a hard qs
			// so just use the existing one
			// and set the query
			hardQs = (HardSelectQueryStruct) this.qs;
			
		} else if(this.qs instanceof SelectQueryStruct) {
			SelectQueryStruct sQs = ((SelectQueryStruct) qs);
			
			if(sQs.getQsType() == QUERY_STRUCT_TYPE.ENGINE) {
				hardQs = new HardSelectQueryStruct();
				hardQs.setEngine(qs.getEngine());
				hardQs.setEngineId(qs.getEngineId());
				hardQs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
			} else {
				hardQs = new HardSelectQueryStruct();
				hardQs.setFrame(qs.getFrame());
				hardQs.setQsType(QUERY_STRUCT_TYPE.RAW_FRAME_QUERY);
			}
		}

		if (hardQs == null) {
			throw new NullPointerException("HardSelectQueryStruct hardQs should not be null here.");
		}

		hardQs.setQuery(query);
		//override it with new query struct
		hardQs.setBigDataEngine(this.qs.getBigDataEngine());
		hardQs.setPragmap(this.qs.getPragmap());
		this.qs = hardQs;

		return this.qs;
	}
	
}
