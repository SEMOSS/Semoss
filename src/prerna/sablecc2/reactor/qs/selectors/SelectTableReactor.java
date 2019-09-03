package prerna.sablecc2.reactor.qs.selectors;

import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class SelectTableReactor extends AbstractQueryStructReactor {	
	
	public SelectTableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TABLE.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();
		// must have used Database reactor before hand
		// so we know this must be the id
		String appId = qs.getEngineId();
		if(appId == null) {
			throw new IllegalArgumentException("Must define the app using Database(<input id here>) prior to SelectTable");
		}
		String table  = this.keyValue.get(ReactorKeysEnum.TABLE.getKey());
		
		List<String> selectors = MasterDatabaseUtility.getConceptPixelSelectors(table, appId);
		for(int i = 0; i < selectors.size(); i++) {
			QueryColumnSelector qsSelector = new QueryColumnSelector(selectors.get(i));
			qs.addSelector(qsSelector);
		}
		
		return qs;
	}
}
