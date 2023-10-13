package prerna.reactor.qs.selectors;

import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.ReactorKeysEnum;

public class SelectTableReactor extends AbstractQueryStructReactor {	
	
	public SelectTableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TABLE.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();
		// must have used Database reactor before hand
		// so we know this must be the id
		String databaseId = qs.getEngineId();
		if(databaseId == null) {
			throw new IllegalArgumentException("Must define the database using Database(<input id here>) prior to SelectTable");
		}
		String table  = this.keyValue.get(ReactorKeysEnum.TABLE.getKey());
		
		List<String> selectors = MasterDatabaseUtility.getConceptPixelSelectors(table, databaseId);
		for(int i = 0; i < selectors.size(); i++) {
			QueryColumnSelector qsSelector = new QueryColumnSelector(selectors.get(i));
			qs.addSelector(qsSelector);
		}
		
		return qs;
	}
}
