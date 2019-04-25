package prerna.sablecc2.reactor.qs.selectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
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
		
		// add the table in the list
		List<String> tables = new ArrayList<String>();
		tables.add(table);
		
		Map<String, List<String>> props = MasterDatabaseUtility.getConceptProperties(tables, appId);
		List<String> properties =  props.get(table);
		if (properties != null && !properties.isEmpty()) {
			for (String column : properties) {
				QueryColumnSelector qsSelector = new QueryColumnSelector();
				qsSelector.setTable(table);
				qsSelector.setColumn(column);
				qsSelector.setAlias(column);
				qs.addSelector(qsSelector);
			}
			
			// add prim key
			QueryColumnSelector qsSelector = new QueryColumnSelector();
			qsSelector.setTable(table);
			qsSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			qsSelector.setAlias(table);
			qs.addSelector(qsSelector);
		}
		
		return qs;
	}
}
