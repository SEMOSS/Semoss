package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class GroupReactor extends AbstractQueryStructReactor {
	
	public GroupReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}

	protected AbstractQueryStruct createQueryStruct() {
		int size = curRow.size();
		for(int selectIndex = 0;selectIndex < size; selectIndex++) {
			Object newSelector = curRow.get(selectIndex);
			String thisSelector = newSelector + "";
			if(thisSelector.contains("__")) {
				String[] selectorSplit = thisSelector.split("__");
				((SelectQueryStruct) qs).addGroupBy(selectorSplit[0], selectorSplit[1]);
			} else {
				((SelectQueryStruct) qs).addGroupBy(thisSelector, SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
		}
		return qs;
	}
}
