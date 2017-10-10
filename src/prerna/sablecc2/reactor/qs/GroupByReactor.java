package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.QueryStruct2;

public class GroupByReactor extends QueryStructReactor {

	QueryStruct2 createQueryStruct() {
		int size = curRow.size();
		for(int selectIndex = 0;selectIndex < size; selectIndex++) {
			Object newSelector = curRow.get(selectIndex);
			String thisSelector = newSelector + "";
			if(thisSelector.contains("__")) {
				String[] selectorSplit = thisSelector.split("__");
				qs.addGroupBy(selectorSplit[0], selectorSplit[1]);
			} else {
				qs.addGroupBy(thisSelector, QueryStruct2.PRIM_KEY_PLACEHOLDER);
			}
		}
		return qs;
	}
}
