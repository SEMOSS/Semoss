package prerna.sablecc2.reactor.qs;

import prerna.query.interpreters.QueryStruct2;
import prerna.query.interpreters.QueryStructSelector;
import prerna.sablecc2.om.GenRowStruct;

public class SelectReactor extends QueryStructReactor {	
	
	QueryStruct2 createQueryStruct() {
		// get all the columns and query structs that are added into the cur row
		// example
		// select( studio, sum(mb) )
		// studio will be a column being added in
		// sum(mb) will be a query struct being merged

		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			//if the query struct is supposed to be used to query a database
			//selectors are as such:
			//concept is "concept"
			//property is "concept__property" where concept is the parent, property is the column name of the property
			for(int selectIndex = 0;selectIndex < qsInputs.size();selectIndex++) {
				Object newSelector = qsInputs.get(selectIndex);
				if(newSelector instanceof QueryStruct2) {
					mergeQueryStruct( (QueryStruct2) newSelector);
				} else if(newSelector instanceof String) {
					String thisSelector = newSelector + "";
					if(thisSelector.contains("__")){
						String[] selectorSplit = thisSelector.split("__");
						QueryStructSelector selector = getSelector(selectorSplit[0], selectorSplit[1]);
						qs.addSelector(selector);
					}
					else {
						QueryStructSelector selector = getSelector(thisSelector, null);
						qs.addSelector(selector);
					}
				} else {
					throw new IllegalArgumentException("ERROR!!! Invalid selector being sent");
				}
			}
		}


		return qs;
	}

	public QueryStructSelector getSelector(String table, String column) {
		QueryStructSelector selector = new QueryStructSelector();
		selector.setTable(table);
		if(column == null) {
			selector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
		} else {
			selector.setColumn(column);
		}
		return selector;
	}
}
