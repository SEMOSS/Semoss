package prerna.sablecc2.reactor.qs.selectors;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class QuerySelectorPathReactor extends AbstractQueryStructReactor {	
	
	/**
	 * This class is meant to be used for getting a query column selector
	 * When we have a path that we are trying to get 
	 * Example : JSON parsing
	 */
	
	
	public QuerySelectorPathReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey()};
	}
	
	protected AbstractQueryStruct createQueryStruct() {
		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			List<IQuerySelector> selectors = new Vector<IQuerySelector>();
			for(int selectIndex = 0;selectIndex < qsInputs.size();selectIndex++) {
				NounMetadata input = qsInputs.getNoun(selectIndex);
				IQuerySelector selector = getSelector(input);
				if(selector != null) {
					selectors.add(selector);
				}
			}
			setAlias(selectors, this.selectorAlias, 0);
			qs.mergeSelectors(selectors);
		}
		return qs;
	}

	private IQuerySelector getSelector(NounMetadata input) {
		QueryColumnSelector c = new QueryColumnSelector();
		c.setTable("PATH");
		c.setColumn(input.getValue().toString());
		return c;
	}
	
}