package prerna.sablecc2.reactor.qs;

import java.util.List;
import java.util.Vector;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class SelectReactor extends QueryStructReactor {	
	
	public SelectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	QueryStruct2 createQueryStruct() {
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
			setAlias(selectors, this.selectorAlias);
			qs.mergeSelectors(selectors);
		}
		return qs;
	}
	
	protected IQuerySelector getSelector(NounMetadata input) {
		PixelDataType nounType = input.getNounType();
		if(nounType == PixelDataType.QUERY_STRUCT) {
			// remember, if it is an embedded selector
			// we return a full QueryStruct even if it has just one selector
			// inside of it
			QueryStruct2 qs = (QueryStruct2) input.getValue();
			List<IQuerySelector> selectors = qs.getSelectors();
			if(selectors.isEmpty()) {
				// umm... merge the other QS stuff
				qs.merge(qs);
				return null;
			}
			return selectors.get(0);
		} else if(nounType == PixelDataType.COLUMN) {
			String thisSelector = input.getValue() + "";
			if(thisSelector.contains("__")){
				String[] selectorSplit = thisSelector.split("__");
				return getColumnSelector(selectorSplit[0], selectorSplit[1]);
			}
			else {
				return getColumnSelector(thisSelector, null);
			}
		} else {
			// we have a constant...
			QueryConstantSelector cSelect = new QueryConstantSelector();
			cSelect.setConstant(input.getValue());
			return cSelect;
		}
	}

	protected IQuerySelector getColumnSelector(String table, String column) {
		QueryColumnSelector selector = new QueryColumnSelector();
		selector.setTable(table);
		if(column == null) {
			selector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
		} else {
			selector.setColumn(column);
		}
		return selector;
	}
}
