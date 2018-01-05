package prerna.sablecc2.reactor.qs.selectors;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryAggregationEnum;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryMultiColMathSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class QuerySelectReactor extends AbstractQueryStructReactor {	
	
	public QuerySelectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	protected QueryStruct2 createQueryStruct() {
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
			return (IQuerySelector) input.getValue();
		} else {
			// we have a constant...
			QueryConstantSelector cSelect = new QueryConstantSelector();
			cSelect.setConstant(input.getValue());
			return cSelect;
		}
	}
	
	protected IQuerySelector genFunctionSelector(QueryAggregationEnum functionName, IQuerySelector innerSelector) {
		QueryMultiColMathSelector newSelector = new QueryMultiColMathSelector();
		newSelector.addInnerSelector(innerSelector);
		newSelector.setMath(functionName);
		return newSelector;
	}
}
