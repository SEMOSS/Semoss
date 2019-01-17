package prerna.sablecc2.reactor.qs.selectors;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskUtility;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class QuerySelectReactor extends AbstractQueryStructReactor {	
	
	public QuerySelectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
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
	
	protected IQuerySelector getSelector(NounMetadata input) {
		PixelDataType nounType = input.getNounType();
		if(nounType == PixelDataType.QUERY_STRUCT) {
			// remember, if it is an embedded selector
			// we return a full QueryStruct even if it has just one selector
			// inside of it
			SelectQueryStruct qs = (SelectQueryStruct) input.getValue();
			List<IQuerySelector> selectors = qs.getSelectors();
			if(selectors.isEmpty()) {
				// umm... merge the other QS stuff
				qs.merge(qs);
				return null;
			}
			return selectors.get(0);
		} else if(nounType == PixelDataType.COLUMN) {
			return (IQuerySelector) input.getValue();
		} else if(nounType == PixelDataType.FORMATTED_DATA_SET) {
			Object value = input.getValue();
			NounMetadata formatData = TaskUtility.getTaskDataScalarElement(value);
			if(formatData == null) {
				throw new IllegalArgumentException("Can only handle query data that is a scalar input");
			} else {
				Object newValue = formatData.getValue();
				QueryConstantSelector cSelect = new QueryConstantSelector();
				cSelect.setConstant(newValue);
				return cSelect;
			}
		}
		else {
			// we have a constant...
			QueryConstantSelector cSelect = new QueryConstantSelector();
			cSelect.setConstant(input.getValue());
			return cSelect;
		}
	}

	protected QueryFunctionSelector genFunctionSelector(String functionName, IQuerySelector innerSelector) {
		return genFunctionSelector(functionName, innerSelector, false);
	}
	
	protected QueryFunctionSelector genFunctionSelector(String functionName, IQuerySelector innerSelector, boolean isDistinct) {
		QueryFunctionSelector newSelector = new QueryFunctionSelector();
		newSelector.addInnerSelector(innerSelector);
		newSelector.setFunction(functionName);
		newSelector.setDistinct(isDistinct);
		return newSelector;
	}
	
	protected QueryFunctionSelector genFunctionSelector(String functionName, List<IQuerySelector> innerSelectors) {
		return genFunctionSelector(functionName, innerSelectors, false);
	}
	
	protected QueryFunctionSelector genFunctionSelector(String functionName, List<IQuerySelector> innerSelectors, boolean isDistinct) {
		QueryFunctionSelector newSelector = new QueryFunctionSelector();
		newSelector.setInnerSelector(innerSelectors);
		newSelector.setFunction(functionName);
		newSelector.setDistinct(isDistinct);
		return newSelector;
	}
}
