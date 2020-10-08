package prerna.sablecc2.reactor.qs.selectors;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskUtility;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class IfReactor extends AbstractQueryStructReactor {	
	
	// if else reactor typically has three building blocks into it
	// all of are query selectors
	// condition -  - this could be a query selector - mandatory - specifies what is the condition under which this is valid
	// then - the precedent - what happens when the condition is true - mandatory again
	// else - the antecedent - what happens if the condition is invalid. PLease note that these themselves could be other if reactors
	// the parent of if else is always a SelectReactor
	
	public IfReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	protected AbstractQueryStruct createQueryStruct() {
		
		// first one is the condition
		// second one is the precedent
		// third one is the antecedent
		QueryIfSelector qis = new QueryIfSelector();
		
		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			List<IQuerySelector> selectors = new Vector<IQuerySelector>();
			// there shoul be three here
			NounMetadata input = qsInputs.getNoun(0);
			IQuerySelector conditionSelector = getSelector(input);
			if(conditionSelector instanceof QueryConstantSelector)
			{
				IQueryFilter filter = (IQueryFilter)((QueryConstantSelector)conditionSelector).getConstant();
				qis.setCondition(filter);
			}
			NounMetadata input2 = qsInputs.getNoun(1);
			IQuerySelector precedent = getSelector(input2);
			qis.setPrecedent(precedent);

			NounMetadata input3 = qsInputs.getNoun(2);
			if(input3 != null)
			{
				IQuerySelector antecedent = getSelector(input3);
				qis.setAntecedent(antecedent);
			}
			selectors.add(qis);
			setAlias(selectors, this.selectorAlias, 0);
			qs.mergeSelectors(selectors);
		}
		
		if(qs.getPragmap() == null)
			qs.setPragmap(new java.util.HashMap());
		
		if(insight.getPragmap() != null)
			qs.getPragmap().putAll(insight.getPragmap());
		
		// convert this into one single query struct and then return it
		// first one is a filter query struct
		
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
		} else if(nounType == PixelDataType.FORMATTED_DATA_SET || nounType == PixelDataType.TASK) {
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
