package prerna.engine.impl.vector;

import java.util.ArrayList;
import java.util.List;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class PGVectorQueryFitlerTranslationHelper {

	public static List<IQueryFilter> convertFilters(List<IQueryFilter> origFilters, String tableName) {
		if(origFilters != null && !origFilters.isEmpty()) {
			List<IQueryFilter> convertedFilters = new ArrayList<IQueryFilter>();
			for(int i = 0; i < origFilters.size(); i++) {
				convertedFilters.add(convertFilter(origFilters.get(i), tableName));
			}
			return convertedFilters;
		}
		// return the empty filters
		return origFilters;
	}

	/**
	 * Convert a filter
	 * Look at left hand side and right hand side
	 * If either is a column, try to convert
	 * @param queryFilter
	 * @param meta
	 * @return
	 */
	public static IQueryFilter convertFilter(IQueryFilter queryFilter, String tableName) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, tableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter, tableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter, tableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			return convertBetweenQueryFilter((BetweenQueryFilter) queryFilter, tableName);
		}else {
			return null;
		}
	}
	
	private static IQueryFilter convertOrQueryFilter(OrQueryFilter queryFilter, String tableName) {
		OrQueryFilter newF = new OrQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, tableName));
		}
		return newF;
	}

	private static IQueryFilter convertAndQueryFilter(AndQueryFilter queryFilter, String tableName) {
		AndQueryFilter newF = new AndQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, tableName));
		}
		return newF;
	}

	private static SimpleQueryFilter convertSimpleQueryFilter(SimpleQueryFilter queryFilter, String tableName) {
		NounMetadata newL = null;
		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newL = new NounMetadata( convertSelector((IQuerySelector) origL.getValue(), tableName) , PixelDataType.COLUMN);
		} 
		// Not going to handle a subquery against the pgvector at this point..
//		else if(origL.getNounType() == PixelDataType.QUERY_STRUCT) {
//			SelectQueryStruct newQs = getPhysicalQs((SelectQueryStruct) origL.getValue(), tableName);
//			newL = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
//		} 
		else {
			newL = origL;
		}
		
		NounMetadata newR = null;
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newR = new NounMetadata( convertSelector((IQuerySelector) origR.getValue(), tableName) , PixelDataType.COLUMN);
		} 
		// Not going to handle a subquery against the pgvector at this point..
//		else if(origR.getNounType() == PixelDataType.QUERY_STRUCT) {
//			SelectQueryStruct newQs = getPhysicalQs((SelectQueryStruct) origR.getValue(), tableName);
//			newR = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
//		} 
		else {
			newR = origR;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, queryFilter.getComparator(), newR);
		return newF;
	}
	
	private static BetweenQueryFilter convertBetweenQueryFilter(BetweenQueryFilter queryFilter, String tableName) {
		// need to convert column to the full name
		queryFilter.setColumn(convertSelector(queryFilter.getColumn(), tableName));
		return queryFilter;
	}
	
	/**
	 * Modify the selectors
	 * @param selector
	 * @return
	 */
	public static IQuerySelector convertSelector(IQuerySelector selector, String tableName) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return convertConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return convertColumnSelector((QueryColumnSelector) selector, tableName);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return convertFunctionSelector((QueryFunctionSelector) selector, tableName);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return convertArithmeticSelector((QueryArithmeticSelector) selector, tableName);
		}else if(selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE)
		{
			return convertIfElseSelector((QueryIfSelector)selector, tableName);
		}
		return null;
	}
	
	
	private static IQuerySelector convertIfElseSelector(QueryIfSelector selector, String tableName)
	{
		// get the condition first
		IQueryFilter condition = selector.getCondition();
		selector.setCondition(convertFilter(condition, tableName));
		
		// get the precedent
		IQuerySelector precedent = selector.getPrecedent();
		selector.setPrecedent(convertSelector(precedent, tableName));

		IQuerySelector antecedent = selector.getAntecedent();
		if(antecedent != null)
			selector.setAntecedent(convertSelector(antecedent, tableName));
		
		return selector;
	}
	

	private static IQuerySelector convertColumnSelector(QueryColumnSelector selector, String tableName) {
		String inputTable = selector.getTable();
		String inputColumn = selector.getColumn();
		
		if(inputColumn == null || inputColumn.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
			// this means the input table is actually the column
			return new QueryColumnSelector(tableName + "__" + inputTable);
		}
		return selector;
	}

	private static IQuerySelector convertArithmeticSelector(QueryArithmeticSelector selector, String tableName) {
		QueryArithmeticSelector newS = new QueryArithmeticSelector();
		newS.setLeftSelector(convertSelector(selector.getLeftSelector(), tableName));
		newS.setRightSelector(convertSelector(selector.getRightSelector(), tableName));
		newS.setMathExpr(selector.getMathExpr());
		newS.setAlias(selector.getAlias());
		return newS;
	}

	private static IQuerySelector convertFunctionSelector(QueryFunctionSelector selector, String tableName) {
		QueryFunctionSelector newS = new QueryFunctionSelector();
		for(IQuerySelector innerS : selector.getInnerSelector()) {
			newS.addInnerSelector(convertSelector(innerS, tableName));
		}
		newS.setFunction(selector.getFunction());
		newS.setDistinct(selector.isDistinct());
		newS.setAlias(selector.getAlias());
		newS.setAdditionalFunctionParams(selector.getAdditionalFunctionParams());
		return newS;

	}

	private static IQuerySelector convertConstantSelector(QueryConstantSelector selector) {
		// do nothing
		return selector;
	}
	
}
