package prerna.query.querystruct.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QSRenameColumnConverter {
	
	private QSRenameColumnConverter() {

	}

	public static QueryStruct2 convertSelector(QueryStruct2 qs, Map<String, String> transformationMap) {
		if(qs instanceof HardQueryStruct) {
			return qs;
		}
		// need to modify and re-add all the selectors
		QueryStruct2 convertedQs = qs.getNewBaseQueryStruct();
		convertedQs.setLimit(qs.getLimit());
		convertedQs.setOffSet(qs.getOffset());

		// grab all the selectors
		// and need to recursively modify the column ones
		List<IQuerySelector> origSelectors = qs.getSelectors();
		List<IQuerySelector> convertedSelectors = new Vector<IQuerySelector>();
		for(int i = 0; i < origSelectors.size(); i++) {
			IQuerySelector origS = origSelectors.get(i);
			IQuerySelector convertedS = convertSelector(origS, transformationMap);
			convertedSelectors.add(convertedS);
		}
		convertedQs.setSelectors(convertedSelectors);

		// now go through the filters
		convertedQs.setImplicitFilters(convertGenRowFilters(qs.getImplicitFilters(), transformationMap));
		convertedQs.setExplicitFilters(convertGenRowFilters(qs.getExplicitFilters(), transformationMap));
		convertedQs.setHavingFilters(convertGenRowFilters(qs.getHavingFilters(), transformationMap));

		// now go through the joins
		Map<String, Map<String, List>> joins = qs.getRelations();
		if(joins != null && !joins.isEmpty()) {
			Map<String, Map<String, List>> convertedJoins = convertJoins(joins, transformationMap);
			convertedQs.setRelations(convertedJoins);
		}
		
		// now go through the group by
		List<QueryColumnSelector> origGroups = qs.getGroupBy();
		if(origGroups != null && !origGroups.isEmpty()) {
			List<QueryColumnSelector> convertedGroups =  new Vector<QueryColumnSelector>();
			for(int i = 0; i < origGroups.size(); i++) {
				IQuerySelector origGrupS = origGroups.get(i);
				QueryColumnSelector convertedGroupS = (QueryColumnSelector) convertSelector(origGrupS, transformationMap);
				convertedGroups.add(convertedGroupS);
			}
			convertedQs.setGroupBy(convertedGroups);
		}
		
		// now go through the order by
		List<QueryColumnOrderBySelector> origOrders = qs.getOrderBy();
		if(origOrders != null && !origOrders.isEmpty()) {
			List<QueryColumnOrderBySelector> convertedOrderBys =  new Vector<QueryColumnOrderBySelector>();
			for(int i = 0; i < origOrders.size(); i++) {
				QueryColumnOrderBySelector origOrderS = origOrders.get(i);
				QueryColumnOrderBySelector convertedOrderByS = convertOrderBySelector(origOrderS, transformationMap);
				convertedOrderBys.add(convertedOrderByS);
			}
			convertedQs.setOrderBy(convertedOrderBys);
		}
		
		return convertedQs;
	}

	public static Map<String, Map<String, List>> convertJoins(Map<String, Map<String, List>> joins, Map<String, String> transformationMap) {
		Map<String, Map<String, List>> convertedJoins = new HashMap<String, Map<String, List>>();
		for(String startCol : joins.keySet()) {
			// grab the comp map before doing conversions
			Map<String, List> compMap = joins.get(startCol);
			
			// try to see if we can get a new start col
			String newStartCol = transformationMap.get(startCol);
			if(newStartCol == null) {
				newStartCol = startCol;
			}
			
			Map<String, List> convertedCompHash = new HashMap<String, List>();
			for(String comparator : compMap.keySet()) {
				List<String> endColList = compMap.get(comparator);
				List<String> convertedEndColList = new ArrayList<String>();
				
				for(String endCol : endColList) {
					// try to see if we can get a new end col
					String newEndCol = transformationMap.get(endCol);
					if(newEndCol == null) {
						newEndCol = endCol;
					}
					convertedEndColList.add(newEndCol);
				}
				
				// add to comp hash
				convertedCompHash.put(comparator, convertedEndColList);
			}
			
			// add to final map
			convertedJoins.put(newStartCol, convertedCompHash);
		}
		
		return convertedJoins;
	}

	/**
	 * Modify the selectors
	 * @param selector
	 * @return
	 */
	public static IQuerySelector convertSelector(IQuerySelector selector, Map<String, String> transformationMap) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return convertConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return convertColumnSelector((QueryColumnSelector) selector, transformationMap);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return convertFunctionSelector((QueryFunctionSelector) selector, transformationMap);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return convertArithmeticSelector((QueryArithmeticSelector) selector, transformationMap);
		}
		return null;
	}

	private static IQuerySelector convertColumnSelector(QueryColumnSelector selector, Map<String, String> transformationMap) {
		String alias = selector.getAlias();
		if(transformationMap.containsKey(alias)) {
			// we need to switch
			String newAlias = transformationMap.get(alias);
			QueryColumnSelector newS = new QueryColumnSelector(newAlias);
			newS.setAlias(newAlias);
			newS.setTableAlias(selector.getTableAlias());
			return newS;
		}
		
		// dont need to switch
		// just return the orig column
		return selector;
	}

	private static IQuerySelector convertArithmeticSelector(QueryArithmeticSelector selector, Map<String, String> transformationMap) {
		QueryArithmeticSelector newS = new QueryArithmeticSelector();
		newS.setLeftSelector(convertSelector(selector.getLeftSelector(), transformationMap));
		newS.setRightSelector(convertSelector(selector.getRightSelector(), transformationMap));
		newS.setMathExpr(selector.getMathExpr());
		newS.setAlias(selector.getAlias());
		return newS;
	}

	private static IQuerySelector convertFunctionSelector(QueryFunctionSelector selector, Map<String, String> transformationMap) {
		QueryFunctionSelector newS = new QueryFunctionSelector();
		for(IQuerySelector innerS : selector.getInnerSelector()) {
			newS.addInnerSelector(convertSelector(innerS, transformationMap));
		}
		newS.setFunction(selector.getFunction());
		newS.setDistinct(selector.isDistinct());
		newS.setAlias(selector.getAlias());
		return newS;

	}

	private static IQuerySelector convertConstantSelector(QueryConstantSelector selector) {
		// do nothing
		return selector;
	}
	
	/**
	 * Convert an order by selector
	 * Same as conversion of a column selector, but adding the sort direction
	 * @param selector
	 * @param meta
	 * @return
	 */
	public static QueryColumnOrderBySelector convertOrderBySelector(QueryColumnOrderBySelector selector, Map<String, String> transformationMap) {
		String newAlias = transformationMap.get(selector.getAlias());
		if(newAlias == null) {
			// nothing to do
			// return the original
			return selector;
		}
		QueryColumnOrderBySelector newS = new QueryColumnOrderBySelector(newAlias);
		newS.setSortDir(selector.getSortDirString());
		newS.setAlias(selector.getAlias());
		return newS;
	}
	
	public static GenRowFilters convertGenRowFilters(GenRowFilters grs, Map<String, String> transformationMap) {
		List<IQueryFilter> origGrf = grs.getFilters();
		if(origGrf != null && !origGrf.isEmpty()) {
			GenRowFilters convertedGrf = new GenRowFilters();
			for(int i = 0; i < origGrf.size(); i++) {
				convertedGrf.addFilters(convertFilter(origGrf.get(i), transformationMap));
			}
			return convertedGrf;
		}
		// return the empty grs
		return grs;
	}

	/**
	 * Convert a filter
	 * Look at left hand side and right hand side
	 * If either is a column, try to convert
	 * @param queryFilter
	 * @param meta
	 * @return
	 */
	public static IQueryFilter convertFilter(IQueryFilter queryFilter, Map<String, String> transformationMap) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, transformationMap);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter, transformationMap);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter, transformationMap);
		} else {
			return null;
		}
	}
	
	private static IQueryFilter convertOrQueryFilter(OrQueryFilter queryFilter, Map<String, String> transformationMap) {
		OrQueryFilter newF = new OrQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, transformationMap));
		}
		return newF;
	}

	private static IQueryFilter convertAndQueryFilter(AndQueryFilter queryFilter, Map<String, String> transformationMap) {
		AndQueryFilter newF = new AndQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, transformationMap));
		}
		return newF;
	}

	private static SimpleQueryFilter convertSimpleQueryFilter(SimpleQueryFilter queryFilter, Map<String, String> transformationMap) {
		NounMetadata newL = null;
		NounMetadata newR = null;

		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newL = new NounMetadata( convertSelector((IQuerySelector) origL.getValue(), transformationMap) , PixelDataType.COLUMN);
		} else {
			newL = origL;
		}
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newR = new NounMetadata( convertSelector((IQuerySelector) origR.getValue(), transformationMap) , PixelDataType.COLUMN);
		} else {
			newR = origR;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, queryFilter.getComparator(), newR);
		return newF;
	}
}
