package prerna.query.querystruct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
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
import prerna.query.querystruct.selectors.QueryMathSelector;
import prerna.query.querystruct.selectors.QueryMultiColMathSelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

public class QueryStructConverter {

	private QueryStructConverter() {

	}

	public static QueryStruct2 getPhysicalQs(QueryStruct2 qs, OwlTemporalEngineMeta meta) {
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
			IQuerySelector convertedS = convertSelector(origS, meta);
			convertedSelectors.add(convertedS);
		}
		convertedQs.setSelectors(convertedSelectors);

		// now go through the filters
		List<IQueryFilter> origGrf = qs.getFilters().getFilters();
		if(origGrf != null && !origGrf.isEmpty()) {
			GenRowFilters convertedGrf = new GenRowFilters();
			for(int i = 0; i < origGrf.size(); i++) {
				convertedGrf.addFilters(convertFilter(origGrf.get(i), meta));
			}
			convertedQs.setFilters(convertedGrf);
		}
		
		// now go through the joins
		Map<String, Map<String, List>> joins = qs.getRelations();
		if(joins != null && !joins.isEmpty()) {
			Map<String, Map<String, List>> convertedJoins = convertJoins(joins, meta);
			convertedQs.setRelations(convertedJoins);
		}
		
		// now go through the group by
		List<QueryColumnSelector> origGroups = qs.getGroupBy();
		if(origGroups != null && !origGroups.isEmpty()) {
			List<QueryColumnSelector> convertedGroups =  new Vector<QueryColumnSelector>();
			for(int i = 0; i < origGroups.size(); i++) {
				IQuerySelector origGrupS = origGroups.get(i);
				QueryColumnSelector convertedGroupS = (QueryColumnSelector) convertSelector(origGrupS, meta);
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
				QueryColumnOrderBySelector convertedOrderByS = convertOrderBySelector(origOrderS, meta);
				convertedOrderBys.add(convertedOrderByS);
			}
			convertedQs.setOrderBy(convertedOrderBys);
		}
		
		return convertedQs;
	}

	public static Map<String, Map<String, List>> convertJoins(Map<String, Map<String, List>> joins, OwlTemporalEngineMeta meta) {
		Map<String, Map<String, List>> convertedJoins = new HashMap<String, Map<String, List>>();
		for(String startCol : joins.keySet()) {
			// grab the comp map before doing conversions
			Map<String, List> compMap = joins.get(startCol);
			
			// try to see if we can get a new start col
			String newStartCol = meta.getUniqueNameFromAlias(startCol);
			if(newStartCol == null) {
				newStartCol = startCol;
			}
			
			Map<String, List> convertedCompHash = new HashMap<String, List>();
			for(String comparator : compMap.keySet()) {
				List<String> endColList = compMap.get(comparator);
				List<String> convertedEndColList = new ArrayList<String>();
				
				for(String endCol : endColList) {
					// try to see if we can get a new end col
					String newEndCol = meta.getUniqueNameFromAlias(endCol);
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
	public static IQuerySelector convertSelector(IQuerySelector selector, OwlTemporalEngineMeta meta) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return convertConstantSelector((QueryConstantSelector) selector, meta);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return convertColumnSelector((QueryColumnSelector) selector, meta);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.MATH) {
			return convertMathSelector((QueryMathSelector) selector, meta);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.MULTI_MATH) {
			return convertMultiMathSelector((QueryMultiColMathSelector) selector, meta);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return convertArithmeticSelector((QueryArithmeticSelector) selector, meta);
		}
		return null;
	}

	private static IQuerySelector convertColumnSelector(QueryColumnSelector selector, OwlTemporalEngineMeta meta) {
		String newQsName = meta.getUniqueNameFromAlias(selector.getQueryStructName());
		if(newQsName == null) {
			// nothing to do
			// return the original
			return selector;
		}
		QueryColumnSelector newS = new QueryColumnSelector();
		if(newQsName.contains("__")) {
			String[] split = newQsName.split("__");
			newS.setTable(split[0]);
			newS.setColumn(split[1]);
		} else {
			newS.setTable(newQsName);
			newS.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
		}
		newS.setAlias(selector.getAlias());
		return newS;
	}

	private static IQuerySelector convertArithmeticSelector(QueryArithmeticSelector selector, OwlTemporalEngineMeta meta) {
		QueryArithmeticSelector newS = new QueryArithmeticSelector();
		newS.setLeftSelector(convertSelector(selector.getLeftSelector(), meta));
		newS.setRightSelector(convertSelector(selector.getRightSelector(), meta));
		newS.setMathExpr(selector.getMathExpr());
		newS.setAlias(selector.getAlias());
		return newS;
	}

	private static IQuerySelector convertMultiMathSelector(QueryMultiColMathSelector selector, OwlTemporalEngineMeta meta) {
		QueryMultiColMathSelector newS = new QueryMultiColMathSelector();
		for(IQuerySelector innerS : selector.getInnerSelector()) {
			newS.addInnerSelector(convertSelector(innerS, meta));
		}
		newS.setMath(selector.getMath());
		newS.setDistinct(selector.isDistinct());
		newS.setAlias(selector.getAlias());
		return newS;

	}

	private static IQuerySelector convertMathSelector(QueryMathSelector selector, OwlTemporalEngineMeta meta) {
		QueryMathSelector newS = new QueryMathSelector();
		newS.setInnerSelector(convertSelector(selector.getInnerSelector(), meta));
		newS.setMath(selector.getMath());
		newS.setDistinct(selector.isDistinct());
		newS.setAlias(selector.getAlias());
		return newS;
	}

	private static IQuerySelector convertConstantSelector(QueryConstantSelector selector, OwlTemporalEngineMeta meta) {
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
	public static QueryColumnOrderBySelector convertOrderBySelector(QueryColumnOrderBySelector selector, OwlTemporalEngineMeta meta) {
		String newQsName = meta.getUniqueNameFromAlias(selector.getQueryStructName());
		if(newQsName == null) {
			// nothing to do
			// return the original
			return selector;
		}
		QueryColumnOrderBySelector newS = new QueryColumnOrderBySelector();
		if(newQsName.contains("__")) {
			String[] split = newQsName.split("__");
			newS.setTable(split[0]);
			newS.setColumn(split[1]);
		} else {
			newS.setTable(newQsName);
			newS.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
		}
		newS.setSortDir(selector.getSortDirString());
		newS.setAlias(selector.getAlias());
		return newS;
	}

	/**
	 * Convert a filter
	 * Look at left hand side and right hand side
	 * If either is a column, try to convert
	 * @param queryFilter
	 * @param meta
	 * @return
	 */
	public static IQueryFilter convertFilter(IQueryFilter queryFilter, OwlTemporalEngineMeta meta) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, meta);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter, meta);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter, meta);
		} else {
			return null;
		}
	}
	
	private static IQueryFilter convertOrQueryFilter(OrQueryFilter queryFilter, OwlTemporalEngineMeta meta) {
		OrQueryFilter newF = new OrQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, meta));
		}
		return newF;
	}

	private static IQueryFilter convertAndQueryFilter(AndQueryFilter queryFilter, OwlTemporalEngineMeta meta) {
		AndQueryFilter newF = new AndQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, meta));
		}
		return newF;
	}

	private static SimpleQueryFilter convertSimpleQueryFilter(SimpleQueryFilter queryFilter, OwlTemporalEngineMeta meta) {
		NounMetadata newL = null;
		NounMetadata newR = null;

		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			String name = origL.getValue().toString();
			String newName = meta.getUniqueNameFromAlias(name);
			if(newName == null) {
				newName = name;
			}
			newL = new NounMetadata(newName, PixelDataType.COLUMN);
		} else {
			newL = origL;
		}
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			String name = origR.getValue().toString();
			String newName = meta.getUniqueNameFromAlias(name);
			if(newName == null) {
				newName = name;
			}
			newR = new NounMetadata(newName, PixelDataType.COLUMN);
		} else {
			newR = origR;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, queryFilter.getComparator(), newR);
		return newF;
	}
}
