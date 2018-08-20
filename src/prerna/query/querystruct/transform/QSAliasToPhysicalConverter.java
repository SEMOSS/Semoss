package prerna.query.querystruct.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
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
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.gson.GsonUtility;

public class QSAliasToPhysicalConverter {

	private QSAliasToPhysicalConverter() {

	}
	
	public static AbstractQueryStruct getPhysicalQs(AbstractQueryStruct qs, OwlTemporalEngineMeta meta) {
		if(qs instanceof SelectQueryStruct) {
			return getPhysicalQs((SelectQueryStruct) qs, meta);
		} else if(qs instanceof UpdateQueryStruct) {
			return getPhysicalQs((UpdateQueryStruct) qs, meta);
		}
		
		return qs;
	}

	public static UpdateQueryStruct getPhysicalQs(UpdateQueryStruct qs, OwlTemporalEngineMeta meta) {
		// need to modify and re-add all the selectors
		UpdateQueryStruct convertedQs = qs.getNewBaseQueryStruct();

		// grab all the selectors
		// and need to recursively modify the column ones
		Map<String, IQuerySelector> aliases = new HashMap<String, IQuerySelector>();
		List<IQuerySelector> origSelectors = qs.getSelectors();
		List<IQuerySelector> convertedSelectors = new Vector<IQuerySelector>();
		for(int i = 0; i < origSelectors.size(); i++) {
			IQuerySelector origS = origSelectors.get(i);
			IQuerySelector convertedS = convertSelector(origS, meta);
			convertedSelectors.add(convertedS);
			aliases.put(convertedS.getAlias(), convertedS);
		}
		convertedQs.setSelectors(convertedSelectors);

		// now go through the filters
		convertedQs.setImplicitFilters(convertGenRowFilters(qs.getImplicitFilters(), meta));
		convertedQs.setExplicitFilters(convertGenRowFilters(qs.getExplicitFilters(), meta));
		convertedQs.setHavingFilters(convertHavingGenRowFilters(qs.getHavingFilters(), meta, aliases));

		// now go through the joins
		Map<String, Map<String, List>> joins = qs.getRelations();
		if(joins != null && !joins.isEmpty()) {
			Map<String, Map<String, List>> convertedJoins = convertJoins(joins, meta);
			convertedQs.setRelations(convertedJoins);
		}
		
		return convertedQs;
	}
	
	public static SelectQueryStruct getPhysicalQs(SelectQueryStruct qs, OwlTemporalEngineMeta meta) {
		if(qs instanceof HardSelectQueryStruct) {
			return qs;
		}
		// need to modify and re-add all the selectors
		SelectQueryStruct convertedQs = qs.getNewBaseQueryStruct();
		convertedQs.setLimit(qs.getLimit());
		convertedQs.setOffSet(qs.getOffset());

		// grab all the selectors
		// and need to recursively modify the column ones
		Map<String, IQuerySelector> aliases = new HashMap<String, IQuerySelector>();
		List<IQuerySelector> origSelectors = qs.getSelectors();
		List<IQuerySelector> convertedSelectors = new Vector<IQuerySelector>();
		for(int i = 0; i < origSelectors.size(); i++) {
			IQuerySelector origS = origSelectors.get(i);
			IQuerySelector convertedS = convertSelector(origS, meta);
			convertedSelectors.add(convertedS);
			aliases.put(convertedS.getAlias(), convertedS);
		}
		convertedQs.setSelectors(convertedSelectors);

		// now go through the filters
		convertedQs.setImplicitFilters(convertGenRowFilters(qs.getImplicitFilters(), meta));
		convertedQs.setExplicitFilters(convertGenRowFilters(qs.getExplicitFilters(), meta));
		convertedQs.setHavingFilters(convertHavingGenRowFilters(qs.getHavingFilters(), meta, aliases));

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
				IQuerySelector origGroupS = origGroups.get(i);
				QueryColumnSelector convertedGroupS = (QueryColumnSelector) convertSelector(origGroupS, meta);
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
			return convertConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return convertColumnSelector((QueryColumnSelector) selector, meta);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return convertFunctionSelector((QueryFunctionSelector) selector, meta);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return convertArithmeticSelector((QueryArithmeticSelector) selector, meta);
		}
		return null;
	}

	private static IQuerySelector convertColumnSelector(QueryColumnSelector selector, OwlTemporalEngineMeta meta) {
		String qsName = selector.getQueryStructName();
		String newQsName = meta.getUniqueNameFromAlias(qsName);
		if(newQsName == null) {
			// see if it is a jsonified selector
			IQuerySelector jSelector = convertJsonifiedSelector(qsName, meta);
			if(jSelector != null) {
				return jSelector;
			}
			// this should be the physical name
			// let us make sure and validate it
			boolean isValid = meta.validateUniqueName(qsName);
			if(!isValid) {
				throw new IllegalArgumentException("Cannot find header for column input = " + qsName);
			}
			return selector;
		}
		
		// see if it is a jsonified selector
		IQuerySelector jSelector = convertJsonifiedSelector(newQsName, meta);
		if(jSelector != null) {
			return jSelector;
		}
					
		// try to see if it is jsonified selector
		QueryColumnSelector newS = new QueryColumnSelector();
		if(newQsName.contains("__")) {
			String[] split = newQsName.split("__");
			newS.setTable(split[0]);
			newS.setColumn(split[1]);
		} else {
			newS.setTable(newQsName);
			newS.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
		}
		newS.setAlias(selector.getAlias());
		newS.setTableAlias(selector.getTableAlias());
		return newS;
	}

	private static IQuerySelector convertJsonifiedSelector(String uniqueName, OwlTemporalEngineMeta meta) {
		Object[] selectorData = meta.getComplexSelector(uniqueName);
		if(selectorData != null) {
			// position 1 is the query type
			// position 2 is the json
			IQuerySelector.SELECTOR_TYPE type = IQuerySelector.convertStringToSelectorType(selectorData[1].toString());
			IQuerySelector selector = (IQuerySelector) GsonUtility.getDefaultGson().fromJson(selectorData[2].toString(), IQuerySelector.getQuerySelectorClassFromType(type));
			return selector;
		}
		
		return null;
	}
	
	private static IQuerySelector convertArithmeticSelector(QueryArithmeticSelector selector, OwlTemporalEngineMeta meta) {
		QueryArithmeticSelector newS = new QueryArithmeticSelector();
		newS.setLeftSelector(convertSelector(selector.getLeftSelector(), meta));
		newS.setRightSelector(convertSelector(selector.getRightSelector(), meta));
		newS.setMathExpr(selector.getMathExpr());
		newS.setAlias(selector.getAlias());
		return newS;
	}

	private static IQuerySelector convertFunctionSelector(QueryFunctionSelector selector, OwlTemporalEngineMeta meta) {
		QueryFunctionSelector newS = new QueryFunctionSelector();
		for(IQuerySelector innerS : selector.getInnerSelector()) {
			newS.addInnerSelector(convertSelector(innerS, meta));
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
			newS.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
		}
		newS.setSortDir(selector.getSortDirString());
		newS.setAlias(selector.getAlias());
		return newS;
	}
	
	public static GenRowFilters convertGenRowFilters(GenRowFilters grs, OwlTemporalEngineMeta meta) {
		List<IQueryFilter> origGrf = grs.getFilters();
		if(origGrf != null && !origGrf.isEmpty()) {
			GenRowFilters convertedGrf = new GenRowFilters();
			for(int i = 0; i < origGrf.size(); i++) {
				convertedGrf.addFilters(convertFilter(origGrf.get(i), meta));
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
			newL = new NounMetadata( convertSelector((IQuerySelector) origL.getValue(), meta) , PixelDataType.COLUMN);
		} else {
			newL = origL;
		}
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newR = new NounMetadata( convertSelector((IQuerySelector) origR.getValue(), meta) , PixelDataType.COLUMN);
		} else {
			newR = origR;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, queryFilter.getComparator(), newR);
		return newF;
	}
	
	private static GenRowFilters convertHavingGenRowFilters(GenRowFilters grs, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases) {
		List<IQueryFilter> origGrf = grs.getFilters();
		if(origGrf != null && !origGrf.isEmpty()) {
			GenRowFilters convertedGrf = new GenRowFilters();
			for(int i = 0; i < origGrf.size(); i++) {
				convertedGrf.addFilters(convertFilter(origGrf.get(i), meta, aliases));
			}
			return convertedGrf;
		}
		// return the empty grs
		return grs;
	}
	
	public static IQueryFilter convertFilter(IQueryFilter queryFilter, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, meta, aliases);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter, meta, aliases);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter, meta, aliases);
		} else {
			return null;
		}
	}
	
	private static IQueryFilter convertOrQueryFilter(OrQueryFilter queryFilter, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases) {
		OrQueryFilter newF = new OrQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, meta));
		}
		return newF;
	}

	private static IQueryFilter convertAndQueryFilter(AndQueryFilter queryFilter, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases) {
		AndQueryFilter newF = new AndQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, meta));
		}
		return newF;
	}

	private static SimpleQueryFilter convertSimpleQueryFilter(SimpleQueryFilter queryFilter, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases) {
		NounMetadata newL = null;
		NounMetadata newR = null;

		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			try {
				newL = new NounMetadata( convertSelector(selector, meta) , PixelDataType.COLUMN);
			} catch(IllegalArgumentException e) {
				IQuerySelector newS = getNewSelectorForAlias(selector, aliases);
				if(newS == null) {
					throw e;
				}
				newL = new NounMetadata(newS, PixelDataType.COLUMN);
			}
		} else {
			newL = origL;
		}
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			IQuerySelector selector = (IQuerySelector) origR.getValue();
			try {
				newR = new NounMetadata( convertSelector(selector, meta) , PixelDataType.COLUMN);
			} catch(IllegalArgumentException e) {
				IQuerySelector newS = getNewSelectorForAlias(selector, aliases);
				if(newS == null) {
					throw e;
				}
				newR = new NounMetadata(newS, PixelDataType.COLUMN);
			}

		} else {
			newR = origR;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, queryFilter.getComparator(), newR);
		return newF;
	}
	
	private static IQuerySelector getNewSelectorForAlias(IQuerySelector selector, Map<String, IQuerySelector> aliases) {
		if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN && aliases.containsKey(selector.getAlias())) {
			IQuerySelector actualSelector = aliases.get(selector.getAlias());
			return actualSelector;
//			if(actualSelector.getSelectorType() == SELECTOR_TYPE.FUNCTION) {
//				List<IQuerySelector> inners = ((QueryFunctionSelector) actualSelector).getInnerSelector();
//				if(inners.size() == 1 && inners.get(0).getSelectorType() == SELECTOR_TYPE.COLUMN) {
//					QueryOpaqueSelector s = new QueryOpaqueSelector();
//					s.setQuerySelectorSyntax(selector.getAlias());
//					return s;
//				}
//			}
		}
		
		return null;
		
	}
}
