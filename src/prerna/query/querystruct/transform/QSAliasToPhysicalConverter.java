package prerna.query.querystruct.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.RelationSet;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.gson.GsonUtility;

public class QSAliasToPhysicalConverter {

	private static final Logger logger = LogManager.getLogger(QSAliasToPhysicalConverter.class.getName());
	
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
			IQuerySelector convertedS = convertSelector(origS, meta, null);
			convertedSelectors.add(convertedS);
			aliases.put(convertedS.getAlias(), convertedS);
		}
		convertedQs.setSelectors(convertedSelectors);

		// now go through the filters
		convertedQs.setImplicitFilters(convertGenRowFilters(qs.getImplicitFilters(), meta, null));
		convertedQs.setExplicitFilters(convertGenRowFilters(qs.getExplicitFilters(), meta, null));
		convertedQs.setHavingFilters(convertHavingGenRowFilters(qs.getHavingFilters(), meta, aliases, null));

		// now go through the joins
		convertedQs.setRelations(convertJoins(qs.getRelations(), meta));
		
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
		
		String customTableName = convertedQs.getCustomFromAliasName();
		// grab all the selectors
		// and need to recursively modify the column ones
		Map<String, IQuerySelector> aliases = new HashMap<String, IQuerySelector>();
		List<IQuerySelector> origSelectors = qs.getSelectors();
		List<IQuerySelector> convertedSelectors = new Vector<IQuerySelector>();
		for(int i = 0; i < origSelectors.size(); i++) {
			IQuerySelector origS = origSelectors.get(i);
			IQuerySelector convertedS = convertSelector(origS, meta, customTableName);
			convertedSelectors.add(convertedS);
			aliases.put(convertedS.getAlias(), convertedS);
		}
		convertedQs.setSelectors(convertedSelectors);

		// now go through the filters
		convertedQs.setImplicitFilters(convertGenRowFilters(qs.getImplicitFilters(), meta, customTableName));
		convertedQs.setExplicitFilters(convertGenRowFilters(qs.getExplicitFilters(), meta, customTableName));
		convertedQs.setHavingFilters(convertHavingGenRowFilters(qs.getHavingFilters(), meta, aliases, customTableName));

		// also do the frame and panel filters
		convertedQs.setFrameImplicitFilters(convertGenRowFilters(qs.getFrameImplicitFilters(), meta, customTableName));
		convertedQs.setPanelImplicitFilters(convertGenRowFilters(qs.getPanelImplicitFilters(), meta, customTableName));
		
		// now go through the joins
		convertedQs.setRelations(convertJoins(qs.getRelations(), meta));

		// now go through the group by
		List<IQuerySelector> origGroups = qs.getGroupBy();
		
		if(origGroups != null && !origGroups.isEmpty()) {
			List<IQuerySelector> convertedGroups =  new Vector<>();
			
			for(int i = 0; i < origGroups.size(); i++) {
				IQuerySelector origGroupS = origGroups.get(i);
				IQuerySelector convertedGroupS = convertSelector(origGroupS, meta, customTableName);
				convertedGroups.add(convertedGroupS);
			}
			convertedQs.setGroupBy(convertedGroups);
		}
		
		// now go through the order by
		List<IQuerySort> origOrders = qs.getOrderBy();
		if(origOrders != null && !origOrders.isEmpty()) {
			List<IQuerySort> convertedOrderBys =  new Vector<IQuerySort>();
			for(int i = 0; i < origOrders.size(); i++) {
				IQuerySort origOrderOp = origOrders.get(i);
				IQuerySort convertedOrderByOp = convertOrderByOperation(origOrderOp, meta);
				convertedOrderBys.add(convertedOrderByOp);
			}
			convertedQs.setOrderBy(convertedOrderBys);
		}
		
		// do the same for the panel order bys
		List<IQuerySort> origPanelOrders = qs.getPanelOrderBy();
		if(origPanelOrders != null && !origPanelOrders.isEmpty()) {
			List<IQuerySort> convertedOrderBys =  new Vector<IQuerySort>();
			for(int i = 0; i < origPanelOrders.size(); i++) {
				IQuerySort origOrderS = origPanelOrders.get(i);
				IQuerySort convertedOrderByS = convertOrderByOperation(origOrderS, meta);
				convertedOrderBys.add(convertedOrderByS);
			}
			convertedQs.setPanelOrderBy(convertedOrderBys);
		}
		
		// also move the pragmap and the querypartmap
		convertedQs.setPragmap(qs.getPragmap());
		convertedQs.getParts().putAll(qs.getParts());
		
		return convertedQs;
	}

	private static Set<IRelation> convertJoins(Set<IRelation> relationsSet, OwlTemporalEngineMeta meta) {
		Set<IRelation> convertedJoins = new RelationSet();
		for(IRelation relationship : relationsSet) {
			if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
				BasicRelationship rel = (BasicRelationship) relationship;
				
				String newStart = meta.getUniqueNameFromAlias(rel.getFromConcept());
				if(newStart == null) {
					newStart = rel.getFromConcept();
				}
				String joinType = rel.getJoinType();
				String newEnd = meta.getUniqueNameFromAlias(rel.getToConcept());
				if(newEnd == null) {
					newEnd = rel.getToConcept();
				}
				
				convertedJoins.add(new BasicRelationship(new String[]{newStart, joinType, newEnd, rel.getComparator(), rel.getRelationName()}));
			} else {
				logger.info("Cannot process relationship of type: " + relationship.getRelationType());
			}
		}
		return convertedJoins;
	}
	
	/**
	 * Modify the selectors
	 * @param selector
	 * @return
	 */
	public static IQuerySelector convertSelector(IQuerySelector selector, OwlTemporalEngineMeta meta, String customTableName) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return convertConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return convertColumnSelector((QueryColumnSelector) selector, meta, customTableName);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return convertFunctionSelector((QueryFunctionSelector) selector, meta, customTableName);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return convertArithmeticSelector((QueryArithmeticSelector) selector, meta, customTableName);
		}else if(selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE)
		{
			return convertIfElseSelector((QueryIfSelector)selector, meta, customTableName);
		}
		return null;
	}
	
	
	private static IQuerySelector convertIfElseSelector(QueryIfSelector selector, OwlTemporalEngineMeta meta, String customTableName)
	{
		// get the condition first
		IQueryFilter condition = selector.getCondition();
		selector.setCondition(convertFilter(condition, meta, customTableName));
		
		// get the precedent
		IQuerySelector precedent = selector.getPrecedent();
		selector.setPrecedent(convertSelector(precedent, meta, customTableName));

		IQuerySelector antecedent = selector.getAntecedent();
		if(antecedent != null)
			selector.setAntecedent(convertSelector(antecedent, meta, customTableName));
		
		return selector;
	}
	

	private static IQuerySelector convertColumnSelector(QueryColumnSelector selector, OwlTemporalEngineMeta meta, String customTableName) {
		String qsName = selector.getQueryStructName();
		if(customTableName != null) {
			if(qsName.contains("__")) {
				return new QueryColumnSelector(customTableName + "__" + qsName.split("__")[1]);
			} else {
				return new QueryColumnSelector(customTableName + "__" + qsName);
			}
		}
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
	
	private static IQuerySelector convertArithmeticSelector(QueryArithmeticSelector selector, OwlTemporalEngineMeta meta, String customTableName) {
		QueryArithmeticSelector newS = new QueryArithmeticSelector();
		newS.setLeftSelector(convertSelector(selector.getLeftSelector(), meta, customTableName));
		newS.setRightSelector(convertSelector(selector.getRightSelector(), meta, customTableName));
		newS.setMathExpr(selector.getMathExpr());
		newS.setAlias(selector.getAlias());
		return newS;
	}

	private static IQuerySelector convertFunctionSelector(QueryFunctionSelector selector, OwlTemporalEngineMeta meta, String customTableName) {
		QueryFunctionSelector newS = new QueryFunctionSelector();
		for(IQuerySelector innerS : selector.getInnerSelector()) {
			newS.addInnerSelector(convertSelector(innerS, meta, customTableName));
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
	public static IQuerySort convertOrderByOperation(IQuerySort orderBy, OwlTemporalEngineMeta meta) {
		if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
			QueryColumnOrderBySelector selector = (QueryColumnOrderBySelector) orderBy;
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
		} else if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.CUSTOM) {
			QueryCustomOrderBy customSort = (QueryCustomOrderBy) orderBy;
			String newQsName = meta.getUniqueNameFromAlias(customSort.getColumnToSort().getQueryStructName());
			if(newQsName == null) {
				// nothing to do
				// return the original
				return customSort;
			}
			QueryCustomOrderBy newCustomS = new QueryCustomOrderBy();
			QueryColumnSelector newS = new QueryColumnSelector();
			if(newQsName.contains("__")) {
				String[] split = newQsName.split("__");
				newS.setTable(split[0]);
				newS.setColumn(split[1]);
			} else {
				newS.setTable(newQsName);
				newS.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
			newCustomS.setColumnToSort(newS);
			newCustomS.setCustomOrder(customSort.getCustomOrder());
			return newCustomS;
		}
		
		return null;
	}
	
	public static GenRowFilters convertGenRowFilters(GenRowFilters grs, OwlTemporalEngineMeta meta, String customTableName) {
		List<IQueryFilter> origGrf = grs.getFilters();
		if(origGrf != null && !origGrf.isEmpty()) {
			GenRowFilters convertedGrf = new GenRowFilters();
			for(int i = 0; i < origGrf.size(); i++) {
				convertedGrf.addFilters(convertFilter(origGrf.get(i), meta, customTableName));
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
	public static IQueryFilter convertFilter(IQueryFilter queryFilter, OwlTemporalEngineMeta meta, String customTableName) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, meta, customTableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter, meta, customTableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter, meta, customTableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			return convertBetweenQueryFilter((BetweenQueryFilter) queryFilter, meta, customTableName);
		}else {
			return null;
		}
	}
	
	private static IQueryFilter convertOrQueryFilter(OrQueryFilter queryFilter, OwlTemporalEngineMeta meta, String customTableName) {
		OrQueryFilter newF = new OrQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, meta, customTableName));
		}
		return newF;
	}

	private static IQueryFilter convertAndQueryFilter(AndQueryFilter queryFilter, OwlTemporalEngineMeta meta, String customTableName) {
		AndQueryFilter newF = new AndQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, meta, customTableName));
		}
		return newF;
	}

	private static SimpleQueryFilter convertSimpleQueryFilter(SimpleQueryFilter queryFilter, OwlTemporalEngineMeta meta, String customTableName) {
		NounMetadata newL = null;
		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newL = new NounMetadata( convertSelector((IQuerySelector) origL.getValue(), meta, customTableName) , PixelDataType.COLUMN);
		} else if(origL.getNounType() == PixelDataType.QUERY_STRUCT) {
			SelectQueryStruct newQs = getPhysicalQs((SelectQueryStruct) origL.getValue(), meta);
			newL = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
		} else {
			newL = origL;
		}
		
		NounMetadata newR = null;
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newR = new NounMetadata( convertSelector((IQuerySelector) origR.getValue(), meta, customTableName) , PixelDataType.COLUMN);
		} else if(origR.getNounType() == PixelDataType.QUERY_STRUCT) {
			SelectQueryStruct newQs = getPhysicalQs((SelectQueryStruct) origR.getValue(), meta);
			newR = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
		} else {
			newR = origR;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, queryFilter.getComparator(), newR);
		return newF;
	}
	
	private static BetweenQueryFilter convertBetweenQueryFilter(BetweenQueryFilter queryFilter, OwlTemporalEngineMeta meta, String customTableName) {
		// need to convert column to the full name
		queryFilter.setColumn(convertSelector(queryFilter.getColumn(), meta, customTableName));
		return queryFilter;
	}
	
	private static GenRowFilters convertHavingGenRowFilters(GenRowFilters grs, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases, String customTableName) {
		List<IQueryFilter> origGrf = grs.getFilters();
		if(origGrf != null && !origGrf.isEmpty()) {
			GenRowFilters convertedGrf = new GenRowFilters();
			for(int i = 0; i < origGrf.size(); i++) {
				convertedGrf.addFilters(convertFilter(origGrf.get(i), meta, aliases, customTableName));
			}
			return convertedGrf;
		}
		// return the empty grs
		return grs;
	}
	
	public static IQueryFilter convertFilter(IQueryFilter queryFilter, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases, String customTableName) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, meta, aliases, customTableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter, meta, aliases, customTableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter, meta, aliases, customTableName);
		} else {
			return null;
		}
	}
	
	private static IQueryFilter convertOrQueryFilter(OrQueryFilter queryFilter, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases, String customTableName) {
		OrQueryFilter newF = new OrQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, meta, customTableName));
		}
		return newF;
	}

	private static IQueryFilter convertAndQueryFilter(AndQueryFilter queryFilter, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases, String customTableName) {
		AndQueryFilter newF = new AndQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, meta, customTableName));
		}
		return newF;
	}

	private static SimpleQueryFilter convertSimpleQueryFilter(SimpleQueryFilter queryFilter, OwlTemporalEngineMeta meta, Map<String, IQuerySelector> aliases, String customTableName) {
		NounMetadata newL = null;
		NounMetadata newR = null;

		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			try {
				newL = new NounMetadata( convertSelector(selector, meta, customTableName) , PixelDataType.COLUMN);
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
				newR = new NounMetadata( convertSelector(selector, meta, customTableName) , PixelDataType.COLUMN);
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
