package prerna.query.querystruct.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QSRenameTableConverter {
	
	private static final Logger logger = LogManager.getLogger(QSRenameTableConverter.class.getName());
	
	private QSRenameTableConverter() {

	}

	public static SelectQueryStruct convertQs(SelectQueryStruct qs, Map<String, String> transformationMap, boolean keepOrigAlias) {
		if(qs instanceof HardSelectQueryStruct) {
			return qs;
		}
		// need to modify and re-add all the selectors
		SelectQueryStruct convertedQs = qs.getNewBaseQueryStruct();
		convertedQs.setLimit(qs.getLimit());
		convertedQs.setOffSet(qs.getOffset());

		// grab all the selectors
		// and need to recursively modify the column ones
		List<IQuerySelector> origSelectors = qs.getSelectors();
		List<IQuerySelector> convertedSelectors = new ArrayList<>();
		for(int i = 0; i < origSelectors.size(); i++) {
			IQuerySelector origS = origSelectors.get(i);
			IQuerySelector convertedS = convertSelector(origS, transformationMap, keepOrigAlias);
			convertedSelectors.add(convertedS);
		}
		convertedQs.setSelectors(convertedSelectors);

		// now go through the filters
		convertedQs.setImplicitFilters(convertGenRowFilters(qs.getImplicitFilters(), transformationMap, keepOrigAlias));
		convertedQs.setExplicitFilters(convertGenRowFilters(qs.getExplicitFilters(), transformationMap, keepOrigAlias));
		convertedQs.setHavingFilters(convertGenRowFilters(qs.getHavingFilters(), transformationMap, keepOrigAlias));

		// also do the frame and panel filters
		convertedQs.setFrameImplicitFilters(convertGenRowFilters(qs.getFrameImplicitFilters(), transformationMap, keepOrigAlias));
		convertedQs.setPanelImplicitFilters(convertGenRowFilters(qs.getPanelImplicitFilters(), transformationMap, keepOrigAlias));

		// now go through the joins
		convertedQs.setRelations(convertJoins(qs.getRelations(), transformationMap));
		
		// now go through the group by
		List<IQuerySelector> origGroups = qs.getGroupBy();
		
		if(origGroups != null && !origGroups.isEmpty()) {
			List<IQuerySelector> convertedGroups =  new ArrayList<>();
			
			for(int i = 0; i < origGroups.size(); i++) {
				IQuerySelector origGroupS = origGroups.get(i);
				IQuerySelector convertedGroupS = convertSelector(origGroupS, transformationMap, keepOrigAlias);
				convertedGroups.add(convertedGroupS);
			}
			convertedQs.setGroupBy(convertedGroups);
		}
		
		// now go through the order by
		List<IQuerySort> origOrders = qs.getOrderBy();
		if(origOrders != null && !origOrders.isEmpty()) {
			List<IQuerySort> convertedOrderBys =  new ArrayList<>();
			for(int i = 0; i < origOrders.size(); i++) {
				IQuerySort origOrderS = origOrders.get(i);
				IQuerySort convertedOrderByS = convertOrderByOperation(origOrderS, transformationMap);
				convertedOrderBys.add(convertedOrderByS);
			}
			convertedQs.setOrderBy(convertedOrderBys);
		}
		
		// do the same for the panel order bys
		List<IQuerySort> origPanelOrders = qs.getPanelOrderBy();
		if(origPanelOrders != null && !origPanelOrders.isEmpty()) {
			List<IQuerySort> convertedOrderBys =  new ArrayList<>();
			for(int i = 0; i < origPanelOrders.size(); i++) {
				IQuerySort origOrderS = origPanelOrders.get(i);
				IQuerySort convertedOrderByS = convertOrderByOperation(origOrderS, transformationMap);
				convertedOrderBys.add(convertedOrderByS);
			}
			convertedQs.setPanelOrderBy(convertedOrderBys);
		}
		
		return convertedQs;
	}

	private static Set<IRelation> convertJoins(Set<IRelation> relationsSet, Map<String, String> transformationMap) {
		Set<IRelation> convertedJoins = new RelationSet();
		for(IRelation relationship : relationsSet) {
			if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
				BasicRelationship rel = (BasicRelationship) relationship;
				
				String newStart = transformationMap.get(rel.getFromConcept());
				if(newStart == null) {
					newStart = rel.getFromConcept();
				}
				String joinType = rel.getJoinType();
				String newEnd = transformationMap.get(rel.getToConcept());
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
	public static IQuerySelector convertSelector(IQuerySelector selector, Map<String, String> transformationMap, boolean keepOrigAlias) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return convertConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return convertColumnSelector((QueryColumnSelector) selector, transformationMap, keepOrigAlias);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return convertFunctionSelector((QueryFunctionSelector) selector, transformationMap, keepOrigAlias);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return convertArithmeticSelector((QueryArithmeticSelector) selector, transformationMap, keepOrigAlias);
		}
		return null;
	}

	private static IQuerySelector convertColumnSelector(QueryColumnSelector selector, Map<String, String> transformationMap, boolean keepOrigAlias) {
		String tableName = selector.getTable();
		if(transformationMap.containsKey(tableName)) {
			// we need to switch
			String newTableName = transformationMap.get(tableName);
			QueryColumnSelector newS = new QueryColumnSelector(newTableName+"__"+selector.getColumn());
			newS.setAlias(selector.getAlias());
			newS.setTableAlias(selector.getTableAlias());
			return newS;
		}
		
		// dont need to switch
		// just return the orig column
		return selector;
	}

	private static IQuerySelector convertArithmeticSelector(QueryArithmeticSelector selector, Map<String, String> transformationMap, boolean keepOrigAlias) {
		QueryArithmeticSelector newS = new QueryArithmeticSelector();
		newS.setLeftSelector(convertSelector(selector.getLeftSelector(), transformationMap, keepOrigAlias));
		newS.setRightSelector(convertSelector(selector.getRightSelector(), transformationMap, keepOrigAlias));
		newS.setMathExpr(selector.getMathExpr());
		newS.setAlias(selector.getAlias());
		return newS;
	}

	private static IQuerySelector convertFunctionSelector(QueryFunctionSelector selector, Map<String, String> transformationMap, boolean keepOrigAlias) {
		QueryFunctionSelector newS = new QueryFunctionSelector();
		for(IQuerySelector innerS : selector.getInnerSelector()) {
			newS.addInnerSelector(convertSelector(innerS, transformationMap, keepOrigAlias));
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
	public static IQuerySort convertOrderByOperation(IQuerySort orderBy, Map<String, String> transformationMap) {
		if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
			QueryColumnOrderBySelector selector = (QueryColumnOrderBySelector) orderBy;
			String newTable = transformationMap.get(selector.getTable());
			if(newTable == null) {
				// nothing to do
				// return the original
				return selector;
			}
			QueryColumnOrderBySelector newS = new QueryColumnOrderBySelector(newTable+selector.getColumn());
			newS.setSortDir(selector.getSortDirString());
			newS.setAlias(selector.getAlias());
			return newS;
		} else if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.CUSTOM) {
			QueryCustomOrderBy customSort = (QueryCustomOrderBy) orderBy;
			String newTable = transformationMap.get(customSort.getColumnToSort().getTable());
			if(newTable == null) {
				// nothing to do
				// return the original
				return customSort;
			}
			QueryCustomOrderBy newCustomSort = new QueryCustomOrderBy();
			QueryColumnSelector newS = new QueryColumnSelector(newTable+"__"+customSort.getColumnToSort().getColumn());
			newS.setAlias(customSort.getColumnToSort().getAlias());
			newCustomSort.setColumnToSort(newS);
			newCustomSort.setCustomOrder(customSort.getCustomOrder());
			return newCustomSort;
		}
		
		return null;
	}
	
	public static GenRowFilters convertGenRowFilters(GenRowFilters grs, Map<String, String> transformationMap, boolean keepOrigAlias) {
		List<IQueryFilter> origGrf = grs.getFilters();
		if(origGrf != null && !origGrf.isEmpty()) {
			GenRowFilters convertedGrf = new GenRowFilters();
			for(int i = 0; i < origGrf.size(); i++) {
				convertedGrf.addFilters(convertFilter(origGrf.get(i), transformationMap, keepOrigAlias));
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
	public static IQueryFilter convertFilter(IQueryFilter queryFilter, Map<String, String> transformationMap, boolean keepOrigAlias) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, transformationMap, keepOrigAlias);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter, transformationMap, keepOrigAlias);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter, transformationMap, keepOrigAlias);
		} else {
			return null;
		}
	}
	
	private static IQueryFilter convertOrQueryFilter(OrQueryFilter queryFilter, Map<String, String> transformationMap, boolean keepOrigAlias) {
		OrQueryFilter newF = new OrQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, transformationMap, keepOrigAlias));
		}
		return newF;
	}

	private static IQueryFilter convertAndQueryFilter(AndQueryFilter queryFilter, Map<String, String> transformationMap, boolean keepOrigAlias) {
		AndQueryFilter newF = new AndQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, transformationMap, keepOrigAlias));
		}
		return newF;
	}

	private static SimpleQueryFilter convertSimpleQueryFilter(SimpleQueryFilter queryFilter, Map<String, String> transformationMap, boolean keepOrigAlias) {
		NounMetadata newL = null;
		NounMetadata newR = null;

		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newL = new NounMetadata( convertSelector((IQuerySelector) origL.getValue(), transformationMap, keepOrigAlias) , PixelDataType.COLUMN);
		} else {
			newL = origL;
		}
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			newR = new NounMetadata( convertSelector((IQuerySelector) origR.getValue(), transformationMap, keepOrigAlias) , PixelDataType.COLUMN);
		} else {
			newR = origR;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, queryFilter.getComparator(), newR);
		return newF;
	}
}
