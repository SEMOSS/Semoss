package prerna.sablecc2.reactor.frame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.InsightPanel;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryAggregationEnum;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryMathSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class FrameFilterModelReactor extends AbstractReactor {

	/*
	 * This reactor has many inputs
	 * 
	 * 1) columnName <- required
	 * 2) filterWord <- optional
	 * 3) limit <- optional
	 * 4) offset <- optional
	 * 5) panel <- optional
	 */
	
	public FrameFilterModelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame dataframe = (ITableDataFrame) this.insight.getDataMaker();

		GenRowStruct colGrs = this.store.getNoun(keysToGet[0]);
		if(colGrs == null || colGrs.isEmpty()) {
			throw new IllegalArgumentException("Need to set the column for the filter model");
		}
		String tableCol = colGrs.get(0).toString();

		String filterWord = null;
		GenRowStruct filterWordGrs = this.store.getNoun(keysToGet[1]);
		if(filterWordGrs != null && !filterWordGrs.isEmpty()) {
			filterWord = filterWordGrs.get(0).toString();
		}

		int limit = -1;
		GenRowStruct limitGrs = this.store.getNoun(keysToGet[2]);
		if(limitGrs != null && !limitGrs.isEmpty()) {
			limit = ((Number) limitGrs.get(0)).intValue();
		}

		int offset = -1;
		GenRowStruct offsetGrs = this.store.getNoun(keysToGet[3]);
		if(offsetGrs != null && !offsetGrs.isEmpty()) {
			offset = ((Number) offsetGrs.get(0)).intValue();
		}

		InsightPanel panel = null;
		GenRowStruct panelGrs = this.store.getNoun(keysToGet[4]);
		if(panelGrs != null && !panelGrs.isEmpty()) {
			panel = (InsightPanel) panelGrs.get(0);
		}

		return getFilterModel(dataframe, tableCol, filterWord, limit, offset, panel);
	}

	public NounMetadata getFilterModel(ITableDataFrame dataframe, String tableCol, String filterWord, int limit, int offset, InsightPanel panel) {
		// store results in this map
		Map<String, Object> retMap = new HashMap<String, Object>();
		// first just return the info that was passed in
		retMap.put("column", tableCol);
		retMap.put("limit", limit);
		retMap.put("offset", offset);
		retMap.put("filterWord", filterWord);
		
		String tableName = null;
		String column = null;
		if(tableCol.contains("__")) {
			String[] split = tableCol.split("__");
			tableName = split[0];
			column = split[1];
		} else {
			tableName = tableCol;
			column = QueryStruct2.PRIM_KEY_PLACEHOLDER;
		}

		// set the base info in the query struct
		QueryStruct2 qs = new QueryStruct2();
		QueryColumnSelector selector = new QueryColumnSelector();
		selector.setTable(tableName);
		selector.setColumn(column);
		qs.addSelector(selector);
		qs.setLimit(limit);
		qs.setOffSet(offset);
		qs.addOrderBy(tableName, column, "ASC");
		
		// get the base filters that are being applied that we are concerned about
		GenRowFilters baseFilters = dataframe.getFrameFilters().copy();
		if(panel != null) {
			baseFilters.merge(panel.getPanelFilters().copy());
		}
		// add the filter word as a like filter
		if(filterWord != null && !filterWord.trim().isEmpty()) {
			NounMetadata lComparison = new NounMetadata(tableCol, PixelDataType.COLUMN);
			String comparator = "?like";
			NounMetadata rComparison = new NounMetadata(filterWord, PixelDataType.CONST_STRING);
			SimpleQueryFilter wFilter = new SimpleQueryFilter(lComparison, comparator, rComparison);
			baseFilters.addFilters(wFilter);
		}
		
		// filter values are the values that the user has filtered
		// i.e. these are the values that are unchecked in the drop selection
		
		// unfilter values are the values that are currently visible
		// i.e. these are the values that have a checkmark in the drop selection
		
		// figure out the visible values
		List<Object> unFilterValues = new ArrayList<Object>();
		// this is just the values of the column given the current filters
		qs.setFilters(baseFilters);
		// now run and flush out the values
		Iterator<IHeadersDataRow> unFilterValuesIt = dataframe.query(qs);
		while(unFilterValuesIt.hasNext()) {
			unFilterValues.add(unFilterValuesIt.next().getValues()[0]);
		}
		retMap.put("unfilterValues", unFilterValues);
		
		// if the current filters doesn't use the column
		// there is no values that are unchecked to select
		// i.e. nothing is done that is filtered that the user can undo for this column
		List<Object> filterValues = new ArrayList<Object>();
		if(baseFilters.getAllFilteredColumns().contains(tableCol)) {
			
			boolean validExistingFilters = true;
			// if we did add a word filter, we only want to execute if there is another filter present
			if(filterWord != null && !filterWord.trim().isEmpty()) {
				if(baseFilters.size() == 1) {
					validExistingFilters = false;
				}
			}
			
			if(validExistingFilters) {
				// to get the values that the user has filtered out
				// we need create the inverse of the filters
				// if they touch the column we care about
				GenRowFilters inverseFilters = new GenRowFilters();
				List<IQueryFilter> baseFiltersList = baseFilters.getFilters();
				for(IQueryFilter filter : baseFiltersList) {
					// check if it is a simple or complex filter
					if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
						if(filter.containsColumn(tableCol)) {
							// reverse the comparator
							SimpleQueryFilter fCopy = (SimpleQueryFilter) filter.copy();
							fCopy.reverseComparator();
							inverseFilters.addFilters(fCopy);
						} else {
							// just add it to the filters
							inverseFilters.addFilters(filter.copy());
						}
					} 
					// okay, this is hard to figure out if it is not simple
					// so just add it
					else {
						inverseFilters.addFilters(filter.copy());
					}
				}
				
				// to get the filtered values
				// run with the inverse filters of the current column
				qs.setFilters(inverseFilters);
		
				// flush out the values 
				Iterator<IHeadersDataRow> filterValuesIt = dataframe.query(qs);
				while(filterValuesIt.hasNext()) {
					filterValues.add(filterValuesIt.next().getValues()[0]);
				}
			}
		}
		retMap.put("filterValues", filterValues);

		// for numerical, also add the min/max
		String alias = tableName;
		if(!column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
			alias += "__" + column;
		}
		String metaName = dataframe.getMetaData().getUniqueNameFromAlias(alias);
		if(metaName == null) {
			metaName = alias;
		}
		if(SemossDataType.NUMBER == dataframe.getMetaData().getHeaderTypeAsEnum(metaName)) {
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			innerSelector.setTable(tableName);
			innerSelector.setColumn(column);
			
			QueryMathSelector mathSelector = new QueryMathSelector();
			mathSelector.setInnerSelector(innerSelector);
			mathSelector.setMath(QueryAggregationEnum.MIN);
			
			QueryStruct2 mathQS = new QueryStruct2();
			mathQS.addSelector(mathSelector);
			
			// get the absolute min when no filters are present
			Map<String, Object> minMaxMap = new HashMap<String, Object>();
			Iterator<IHeadersDataRow> it = dataframe.query(mathQS);
			minMaxMap.put("absMin", it.next().getValues()[0]);
			// get the abs max when no filters are present
			mathSelector.setMath(QueryAggregationEnum.MAX);
			it = dataframe.query(mathQS);
			minMaxMap.put("absMax", it.next().getValues()[0]);
			
			// add in the filters now and repeat
			mathQS.setFilters(baseFilters);
			// run for actual max
			it = dataframe.query(mathQS);
			minMaxMap.put("max", it.next().getValues()[0]);
			// run for actual min
			mathSelector.setMath(QueryAggregationEnum.MIN);
			it = dataframe.query(mathQS);
			minMaxMap.put("min", it.next().getValues()[0]);
			
			retMap.put("minMax", minMaxMap);
		}

		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FILTER_MODEL);
	}
}
