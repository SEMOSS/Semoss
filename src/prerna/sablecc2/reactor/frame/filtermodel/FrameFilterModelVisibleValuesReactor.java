package prerna.sablecc2.reactor.frame.filtermodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.filter.AbstractFilterReactor;

public class FrameFilterModelVisibleValuesReactor extends AbstractFilterReactor {

	/**
	 * This reactor has many inputs
	 * 
	 * 1) columnName <- required
	 * 2) filterWord <- optional
	 * 3) limit <- optional
	 * 4) offset <- optional
	 * 5) panel <- optional
	 * 
	 * This reactor returns the visible values for a column
	 * i.e. these would be values that are checked in a drop down selection
	 */
	
	public FrameFilterModelVisibleValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame dataframe = getFrame();

		GenRowStruct colGrs = this.store.getNoun(keysToGet[0]);
		if (colGrs == null || colGrs.isEmpty()) {
			throw new IllegalArgumentException("Need to set the column for the filter model");
		}
		String tableCol = colGrs.get(0).toString();

		String filterWord = null;
		GenRowStruct filterWordGrs = this.store.getNoun(keysToGet[1]);
		if (filterWordGrs != null && !filterWordGrs.isEmpty()) {
			filterWord = filterWordGrs.get(0).toString();
		}

		int limit = -1;
		GenRowStruct limitGrs = this.store.getNoun(keysToGet[2]);
		if (limitGrs != null && !limitGrs.isEmpty()) {
			limit = ((Number) limitGrs.get(0)).intValue();
		}

		int offset = -1;
		GenRowStruct offsetGrs = this.store.getNoun(keysToGet[3]);
		if (offsetGrs != null && !offsetGrs.isEmpty()) {
			offset = ((Number) offsetGrs.get(0)).intValue();
		}

		InsightPanel panel = null;
		GenRowStruct panelGrs = this.store.getNoun(keysToGet[4]);
		if (panelGrs != null && !panelGrs.isEmpty()) {
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

		// set the base info in the query struct
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryColumnSelector selector = new QueryColumnSelector(tableCol);
		qs.addSelector(selector);
		qs.setLimit(limit);
		qs.setOffSet(offset);
		qs.addOrderBy(new QueryColumnOrderBySelector(tableCol));

		// get the base filters that are being applied that we are concerned about
		GenRowFilters baseFilters = dataframe.getFrameFilters().copy();
		if (panel != null) {
			baseFilters.merge(panel.getPanelFilters().copy());
		}
		// add the filter word as a like filter
		if (filterWord != null && !filterWord.trim().isEmpty()) {
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector(tableCol), PixelDataType.COLUMN);
			String comparator = "?like";
			NounMetadata rComparison = new NounMetadata(filterWord, PixelDataType.CONST_STRING);
			SimpleQueryFilter wFilter = new SimpleQueryFilter(lComparison, comparator, rComparison);
			baseFilters.addFilters(wFilter);
		}
		
		// figure out the visible values
		List<Object> frameValues = new ArrayList<Object>();
		// this is just the values of the column given the current filters
		qs.setExplicitFilters(baseFilters);
		// now run and flush out the values
		Iterator<IHeadersDataRow> unFilterValuesIt = dataframe.query(qs);
		while (unFilterValuesIt.hasNext()) {
			frameValues.add(unFilterValuesIt.next().getValues()[0]);
		}
		retMap.put("unfilterValues", frameValues);
		
		// for numerical, also add the min/max
		String alias = selector.getAlias();
		String metaName = dataframe.getMetaData().getUniqueNameFromAlias(alias);
		if (metaName == null) {
			metaName = alias;
		}
		SemossDataType columnType = dataframe.getMetaData().getHeaderTypeAsEnum(metaName);
		if (SemossDataType.INT == columnType || SemossDataType.DOUBLE == columnType) {
			QueryColumnSelector innerSelector = new QueryColumnSelector(tableCol);

			QueryFunctionSelector mathSelector = new QueryFunctionSelector();
			mathSelector.addInnerSelector(innerSelector);
			mathSelector.setFunction(QueryFunctionHelper.MIN);
			
			SelectQueryStruct mathQS = new SelectQueryStruct();
			mathQS.addSelector(mathSelector);

			// get the absolute min when no filters are present
			Map<String, Object> minMaxMap = new HashMap<String, Object>();
			Iterator<IHeadersDataRow> it = dataframe.query(mathQS);
			minMaxMap.put("absMin", it.next().getValues()[0]);
			// get the abs max when no filters are present
			mathSelector.setFunction(QueryFunctionHelper.MAX);
			it = dataframe.query(mathQS);
			minMaxMap.put("absMax", it.next().getValues()[0]);

			// add in the filters now and repeat
			mathQS.setExplicitFilters(baseFilters);
			// run for actual max
			it = dataframe.query(mathQS);
			minMaxMap.put("max", it.next().getValues()[0]);
			// run for actual min
			mathSelector.setFunction(QueryFunctionHelper.MIN);
			it = dataframe.query(mathQS);
			minMaxMap.put("min", it.next().getValues()[0]);

			retMap.put("minMax", minMaxMap);
		}

		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FILTER_MODEL);
	}
}
