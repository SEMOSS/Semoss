package prerna.reactor.frame.filtermodel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GetPanelFilterStateReactor extends AbstractFilterReactor {

	private static final Logger classLogger = LogManager.getLogger(GetPanelFilterStateReactor.class);

	/**
	 * This reactor has many inputs
	 * 
	 * 1) columnName <- required 2) filterWord <- optional 3) limit <- optional
	 * 4) offset <- optional 5) panel <- optional
	 * 
	 * This reactor returns the filter values that are filtered out i.e. these
	 * would be values that are unchecked in a drop down selection
	 */

	public GetPanelFilterStateReactor() {
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

	public NounMetadata getFilterModel(ITableDataFrame dataframe, String tableCol, String filterWord, int limit,
			int offset, InsightPanel panel) {
		// store results in this map
		Map<String, Object> retMap = new HashMap<String, Object>();
		// first just return the info that was passed in
		retMap.put("column", tableCol);
		retMap.put("limit", limit);
		retMap.put("offset", offset);
		retMap.put("filterWord", filterWord);

		// get the base filters that are being applied that we are concerned
		// about
		GenRowFilters baseFilters = dataframe.getFrameFilters().copy();
		GenRowFilters baseFiltersExcludeCol = dataframe.getFrameFilters().copy();
		if (panel != null) {
			baseFilters.merge(panel.getPanelFilters().copy());
			baseFiltersExcludeCol.merge(panel.getPanelFilters().copy());
		}
		baseFiltersExcludeCol.removeColumnFilter(tableCol);

		// unique count function for table col
		QueryFunctionSelector uCountFunc = new QueryFunctionSelector();
		uCountFunc.setDistinct(true);
		uCountFunc.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
		QueryColumnSelector inner = new QueryColumnSelector(tableCol);
		uCountFunc.addInnerSelector(inner);

		// get total count of options
		SelectQueryStruct totalCountQS = new SelectQueryStruct();
		totalCountQS.addSelector(uCountFunc);
		
		// if search add to totalCount
		// add the filter word as a like filter
		SimpleQueryFilter wFilter = null;
		if (filterWord != null && !filterWord.trim().isEmpty()) {
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector(tableCol), PixelDataType.COLUMN);
			String comparator = "?like";
			NounMetadata rComparison = new NounMetadata(filterWord, PixelDataType.CONST_STRING);
			wFilter = new SimpleQueryFilter(lComparison, comparator, rComparison);
			totalCountQS.addExplicitFilter(wFilter);
		}
		
		int totalCount = 0;
		IRawSelectWrapper totalCountIt = null;
		try {
			totalCountIt = dataframe.query(totalCountQS);
			while (totalCountIt.hasNext()) {
				Object numUnique = totalCountIt.next().getValues()[0];
				totalCount = ((Number) numUnique).intValue();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			if(totalCountIt != null) {
				try {
					totalCountIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		retMap.put("totalCount", totalCount);

		// set the base info in the query struct to collect values
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryColumnSelector selector = new QueryColumnSelector(tableCol);
		qs.addSelector(selector);
		qs.setLimit(limit);
		qs.setOffSet(offset);
		qs.addOrderBy(new QueryColumnOrderBySelector(tableCol));
		
		if (filterWord != null && !filterWord.trim().isEmpty()) {
			qs.addExplicitFilter(wFilter);
		}
		// grab all the values
		List<Object> options = new ArrayList<Object>();
		// flush out the values
		IRawSelectWrapper allValuesIt = null;
		try {
			allValuesIt = dataframe.query(qs);
			while (allValuesIt.hasNext()) {
				options.add(allValuesIt.next().getValues()[0]);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(allValuesIt != null) {
				try {
					allValuesIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		retMap.put("options", options);

		////////////////////////////////////////
		//// get options
		///////////////////////////////////////
		// set the base info in the query struct
		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs2.addSelector(selector);
		qs2.setLimit(limit);
		qs2.setOffSet(offset);
		qs2.addOrderBy(new QueryColumnOrderBySelector(tableCol));

		// add the filter word as a like filter
		if (filterWord != null && !filterWord.trim().isEmpty()) {
			baseFilters.addFilters(wFilter);
		}

		// figure out the selected values
		List<Object> selectedValues = new ArrayList<Object>();
		if (filterWord != null && !filterWord.trim().isEmpty()) {
			baseFilters.addFilters(wFilter);
		}
		// this is just the values of the column given the current filters
		qs2.setExplicitFilters(baseFilters);

//		if (!selectAll) {
			// now run and flush out the values
			IRawSelectWrapper unFilterValuesIt = null;
			try {
				unFilterValuesIt = dataframe.query(qs2);
				while (unFilterValuesIt.hasNext()) {
					selectedValues.add(unFilterValuesIt.next().getValues()[0]);
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(unFilterValuesIt != null) {
					try {
						unFilterValuesIt.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
//		} else {
//			selectedValues.addAll(options);
//		}

		retMap.put("selectedValues", selectedValues);

		// get selected count
		SelectQueryStruct selectedCountQS = new SelectQueryStruct();
		selectedCountQS.addSelector(uCountFunc);
		selectedCountQS.setExplicitFilters(baseFilters);

		int selectedCount = 0;
		IRawSelectWrapper selectedCountIt = null;
		try {
			selectedCountIt = dataframe.query(selectedCountQS);
			while (selectedCountIt.hasNext()) {
				Object numUnique = selectedCountIt.next().getValues()[0];
				selectedCount = ((Number) numUnique).intValue();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(selectedCountIt != null) {
				try {
					selectedCountIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		retMap.put("selectedCount", selectedCount);

		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_FILTER_MODEL);
	}
}