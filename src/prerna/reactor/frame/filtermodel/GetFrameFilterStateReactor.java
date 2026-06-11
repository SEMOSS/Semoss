/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.frame.filtermodel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
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
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetFrameFilterStateReactor extends AbstractFilterReactor {

	private static final Logger classLogger = LogManager.getLogger(GetFrameFilterStateReactor.class);

	/**
	 * <p>
	 * This reactor has many inputs
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>columnName <- required</li>
	 * <li>filterWord <- optional</li>
	 * <li>limit <- optional</li>
	 * <li>offset <- optional</li>
	 * <li>panel <- optional</li>
	 * </ul>
	 *
	 * <p>
	 * This reactor returns the filter values that are filtered out i.e. these would
	 * be values that are unchecked in a drop down selection
	 * </p>
	 */

	public GetFrameFilterStateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.PANEL.getKey(),
				DYNAMIC_KEY, OPTIONS_CACHE_KEY };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame dataframe = getFrame();

		GenRowStruct colGrs = this.store.getGenRowStruct(keysToGet[0]);
		if (colGrs == null || colGrs.isEmpty()) {
			throw new IllegalArgumentException("Need to set the column for the filter model");
		}
		String tableCol = colGrs.get(0).toString();

		String filterWord = null;
		GenRowStruct filterWordGrs = this.store.getGenRowStruct(keysToGet[1]);
		if (filterWordGrs != null && !filterWordGrs.isEmpty()) {
			filterWord = filterWordGrs.get(0).toString();
		}

		int limit = -1;
		GenRowStruct limitGrs = this.store.getGenRowStruct(keysToGet[2]);
		if (limitGrs != null && !limitGrs.isEmpty()) {
			limit = ((Number) limitGrs.get(0)).intValue();
		}

		int offset = -1;
		GenRowStruct offsetGrs = this.store.getGenRowStruct(keysToGet[3]);
		if (offsetGrs != null && !offsetGrs.isEmpty()) {
			offset = ((Number) offsetGrs.get(0)).intValue();
		}

		InsightPanel panel = null;
		GenRowStruct panelGrs = this.store.getGenRowStruct(keysToGet[4]);
		if (panelGrs != null && !panelGrs.isEmpty()) {
			String panelId = panelGrs.get(0) + "";
			panel = this.insight.getInsightPanel(panelId);
		}

		boolean dynamic = false;
		GenRowStruct dynamicGrs = this.store.getGenRowStruct(keysToGet[5]);
		if (dynamicGrs != null && !dynamicGrs.isEmpty()) {
			dynamic = Boolean.parseBoolean(dynamicGrs.get(0) + "");
		}

		boolean optionsCache = false;
		GenRowStruct optionsCacheGrs = this.store.getGenRowStruct(keysToGet[6]);
		if (optionsCacheGrs != null && !optionsCacheGrs.isEmpty()) {
			optionsCache = Boolean.parseBoolean(optionsCacheGrs.get(0) + "");
		}

		if (dynamic && optionsCache) {
			throw new IllegalArgumentException("Cannot have dynamic filters with cached options");
		}

		return getFilterModel(dataframe, tableCol, filterWord, limit, offset, dynamic, optionsCache, panel);
	}

	public NounMetadata getFilterModel(ITableDataFrame dataframe, String tableCol, String filterWord, int limit,
			int offset, boolean dynamic, boolean optionsCache, InsightPanel panel) {

		DataFrameTypeEnum frameType = dataframe.getFrameType();
		ITableDataFrame queryFrame = dataframe;
		if (optionsCache) {
			String uKey = dataframe.getName() + tableCol;
			ITableDataFrame cache = insight.getCachedFitlerModelFrame(uKey);
			if (cache == null) {
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.addSelector(new QueryColumnSelector(tableCol));
				qs.setFrame(dataframe);
				IRawSelectWrapper it = null;
				try {
					it = dataframe.query(qs);
				} catch (Exception e) {
					classLogger.error("Failed to query source frame {} while preparing options cache for column {}.",
							dataframe.getName(), tableCol, e);
					throw new SemossPixelException(
							new NounMetadata("Error occurred executing query before loading into frame",
									PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				}
				try {
					cache = FrameFactory.getFrame(this.insight, frameType.getTypeAsString(), uKey);
				} catch (Exception e) {
					throw new IllegalArgumentException(
							"Error occurred trying to create the cached options frame of type " + frameType, e);
				}
				// insert the data for the new frame
				IImporter importer = ImportFactory.getImporter(cache, qs, it);
				try {
					importer.insertData();
				} catch (Exception e) {
					classLogger.error("Failed to populate cached options frame {} for source frame {} and column {}.",
							uKey, dataframe.getName(), tableCol, e);
					throw new SemossPixelException(e.getMessage());
				}
				// now store this
				insight.addCachedFitlerModelFrame(uKey, cache);
			}
			// set the new dataframe reference to the cache
			queryFrame = cache;
		}

		// store results in this map
		Map<String, Object> retMap = new HashMap<String, Object>();
		// first just return the info that was passed in
		retMap.put("column", tableCol);
		retMap.put("limit", limit);
		retMap.put("offset", offset);
		retMap.put("filterWord", filterWord);

		// get the base filters that are being applied that we are concerned
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
		if (dynamic) {
			totalCountQS.mergeImplicitFilters(baseFiltersExcludeCol);
		}

		int totalCount = 0;
		IRawSelectWrapper totalCountIt = null;
		try {
			totalCountIt = queryFrame.query(totalCountQS);
			while (totalCountIt.hasNext()) {
				Object numUnique = totalCountIt.next().getValues()[0];
				totalCount = ((Number) numUnique).intValue();
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve total distinct count for column {} in frame filter state.", tableCol,
					e);
		} finally {
			if (totalCountIt != null) {
				try {
					totalCountIt.close();
				} catch (IOException e) {
					classLogger.error("Failed to close total-count iterator for column {} in frame filter state.",
							tableCol, e);
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
		if (dynamic) {
			qs.mergeImplicitFilters(baseFiltersExcludeCol);
		}
		// grab all the values
		List<Object> options = new ArrayList<Object>();
		// flush out the values
		IRawSelectWrapper allValuesIt = null;
		try {
			allValuesIt = queryFrame.query(qs);
			while (allValuesIt.hasNext()) {
				options.add(allValuesIt.next().getValues()[0]);
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve option values for column {} in frame filter state.", tableCol, e);
		} finally {
			if (allValuesIt != null) {
				try {
					allValuesIt.close();
				} catch (IOException e) {
					classLogger.error("Failed to close options iterator for column {} in frame filter state.", tableCol,
							e);
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

		// now run and flush out the values
		IRawSelectWrapper unFilterValuesIt = null;
		try {
			unFilterValuesIt = queryFrame.query(qs2);
			while (unFilterValuesIt.hasNext()) {
				selectedValues.add(unFilterValuesIt.next().getValues()[0]);
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve selected values for column {} in frame filter state.", tableCol, e);
		} finally {
			if (unFilterValuesIt != null) {
				try {
					unFilterValuesIt.close();
				} catch (IOException e) {
					classLogger.error("Failed to close selected-values iterator for column {} in frame filter state.",
							tableCol, e);
				}
			}
		}

		retMap.put("selectedValues", selectedValues);

		// get selected count
		SelectQueryStruct selectedCountQS = new SelectQueryStruct();
		selectedCountQS.addSelector(uCountFunc);
		selectedCountQS.setExplicitFilters(baseFilters);

		int selectedCount = 0;
		IRawSelectWrapper selectedCountIt = null;
		try {
			selectedCountIt = queryFrame.query(selectedCountQS);
			while (selectedCountIt.hasNext()) {
				Object numUnique = selectedCountIt.next().getValues()[0];
				selectedCount = ((Number) numUnique).intValue();
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve selected distinct count for column {} in frame filter state.",
					tableCol, e);
		} finally {
			if (selectedCountIt != null) {
				try {
					selectedCountIt.close();
				} catch (IOException e) {
					classLogger.error("Failed to close selected-count iterator for column {} in frame filter state.",
							tableCol, e);
				}
			}
		}
		retMap.put("selectedCount", selectedCount);

		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FILTER_MODEL);
	}
}
