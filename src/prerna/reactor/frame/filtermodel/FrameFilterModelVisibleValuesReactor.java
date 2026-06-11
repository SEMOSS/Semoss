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

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
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

public class FrameFilterModelVisibleValuesReactor extends AbstractFilterReactor {

	private static final Logger classLogger = LogManager.getLogger(FrameFilterModelVisibleValuesReactor.class);

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
	 * This reactor returns the visible values for a column i.e. these would be
	 * values that are checked in a drop down selection
	 * </p>
	 */

	public FrameFilterModelVisibleValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.PANEL.getKey() };
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
		IRawSelectWrapper unFilterValuesIt = null;
		try {
			unFilterValuesIt = dataframe.query(qs);
			while (unFilterValuesIt.hasNext()) {
				frameValues.add(unFilterValuesIt.next().getValues()[0]);
			}
		} catch (Exception e1) {
			classLogger.error("Failed to retrieve visible values for column {} in frame filter model.", tableCol, e1);
		} finally {
			if (unFilterValuesIt != null) {
				try {
					unFilterValuesIt.close();
				} catch (IOException e) {
					classLogger.error("Failed to close visible-values iterator for column {} in frame filter model.",
							tableCol, e);
				}
			}
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
			IRawSelectWrapper it = null;
			try {
				it = dataframe.query(mathQS);
				minMaxMap.put("absMin", it.next().getValues()[0]);
			} catch (Exception e) {
				classLogger.error("Failed to retrieve absolute minimum for numeric column {} in frame filter model.",
						tableCol, e);
			} finally {
				if (it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error("Failed to close numeric-range iterator for column {} in frame filter model.",
								tableCol, e);
					}
				}
			}
			// get the abs max when no filters are present
			mathSelector.setFunction(QueryFunctionHelper.MAX);
			try {
				it = dataframe.query(mathQS);
				minMaxMap.put("absMax", it.next().getValues()[0]);
			} catch (Exception e) {
				classLogger.error("Failed to retrieve absolute maximum for numeric column {} in frame filter model.",
						tableCol, e);
			} finally {
				if (it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error("Failed to close numeric-range iterator for column {} in frame filter model.",
								tableCol, e);
					}
				}
			}

			// add in the filters now and repeat
			mathQS.setExplicitFilters(baseFilters);
			// run for actual max
			try {
				it = dataframe.query(mathQS);
				minMaxMap.put("max", it.next().getValues()[0]);
			} catch (Exception e) {
				classLogger.error("Failed to retrieve filtered maximum for numeric column {} in frame filter model.",
						tableCol, e);
			} finally {
				if (it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error("Failed to close numeric-range iterator for column {} in frame filter model.",
								tableCol, e);
					}
				}
			}
			// run for actual min
			mathSelector.setFunction(QueryFunctionHelper.MIN);
			try {
				it = dataframe.query(mathQS);
				minMaxMap.put("min", it.next().getValues()[0]);
			} catch (Exception e) {
				classLogger.error("Failed to retrieve filtered minimum for numeric column {} in frame filter model.",
						tableCol, e);
			} finally {
				if (it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error("Failed to close numeric-range iterator for column {} in frame filter model.",
								tableCol, e);
					}
				}
			}

			retMap.put("minMax", minMaxMap);
		}

		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FILTER_MODEL);
	}
}
