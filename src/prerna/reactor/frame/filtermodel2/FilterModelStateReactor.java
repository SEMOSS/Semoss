package prerna.reactor.frame.filtermodel2;

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
import prerna.util.Constants;

public class FilterModelStateReactor extends AbstractFilterReactor {

	private static final Logger classLogger = LogManager.getLogger(FilterModelStateReactor.class);
	public static final String CLASS_NAME = FilterModelStateReactor.class.getName();
	
	/**
	 * This reactor has many inputs
	 * 
	 * 1) columnName <- required 2) filterWord <- optional 3) limit <- optional
	 * 4) offset <- optional 5) panel <- required 6) dynamic <- optional
	 * 
	 * This reactor returns the filter values that are filtered out i.e. these
	 * would be values that are unchecked in a drop down selection
	 */

	public FilterModelStateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.PANEL.getKey(),
				DYNAMIC_KEY, OPTIONS_CACHE_KEY};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataframe = getFrame();

		InsightPanel panel = getInsightPanel();
		if(panel == null) {
			throw new IllegalArgumentException("Must pass the panel that contains the curent filter state");
		}

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

		boolean dynamic = false;
		GenRowStruct dynamicGrs = this.store.getNoun(keysToGet[5]);
		if (dynamicGrs != null && !dynamicGrs.isEmpty()) {
			dynamic = Boolean.parseBoolean(dynamicGrs.get(0) + "");
		}
		
		boolean optionsCache = false;
		GenRowStruct optionsCacheGrs = this.store.getNoun(keysToGet[6]);
		if (optionsCacheGrs != null && !optionsCacheGrs.isEmpty()) {
			optionsCache = Boolean.parseBoolean(optionsCacheGrs.get(0) + "");
		}
		
		if(dynamic && optionsCache) {
			throw new IllegalArgumentException("Cannot have dynamic filters with cached options");
		}

		return getFilterModel(dataframe, tableCol, filterWord, limit, offset, dynamic, optionsCache, panel, logger);
	}

	public NounMetadata getFilterModel(ITableDataFrame dataframe, String tableCol, String filterWord, int limit,
			int offset, boolean dynamic, boolean optionsCache, InsightPanel panel, Logger logger) {
		
		DataFrameTypeEnum frameType = dataframe.getFrameType();
		ITableDataFrame queryFrame = dataframe;
		if(optionsCache) {
			String uKey = dataframe.getName() + tableCol;
			ITableDataFrame cache = insight.getCachedFitlerModelFrame(uKey);
			if(cache == null) {
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.addSelector(new QueryColumnSelector(tableCol));
				qs.setFrame(dataframe);
				IRawSelectWrapper it = null;
				try {
					it = dataframe.query(qs);
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new SemossPixelException(
							new NounMetadata("Error occurred executing query before loading into frame", 
									PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				}
				try {
					cache = FrameFactory.getFrame(this.insight, frameType.getTypeAsString(), uKey);
				} catch (Exception e) {
					throw new IllegalArgumentException("Error occurred trying to create the cached options frame of type " + frameType, e);
				}
				// insert the data for the new frame
				IImporter importer = ImportFactory.getImporter(cache, qs, it);
				try {
					importer.insertData();
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
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

		SimpleQueryFilter wFilter = null;
		if (filterWord != null && !filterWord.trim().isEmpty()) {
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector(tableCol), PixelDataType.COLUMN);
			String comparator = "?like";
			NounMetadata rComparison = new NounMetadata(filterWord, PixelDataType.CONST_STRING);
			wFilter = new SimpleQueryFilter(lComparison, comparator, rComparison);
		}
		
		// get the base filters that are being applied that we are concerned
		GenRowFilters baseFiltersExcludeCol = dataframe.getFrameFilters().copy();
		baseFiltersExcludeCol.removeColumnFilter(tableCol);

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
		if(dynamic) {
			qs.mergeImplicitFilters(baseFiltersExcludeCol);
		}
		
		// grab all the values
		logger.info(getExecutionQueryMessage(frameType, "retrieve all column values"));
		List<Object> options = new ArrayList<Object>();
		// flush out the values
		IRawSelectWrapper allValuesIt = null;
		try {
			allValuesIt = queryFrame.query(qs);
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
		if (filterWord != null && !filterWord.trim().isEmpty()) {
			totalCountQS.addExplicitFilter(wFilter);
		}
		if(dynamic) {
			totalCountQS.mergeImplicitFilters(baseFiltersExcludeCol);
		}
		
		logger.info(getExecutionQueryMessage(frameType, "retrieve the distinct count of all column values"));
		int totalCount = 0;
		IRawSelectWrapper totalCountIt = null;
		try {
			totalCountIt = queryFrame.query(totalCountQS);
			while (totalCountIt.hasNext()) {
				Object numUnique = totalCountIt.next().getValues()[0];
				totalCount = ((Number) numUnique).intValue();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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

		////////////////////////////////////////
		//// get options
		///////////////////////////////////////
		// set the base info in the query struct
		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs2.addSelector(selector);
		qs2.setLimit(limit);
		qs2.setOffSet(offset);
		qs2.addOrderBy(new QueryColumnOrderBySelector(tableCol));

		GenRowFilters baseFilters = null;
		GenRowFilters extractedCurrentFilters = null;
		if(optionsCache) {
			baseFilters = new GenRowFilters();
			// extract the current filters on this column
			extractedCurrentFilters = dataframe.getFrameFilters().copy().extractColumnFilters(tableCol);
		} else {
			baseFilters = dataframe.getFrameFilters().copy();
			// extract the current filters on this column
			extractedCurrentFilters = baseFilters.extractColumnFilters(tableCol);
		}
		// then deconflict with what is being proposed
		mergeFilters(panel.getTempFilterModelGrf(), extractedCurrentFilters);
		// then merge back with the other filters
		baseFilters.merge(extractedCurrentFilters);

		// this is just the values of the column given the current filters
		qs2.setExplicitFilters(baseFilters);
		
		logger.info(getExecutionQueryMessage(frameType, "retrieve the user selected values"));
		// figure out the selected values
		List<Object> selectedValues = new ArrayList<Object>();
		// now run and flush out the values
		IRawSelectWrapper unFilterValuesIt = null;
		try {
			unFilterValuesIt = queryFrame.query(qs2);
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

		retMap.put("selectedValues", selectedValues);

		// get selected count
		SelectQueryStruct selectedCountQS = new SelectQueryStruct();
		selectedCountQS.addSelector(uCountFunc);
		selectedCountQS.setExplicitFilters(baseFilters);
		
		logger.info(getExecutionQueryMessage(frameType, "retrieve the distinct count of the selected values"));
		int selectedCount = 0;
		IRawSelectWrapper selectedCountIt = null;
		try {
			selectedCountIt = queryFrame.query(selectedCountQS);
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

		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FILTER_MODEL);
	}
	
	/**
	 * 
	 * @param frameType
	 * @param baseMessage
	 * @return
	 */
	private String getExecutionQueryMessage(DataFrameTypeEnum frameType, String baseMessage) {
		if(frameType == DataFrameTypeEnum.NATIVE) {
			return "Executing query against the database to " + baseMessage;
		} 
		return "Executing query against the frame to " + baseMessage;
	}
	
}
