package prerna.reactor.frame.filtermodel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
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

public class GetFrameFilterRange extends AbstractFilterReactor {

	private static final Logger classLogger = LogManager.getLogger(GetFrameFilterRange.class);

	/**
	 * Get the absolute min/max for the column and the relative min/max based on
	 * the filters
	 */

	public GetFrameFilterRange() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.PANEL.getKey(), DYNAMIC_KEY , OPTIONS_CACHE_KEY};
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame dataframe = getFrame();

		GenRowStruct colGrs = this.store.getNoun(keysToGet[0]);
		if (colGrs == null || colGrs.isEmpty()) {
			throw new IllegalArgumentException("Need to set the column for the filter model");
		}
		String tableCol = colGrs.get(0).toString();

		InsightPanel panel = null;
		GenRowStruct panelGrs = this.store.getNoun(keysToGet[1]);
		if (panelGrs != null && !panelGrs.isEmpty()) {
			String panelId = panelGrs.get(0) + "";
			panel = this.insight.getInsightPanel(panelId);
		}
		
		boolean dynamic = false;
		GenRowStruct dynamicGrs = this.store.getNoun(keysToGet[2]);
		if (dynamicGrs != null && !dynamicGrs.isEmpty()) {
			dynamic = Boolean.parseBoolean(dynamicGrs.get(0) + "");
		}

		boolean optionsCache = false;
		GenRowStruct optionsCacheGrs = this.store.getNoun(keysToGet[3]);
		if (optionsCacheGrs != null && !optionsCacheGrs.isEmpty()) {
			optionsCache = Boolean.parseBoolean(optionsCacheGrs.get(0) + "");
		}
		
		if(dynamic && optionsCache) {
			throw new IllegalArgumentException("Cannot have dynamic filters with cached options");
		}
		
		return getFilterModel(dataframe, tableCol, dynamic, optionsCache, panel);
	}

	public NounMetadata getFilterModel(ITableDataFrame dataframe, String tableCol, boolean dynamic, boolean optionsCache, InsightPanel panel) {
		DataFrameTypeEnum frameType = dataframe.getFrameType();
		ITableDataFrame queryFrame = dataframe;
		if(optionsCache) {
			String uKey = dataframe.getName() + tableCol;
			ITableDataFrame cache = this.insight.getCachedFitlerModelFrame(uKey);
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

		// create the selector
		QueryColumnSelector selector = new QueryColumnSelector(tableCol);
		// get the base filters that are being applied that we are concerned
		GenRowFilters baseFilters = dataframe.getFrameFilters().copy();
		GenRowFilters baseFiltersExcludeCol = dataframe.getFrameFilters().copy();
		if (panel != null) {
			baseFilters.merge(panel.getPanelFilters().copy());
			baseFiltersExcludeCol.merge(panel.getPanelFilters().copy());
		}
		if(optionsCache) {
			baseFilters = baseFilters.extractColumnFilters(tableCol);
			baseFiltersExcludeCol = new GenRowFilters();
		} else {
			baseFiltersExcludeCol.removeColumnFilter(tableCol);
		}

		// for numerical, also add the min/max
		String alias = selector.getAlias();
		String metaName = dataframe.getMetaData().getUniqueNameFromAlias(alias);
		if (metaName == null) {
			metaName = alias;
		}
		// check it is in fact numeric
		SemossDataType columnType = dataframe.getMetaData().getHeaderTypeAsEnum(metaName);
		if (SemossDataType.INT == columnType || SemossDataType.DOUBLE == columnType || SemossDataType.DATE == columnType
				|| SemossDataType.TIMESTAMP == columnType) {
			QueryColumnSelector innerSelector = new QueryColumnSelector(tableCol);

			SelectQueryStruct mathQS = new SelectQueryStruct();
			if(dynamic) {
				mathQS.setImplicitFilters(baseFiltersExcludeCol);
			}
			
			QueryFunctionSelector mathSelector = new QueryFunctionSelector();
			mathSelector.addInnerSelector(innerSelector);
			mathSelector.setFunction(QueryFunctionHelper.MIN);
			mathQS.addSelector(mathSelector);
			// get the absolute min when no filters are present
			Map<String, Object> minMaxMap = new HashMap<String, Object>();
			IRawSelectWrapper it = null;
			try {
				it = queryFrame.query(mathQS);
				minMaxMap.put("absMin", it.next().getValues()[0]);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			// get the abs max when no filters are present
			mathSelector.setFunction(QueryFunctionHelper.MAX);
			try {
				it = queryFrame.query(mathQS);
				minMaxMap.put("absMax", it.next().getValues()[0]);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}

			// add in the filters now and repeat
			mathQS.setImplicitFilters(baseFilters);
			// run for actual max
			try {
				it = queryFrame.query(mathQS);
				minMaxMap.put("max", it.next().getValues()[0]);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			// run for actual min
			mathSelector.setFunction(QueryFunctionHelper.MIN);
			try {
				it = queryFrame.query(mathQS);
				minMaxMap.put("min", it.next().getValues()[0]);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}

			retMap.put("minMax", minMaxMap);
		}

		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FILTER_MODEL);
	}
}