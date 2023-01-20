package prerna.ds.nativeframe;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.ds.shared.CachedIterator;
import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.gson.SelectQueryStructAdapter;

public class NativeFrame extends AbstractTableDataFrame {

	private static final Logger logger = LogManager.getLogger(NativeFrame.class);

	public static final String DATA_MAKER_NAME = "NativeFrame";

	private static List<IEngine.ENGINE_TYPE> cacheEngines = new Vector<>();
	static {
		cacheEngines.add(IEngine.ENGINE_TYPE.SESAME);
		cacheEngines.add(IEngine.ENGINE_TYPE.JENA);
		cacheEngines.add(IEngine.ENGINE_TYPE.RDBMS);
		cacheEngines.add(IEngine.ENGINE_TYPE.IMPALA);
		cacheEngines.add(IEngine.ENGINE_TYPE.NEO4J_EMBEDDED);
		cacheEngines.add(IEngine.ENGINE_TYPE.NEO4J);
	}

	private SelectQueryStruct qs;


	public NativeFrame() {
		super();
		this.qs = new SelectQueryStruct();
		this.qs.setFrame(this);
		// by default set to engine
		this.qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
		setDefaultName();
		this.originalName = this.frameName;
	}
	
	public NativeFrame(String alias) {
		super();
		this.qs = new SelectQueryStruct();
		this.qs.setFrame(this);
		// by default set to engine
		this.qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
		this.frameName = alias;
		this.originalName = this.frameName;
	}

	private void setDefaultName() {
		String uuid = UUID.randomUUID().toString().toUpperCase();
		uuid = uuid.replaceAll("-", "_");
		setName("NATIVE_" + uuid);
	}

	public void setConnection(String engineName) {
		qs.setEngineId(engineName);
	}

	@Override
	public Double getMax(String columnHeader) {
		SemossDataType dataType = this.metaData.getHeaderTypeAsEnum(columnHeader);
		if (dataType == SemossDataType.INT|| dataType == SemossDataType.DOUBLE) {
			QueryFunctionSelector selector = new QueryFunctionSelector();
			QueryColumnSelector innerSelector = new QueryColumnSelector(columnHeader);
			selector.addInnerSelector(innerSelector);
			selector.setFunction(QueryFunctionHelper.MAX);

			SelectQueryStruct mQs = new SelectQueryStruct();
			mQs.addSelector(selector);
			// merge the base filters
			mQs.mergeExplicitFilters(qs.getExplicitFilters());
			// merge the additional filters added to frame
			mQs.mergeImplicitFilters(this.grf);
			// merge the joins
			mQs.mergeRelations(qs.getRelations());

			IRawSelectWrapper it = null;
			try {
				it = query(mQs);
				return ((Number) it.next().getValues()[1]).doubleValue();
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					it.cleanUp();
				}
			}
		}
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		SemossDataType dataType = this.metaData.getHeaderTypeAsEnum(columnHeader);
		if (dataType == SemossDataType.INT|| dataType == SemossDataType.DOUBLE) {
			QueryFunctionSelector selector = new QueryFunctionSelector();
			QueryColumnSelector innerSelector = new QueryColumnSelector(columnHeader);
			selector.addInnerSelector(innerSelector);
			selector.setFunction(QueryFunctionHelper.MIN);

			SelectQueryStruct mQs = new SelectQueryStruct();
			mQs.addSelector(selector);
			// merge the base filters
			mQs.mergeExplicitFilters(qs.getExplicitFilters());
			// merge the additional filters added to frame
			mQs.mergeImplicitFilters(this.grf);
			// merge the joins
			mQs.mergeRelations(qs.getRelations());

			IRawSelectWrapper it = null;
			try {
				it = query(mQs);
				return ((Number) it.next().getValues()[1]).doubleValue();
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					it.cleanUp();
				}
			}
		}
		return null;
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		SelectQueryStruct newQs = new SelectQueryStruct();
		QueryColumnSelector selector = new QueryColumnSelector();
		String[] split = columnHeader.split("__");
		selector.setTable(split[0]);
		selector.setColumn(split[1]);
		newQs.addSelector(selector);
		// merge the base filters
		newQs.mergeExplicitFilters(qs.getExplicitFilters());
		// merge the additional filters added to frame
		newQs.mergeImplicitFilters(this.grf);
		// merge the joins
		newQs.mergeRelations(qs.getRelations());

		List<Object> values = new Vector<Object>();

		IRawSelectWrapper it = null;
		try {
			it = query(newQs);
			while(it.hasNext()) {
				values.add(it.next().getValues()[0]);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				it.cleanUp();
			}
		}
		return values.toArray(new Double[values.size()]);
	}

	@Override
	public Object[] getColumn(String columnHeader) {
		SelectQueryStruct newQs = new SelectQueryStruct();
		QueryColumnSelector selector = new QueryColumnSelector();
		String[] split = columnHeader.split("__");
		selector.setTable(split[0]);
		selector.setColumn(split[1]);
		newQs.addSelector(selector);
		// merge the base filters
		newQs.mergeExplicitFilters(qs.getExplicitFilters());
		// merge the additional filters added to frame
		newQs.mergeImplicitFilters(this.grf);
		// merge the joins
		newQs.mergeRelations(qs.getRelations());

		List<Object> values = new Vector<>();

		IRawSelectWrapper it = null;
		try {
			it = query(newQs);
			while(it.hasNext()) {
				values.add(it.next().getValues()[0]);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				it.cleanUp();
			}
		}
		
		return values.toArray();
	}

	@Override
	public String getDataMakerName() {
		return DATA_MAKER_NAME;
	}

	public void mergeQueryStruct(SelectQueryStruct qs) {
		this.qs.merge(qs);
	}

	public String getEngineId() {
		return qs.getEngineId();
	}

	public SelectQueryStruct getQueryStruct() {
		return this.qs;
	}

	@Override
	public long size(String tableName) {
		// nothing is held in memory...
		return 0;
	}

	@Override
	public boolean isEmpty() {
		IEngine engine = this.qs.retrieveQueryStructEngine();
		if(engine == null) {
			return true;
		}
		
		boolean empty = false;
		IRawSelectWrapper it = null;
		try {
			it = WrapperManager.getInstance().getRawWrapper(engine, this.qs);
			empty = !(it.hasNext());
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				it.cleanUp();
			}
		}
		
		return empty;
	}

	@Override
	public IRawSelectWrapper query(String query) throws Exception {
		long start = System.currentTimeMillis();
		IEngine engine = this.qs.retrieveQueryStructEngine();
		logger.info("Executing query on engine " + engine.getEngineId());
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(this.qs.retrieveQueryStructEngine(), query);
		long end = System.currentTimeMillis();
		logger.info("Engine execution time = " + (end-start) + "ms");
		return it;
	}

	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) throws Exception {
		long start = System.currentTimeMillis();

		// prepare the query struct
		qs = prepQsForExecution(qs);
		// we can cache a few different engine types
		boolean cache = false;
		if(qs.getPragmap() != null && qs.getPragmap().containsKey("xCache")) {
			cache = ((String)qs.getPragmap().get("xCache")).equalsIgnoreCase("True") ? true : false;
		}

		IRawSelectWrapper it = null;
		if(cache) {
			if(NativeFrame.cacheEngines.contains(this.qs.retrieveQueryStructEngine().getEngineType())) {
				// this is an engine whose results can be cached
				IQueryInterpreter interpreter = this.qs.retrieveQueryStructEngine().getQueryInterpreter();
				interpreter.setQueryStruct(qs);
				String query = interpreter.composeQuery();

				if(this.queryCache.containsKey(query)) {
					CachedIterator cached = this.queryCache.get(query);
					RawCachedWrapper rcw = new RawCachedWrapper();
					rcw.setIterator(cached);
					it = rcw;
				}
			}
		}

		// if we still dont have an iterator
		// create it
		if(it == null) {
			IEngine engine = this.qs.retrieveQueryStructEngine();
			logger.info("Executing query on engine " + Utility.cleanLogString(engine.getEngineId()));
			it = WrapperManager.getInstance().getRawWrapper(engine, qs);
			long end = System.currentTimeMillis();
			logger.info("Engine execution time = " + (end-start) + "ms");
			return it;
		}

		return it;
	}
	
	public SelectQueryStruct prepQsForExecution(SelectQueryStruct qs) {
		IEngine engine = this.qs.retrieveQueryStructEngine();
		// account for potential double aggregations
		// TODO: account for double aggregation on other DB types...
		boolean doubleAggregation = false;
		if(engine instanceof IRDBMSEngine) {
			if(this.qs.getGroupBy() != null && !this.qs.getGroupBy().isEmpty()) {
				// we have a double aggregation
				// need to properly account for this
				// get the current this.QS and flush it to a query
				// and use that as a custom from
				
				IQueryInterpreter interpreter = engine.getQueryInterpreter();
				interpreter.setQueryStruct(this.qs);
				String newFromQuery = interpreter.composeQuery();
				// update the QS being executed
				qs.setCustomFrom(newFromQuery);
				qs.setCustomFromAliasName("embed_subquery");
				doubleAggregation = true;
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
			}
		}
		// normal combination
		if(!doubleAggregation) {
			// only convert at the beginning
			// since this.qs already has
			// everything as the physical
			qs.setCustomFrom(this.qs.getCustomFrom());
			qs.setCustomFromAliasName(this.qs.getCustomFromAliasName());
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);

			// we need to merge everything with the current qs
			qs.mergeGroupBy(this.qs.getGroupBy());
			qs.mergeOrderBy(this.qs.getOrderBy());
			// filters are a bit tricky
			// if a user is filtering in more on a specific column
			// we do not want to merge
			// but want to override
			Set<String> qsFilterCols = qs.getExplicitFilters().getAllFilteredColumns();
			List<IQueryFilter> importFilters = this.qs.getExplicitFilters().getFilters();
			// if the qsFilterCols doesn't have the base import filter
			// add the filter
			// otherwise, do nothing
			for(IQueryFilter filter : importFilters) {
				// we only do this for the simple filters
				// since the get added together / combined 
				if(filter instanceof SimpleQueryFilter) {
					Set<String> importColsFilters = filter.getAllUsedColumns();
					if(!qsFilterCols.containsAll(importColsFilters)) {
						// the import filter is not being overridden
						// so add it into the qs to sue
						qs.addImplicitFilter(filter);
					}
				} else {
					// add the filter
					// most likely an OR
					qs.addImplicitFilter(filter);
				}
			}
		}
		// setters
		qs.setEngine(this.qs.getEngine());
		qs.setRelations(this.qs.getRelations());
		qs.setBigDataEngine(this.qs.getBigDataEngine());
		
		return qs;
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return this.qs.retrieveQueryStructEngine().getQueryInterpreter();
	}

	public boolean engineQueryCacheable() {
		return NativeFrame.cacheEngines.contains(this.qs.retrieveQueryStructEngine().getEngineType());
	}

	@Override
	public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();

		String randFrameName = "Native" + Utility.getRandomString(6);
		cf.setFrameName(randFrameName);
		String frameFileName = folderDir + DIR_SEPARATOR + randFrameName + ".json";
		File frameFile = new File(frameFileName);
		Writer writer = null;
		JsonWriter jWriter = null;
		try {
			if(cipher != null) {
				writer = new BufferedWriter(new OutputStreamWriter(new CipherOutputStream(new FileOutputStream(frameFile), cipher)));
			} else {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(frameFile)));
			}
			jWriter = new JsonWriter(writer);
			SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
			adapter.write(jWriter, this.qs);
			jWriter.flush();
			jWriter.close();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IOException("Error occurred attempting to save native frame");
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(jWriter != null) {
				try {
					jWriter.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		cf.setFrameCacheLocation(frameFileName);

		// also save the meta details
		this.saveMeta(cf, folderDir, randFrameName, cipher);
		return cf;
	}

	@Override
	public void open(CachePropFileFrameObject cf, Cipher cipher) {
		// load the frame
		// this is just the QS
		Reader reader = null;
		JsonReader jReader = null;
		try {
			File frameFile = new File(Utility.normalizePath(cf.getFrameCacheLocation()));
			if(cipher != null) {
				reader = new BufferedReader(new InputStreamReader(new CipherInputStream(new FileInputStream(frameFile), cipher)));
			} else {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(frameFile)));
			}
			jReader = new JsonReader(reader);
			SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
			this.qs = adapter.read(jReader);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(jReader != null) {
				try {
					jReader.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// open the meta details
		this.openCacheMeta(cf, cipher);
	}
	
	@Override
	public DataFrameTypeEnum getFrameType() {
		return DataFrameTypeEnum.NATIVE;
	}
	
	@Override
	public Object querySQL(String query) {
		Map<String, Object> retMap = new HashMap<>();
		List <List<Object>> data = new ArrayList<List<Object>>();
		
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQuery(query);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = query(qs);
			while(wrapper.hasNext()) {
				data.add( Arrays.asList(wrapper.next().getValues()) );
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing sql: " + query);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		retMap.put("data", data);
		retMap.put("types", SemossDataType.convertSemossDataTypeArrToStringArr( wrapper.getTypes()) );
		retMap.put("columns", wrapper.getHeaders());
		return retMap;
	}

	/******************************* UNNECESSARY ON NATIVE FRAME FOR NOW BUT NEED TO OVERRIDE *************************************************/

//	@Override
//	@Deprecated
//	public Map<String, String> getScriptReactors() {
//		Map<String, String> reactorNames = super.getScriptReactors();
//		reactorNames.put(PKQLEnum.DATA_CONNECTDB, "prerna.sablecc.DataConnectDBReactor");
//		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
//		reactorNames.put(PKQLEnum.API, "prerna.sablecc.NativeApiReactor");
//		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.NativeImportDataReactor");
//
//		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
//		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
//		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
//		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
//		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
//		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
//		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
//		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
//		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
//		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
//
//		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.NativeApiReactor");
//		return reactorNames;
//	}

	@Override
	@Deprecated
	public void processDataMakerComponent(DataMakerComponent component) {
	}

	@Override
	@Deprecated
	public void removeColumn(String columnHeader) {
	}

	@Override
	@Deprecated
	public void addRow(Object[] cleanCells, String[] headers) {
	}
}
