package prerna.ds.nativeframe;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.ds.shared.CachedIterator;
import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Utility;
import prerna.util.gson.SelectQueryStructAdapter;

public class NativeFrame extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "NativeFrame";

	private static List<IEngine.ENGINE_TYPE> cacheEngines = new Vector<IEngine.ENGINE_TYPE>();
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
		setDefaultName();
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
				e.printStackTrace();
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
				e.printStackTrace();
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
			e.printStackTrace();
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

		List<Object> values = new Vector<Object>();

		IRawSelectWrapper it = null;
		try {
			it = query(newQs);
			while(it.hasNext()) {
				values.add(it.next().getValues()[0]);
			}
		} catch (Exception e) {
			e.printStackTrace();
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
			empty = it.hasNext();
		} catch (Exception e) {
			e.printStackTrace();
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
		this.logger.info("Executing query on engine " + engine.getEngineId());
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(this.qs.retrieveQueryStructEngine(), query);
		long end = System.currentTimeMillis();
		this.logger.info("Engine execution time = " + (end-start) + "ms");
		return it;
	}

	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) throws Exception {
		long start = System.currentTimeMillis();
		IEngine engine = this.qs.retrieveQueryStructEngine();
		// we need to merge everything with the current qs
		qs.mergeGroupBy(this.qs.getGroupBy());
		qs.mergeOrderBy(this.qs.getOrderBy());
		// filters are a bit tricky
		// if a user is filtering in more on a specific column
		// we do not want to merge
		// but want to override
		GenRowFilters qsGrs = qs.getExplicitFilters();
		Set<String> qsFilterCols = qsGrs.getAllFilteredColumns();
		List<IQueryFilter> importFilters = this.qs.getExplicitFilters().getFilters();
		// if the qsFilterCols doesn't have the base import filter
		// add the filter
		// otherwise, do nothing
		for(IQueryFilter filter : importFilters) {
			Set<String> importColsFilters = filter.getAllUsedColumns();
			if(!qsFilterCols.containsAll(importColsFilters)) {
				// the import filter is not being overridden
				// so add it into the qs to sue
				qs.addImplicitFilter(filter);
			}
		}

		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		// setters
		qs.setEngine(this.qs.getEngine());
		qs.setRelations(this.qs.getRelations());
		qs.setBigDataEngine(this.qs.getBigDataEngine());
		qs.setCustomFrom(this.qs.getCustomFrom());
		qs.setCustomFromAliasName(this.qs.getCustomFromAliasName());
		// execution

		// we can cache a few different engine types
		boolean cache = true;
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
			this.logger.info("Executing query on engine " + engine.getEngineId());
			it = WrapperManager.getInstance().getRawWrapper(engine, qs);
			long end = System.currentTimeMillis();
			this.logger.info("Engine execution time = " + (end-start) + "ms");
			return it;
		}

		return it;
	}

	public boolean engineQueryCacheable() {
		return NativeFrame.cacheEngines.contains(this.qs.retrieveQueryStructEngine().getEngineType());
	}

	@Override
	public CachePropFileFrameObject save(String folderDir) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();

		String randFrameName = "Native" + Utility.getRandomString(6);
		cf.setFrameName(randFrameName);
		String frameFileName = folderDir + DIR_SEPARATOR + randFrameName + ".json";

		// save frame - this is just the QS
		StringWriter writer = new StringWriter();
		JsonWriter jWriter = new JsonWriter(writer);
		SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
		try {
			adapter.write(jWriter, this.qs);
			FileUtils.writeStringToFile(new File(frameFileName), writer.toString());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Error occured attempting to save native frame");
		}
		cf.setFrameCacheLocation(frameFileName);

		// also save the meta details
		this.saveMeta(cf, folderDir, randFrameName);
		return cf;
	}

	@Override
	public void open(CachePropFileFrameObject cf) {
		// load the frame
		// this is just the QS
		try {
			StringReader reader = new StringReader(FileUtils.readFileToString(new File(cf.getFrameCacheLocation())));
			JsonReader jReader = new JsonReader(reader);
			SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
			this.qs = adapter.read(jReader);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// open the meta details
		this.openCacheMeta(cf);
	}

	/******************************* UNNECESSARY ON NATIVE FRAME FOR NOW BUT NEED TO OVERRIDE *************************************************/

	@Override
	@Deprecated
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.DATA_CONNECTDB, "prerna.sablecc.DataConnectDBReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.NativeApiReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.NativeImportDataReactor");

		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");

		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.NativeApiReactor");
		return reactorNames;
	}

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
