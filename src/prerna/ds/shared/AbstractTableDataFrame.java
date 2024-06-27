package prerna.ds.shared;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.gson.GenRowFiltersAdapter;

public abstract class AbstractTableDataFrame implements ITableDataFrame {

	private static final Logger classLogger = LogManager.getLogger(AbstractTableDataFrame.class);
	
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	@Deprecated
	public static final String SELECTORS = "selectors";
	@Deprecated
	public static final String DE_DUP = "dedup";
	@Deprecated
	public static final String TEMPORAL_BINDINGS = "temporalBindings";

	// the meta data for the frame
	protected OwlTemporalEngineMeta metaData;

	// the name of the frame
	// note, in r/py this is also the variable name being used
	protected String frameName;
	// also store an original 
	// in case for anything like r/py we swapped the variable name out
	protected String originalName;
	
	// the header names persisted on the frame
	// this is taken from the frame
	// but it doesn't include prim keys
	protected String[] qsNames;
	
	// so that we do not need to re-execute on the frame multiple times
	// to determine if a header has duplicates or not
	protected Map<String, Boolean> uniqueColumnCache = new HashMap<String, Boolean>();
	protected Map<String, Double> uniqueColumnMaxCache = new HashMap<String, Double>();
	protected Map<String, Double> uniqueColumnMinCache = new HashMap<String, Double>();
	
	// the user id of the user who executed to create this frame
	// this has a lot of use for the specific implementation of H2Frame
	// H2Frame determines the schema to add the in-memory tables based on the userId
	protected transient String userId;

	// used to determine if the data id has been altered
	// this is only being updated when logic goes through pkql
	protected transient BigInteger dataId = BigInteger.valueOf(0);

	// this is used to persist filters within a frame
	protected transient GenRowFilters grf = new GenRowFilters();

	// this is used for correct logging based on the pixel passed
	protected transient Logger logger;
	
	// see if the frame has been closed
	protected boolean isClosed = false;
	
	protected transient Map<String, CachedIterator> queryCache = new HashMap<String, CachedIterator>();

	/**
	 * Constructor
	 */
	public AbstractTableDataFrame() {
		// create the OWL object
		this.metaData = new OwlTemporalEngineMeta();
		// we should define this just so we have a default logger
		this.logger = LogManager.getLogger(this.getClass().getName());
	}
	
	@Override
	public OwlTemporalEngineMeta getMetaData() {
		return this.metaData;
	}
	
	@Override
	public void setMetaData(OwlTemporalEngineMeta metaData) {
		this.metaData = metaData;
		this.syncHeaders();
	}
	
	@Override
	public void syncHeaders() {
		List<String> frameSelectors = this.metaData.getFrameSelectors();
		this.qsNames = frameSelectors.toArray(new String[frameSelectors.size()]);
	}
	
	@Override
	public Map<String, Object> getFrameHeadersObject(String... headerTypes) {
		// get types to include
		Map<String, Object> headersObj = this.metaData.getTableHeaderObjects(headerTypes);
		// now loop through and add if there are any filters on the header
		Set<String> filteredCols = this.getFrameFilters().getAllFilteredColumns();
		List<Map<String, Object>> headersMap = (List<Map<String, Object>>) headersObj.get("headers");
		for(Map<String, Object> headerMap : headersMap) {
			String alias = (String) headerMap.get("alias");
			String rawHeader = (String) headerMap.get("header");
			if(filteredCols.contains(alias) || filteredCols.contains(rawHeader)) {
				headerMap.put("isFiltered", true);
			} else {
				headerMap.put("isFiltered", false);
			}
		}
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("name", this.frameName);
		retMap.put("type", this.getFrameType().getTypeAsString());
		retMap.put("headerInfo", headersObj);
		return retMap;
	}
	
	////////////////////////////////////////////////////////////////////////////

	// logging
	
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	////////////////////////////////////////////////////////////////////////////

	////////////////////////////////////////////////////////////////////////////
	
	// caching methods for information on the frame
	
	@Override
	public Boolean isUniqueColumn(String columnHeader) {
		String uniqueColName = this.metaData.getUniqueNameFromAlias(columnHeader);
		if(uniqueColName == null) {
			uniqueColName = columnHeader;
		}
		if(!this.uniqueColumnCache.containsKey(uniqueColName)) {
			boolean isUnique = calculateIsUnqiueColumn(uniqueColName);
			this.uniqueColumnCache.put(uniqueColName, isUnique);
			return isUnique;
		}
		return this.uniqueColumnCache.get(uniqueColName);
	}
	
	protected Boolean calculateIsUnqiueColumn(String columnName) {
		// This reactor checks for duplicates
		boolean isUnique = false;

		// for a simple table
		// all we need to do is the following:
		// compare the count of a column
		// to the unique count of a column
		
		// calculate the count of a column
		SelectQueryStruct qs1 = new SelectQueryStruct();
		{
			QueryFunctionSelector countSelector = new QueryFunctionSelector();
			countSelector.setFunction(QueryFunctionHelper.COUNT);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if(columnName.contains("__")) {
				String[] split = columnName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(columnName);
				innerSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
			countSelector.addInnerSelector(innerSelector);
			qs1.addSelector(countSelector);
		}
		
		long nRow = 0;
		IRawSelectWrapper nRowIt = null;
		try {
			nRowIt = query(qs1);
			nRow = ((Number) nRowIt.next().getValues()[0]).longValue();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		} finally {
			if(nRowIt != null) {
				try {
					nRowIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// calculate the unique count of a column
		SelectQueryStruct qs2 = new SelectQueryStruct();
		{
			QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
			uniqueCountSelector.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
			uniqueCountSelector.setDistinct(true);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if(columnName.contains("__")) {
				String[] split = columnName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(columnName);
				innerSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
			uniqueCountSelector.addInnerSelector(innerSelector);
			qs2.addSelector(uniqueCountSelector);
		}
		
		long uniqueNRow = 0;
		IRawSelectWrapper uniqueNRowIt = null;
		try {
			uniqueNRowIt = query(qs2);
			uniqueNRow = ((Number) uniqueNRowIt.next().getValues()[0]).longValue();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		} finally {
			if(uniqueNRowIt != null) {
				try {
					uniqueNRowIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// if they are not equal, we have duplicates!
		isUnique = (long) nRow == (long) uniqueNRow;
		return isUnique;
	}
	
	/**
	 * Clear caching of any data on the frame
	 */
	@Override
	public void clearCachedMetrics() {
		this.uniqueColumnCache.clear();
		this.uniqueColumnMaxCache.clear();
		this.uniqueColumnMinCache.clear();
	}

	////////////////////////////////////////////////////////////////////////////

	@Override
	public boolean[] isNumeric() {
		String[] headers = getQsHeaders();
		int size = headers.length;
		boolean[] isNumeric = new boolean[size];
		for(int i = 0; i < size; i++) {
			isNumeric[i] = isNumeric(headers[i]);
		}
		return isNumeric;
	}

	@Override
	public boolean isNumeric(String name) {
		String uniqueName = this.metaData.getUniqueNameFromAlias(name);
		if(uniqueName == null) {
			uniqueName = name;
		}
		SemossDataType dataType = this.metaData.getHeaderTypeAsEnum(uniqueName);
		return (dataType.equals(SemossDataType.INT) || dataType.equals(SemossDataType.DOUBLE));
	}

	@Override
	public String[] getColumnHeaders() {
		List<String> fHeaders = this.metaData.getOrderedAliasOrUniqueNames();
		return fHeaders.toArray(new String[fHeaders.size()]);
	}
	
	@Override
	public String[] getQsHeaders() {
		List<String> selectors = getSelectors();
		return selectors.toArray(new String[selectors.size()]);
	}

	@Override
	public Object[] getColumn(String columnHeader) {
		QueryColumnSelector colSelector = new QueryColumnSelector(columnHeader);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(colSelector);
		// dont forget about filters
		qs.setExplicitFilters(this.grf);
		
		List<Object> values = new ArrayList<Object>();
		IRawSelectWrapper it = null;
		try {
			it = query(qs);
			while(it.hasNext()) {
				values.add(it.next().getValues()[0]);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return values.toArray();
	}
	
	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		QueryColumnSelector colSelector = new QueryColumnSelector(columnHeader);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(colSelector);
		qs.setExplicitFilters(this.grf);
		
		List<Double> values = new ArrayList<Double>();
		IRawSelectWrapper it = null;
		try {
			it = query(qs);
			while(it.hasNext()) {
				values.add( ((Number) it.next().getValues()[0]).doubleValue());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return values.toArray(new Double[values.size()]);
	}

	public Double[] getMax() {
		int size = qsNames.length;
		Double[] max = new Double[size];
		for(int i = 0; i < size; i++) {
			if(isNumeric(qsNames[i])) {
				max[i] = getMax(qsNames[i]);
			}
		}

		return max;
	}

	public Double getMax(String columnHeader) {
		String uniqueColName = this.metaData.getUniqueNameFromAlias(columnHeader);
		if(uniqueColName == null) {
			uniqueColName = columnHeader;
		}
		if(!this.uniqueColumnMaxCache.containsKey(uniqueColName)) {
			Double min =  calculateMax(uniqueColName);
			this.uniqueColumnMaxCache.put(uniqueColName, min);
			return min;
		}
		return this.uniqueColumnMaxCache.get(uniqueColName);
	}
	
	protected Double calculateMax(String columnHeader) {
		SemossDataType dataType = this.metaData.getHeaderTypeAsEnum(columnHeader);
		if (dataType == SemossDataType.INT || dataType == SemossDataType.DOUBLE) {
			QueryColumnSelector innerSelector = new QueryColumnSelector(columnHeader);
			QueryFunctionSelector mathSelector = new QueryFunctionSelector();
			mathSelector.addInnerSelector(innerSelector);
			mathSelector.setFunction(QueryFunctionHelper.MAX);

			SelectQueryStruct mathQS = new SelectQueryStruct();
			mathQS.addSelector(mathSelector);
			// dont forget to add the current frame filters!
			mathQS.setExplicitFilters(this.grf);

			IRawSelectWrapper it = null;
			try {
				it = query(mathQS);
				while(it.hasNext()) {
					return ((Number) it.next().getValues()[0]).doubleValue();
				}
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
			
		}
		return null;
	}
	
	public Double[] getMin() {
		int size = qsNames.length;
		Double[] min = new Double[size];
		for(int i = 0; i < size; i++) {
			if(isNumeric(qsNames[i])) {
				min[i] = getMin(qsNames[i]);
			}
		}

		return min;
	}
	
	public Double getMin(String columnHeader) {
		String uniqueColName = this.metaData.getUniqueNameFromAlias(columnHeader);
		if(uniqueColName == null) {
			uniqueColName = columnHeader;
		}
		if(!this.uniqueColumnMinCache.containsKey(uniqueColName)) {
			Double min =  calculateMin(uniqueColName);
			this.uniqueColumnMinCache.put(uniqueColName, min);
			return min;
		}
		return this.uniqueColumnMinCache.get(uniqueColName);
	}
	
	protected Double calculateMin(String columnHeader) {
		SemossDataType dataType = this.metaData.getHeaderTypeAsEnum(columnHeader);
		if (dataType == SemossDataType.INT || dataType == SemossDataType.DOUBLE) {
			QueryColumnSelector innerSelector = new QueryColumnSelector(columnHeader);
			QueryFunctionSelector mathSelector = new QueryFunctionSelector();
			mathSelector.addInnerSelector(innerSelector);
			mathSelector.setFunction(QueryFunctionHelper.MIN);

			SelectQueryStruct mathQS = new SelectQueryStruct();
			mathQS.addSelector(mathSelector);
			// dont forget to add the current frame filters!
			mathQS.setExplicitFilters(this.grf);

			IRawSelectWrapper it = null;
			try {
				it = query(mathQS);
				while(it.hasNext()) {
					return ((Number) it.next().getValues()[0]).doubleValue();
				}
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
		}
		return null;
	}

	public List<String> getSelectors() {
		List<String> selectors = new ArrayList<String>();
		if(this.qsNames == null || this.qsNames.length == 0) {
			List<String> frameSelectors = this.metaData.getFrameSelectors();
			this.qsNames = frameSelectors.toArray(new String[frameSelectors.size()]);
		}
		if(this.qsNames != null) {
			for(int i = 0; i < this.qsNames.length; i++) {
				selectors.add(this.qsNames[i]);
			}
		}
		return selectors;
	}

	@Override
	public void setUserId(String userId) {
		this.userId = userId;
	}

	@Override
	public String getUserId() {
		return this.userId;
	}

	@Override
	public void updateDataId() {
		this.dataId = this.dataId.add(BigInteger.valueOf(1));
	}

	@Override
	public int getDataId() {
		return this.dataId.intValue();
	}

	@Override
	public void resetDataId() {
		this.dataId = BigInteger.valueOf(0);
	}

	@Override
	public String getName() {
		return this.frameName;
	}
	
	@Override
	public void setName(String name) {
		if(name != null && !name.isEmpty()) {
			this.frameName = name;
		}
	}
	
	@Override
	public String getOriginalName() {
		return this.originalName;
	}
	
	@Override
	public void setOriginalName(String name) {
		if(name != null && !name.isEmpty()) {
			this.originalName = name;
		}
	}

	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////

	/*
	 * Filter done through generic grf construct which is interpreted through 
	 * the interpreters to generate the appropriate query string
	 */

	@Override
	public GenRowFilters getFrameFilters() {
		return this.grf;
	}
	
	@Override
	public void setFrameFilters(GenRowFilters filter) {
		this.grf = filter;
	}


	@Override
	public void addFilter(GenRowFilters filter) {
		this.grf.merge(filter);
		this.uniqueColumnCache.clear();
		this.clearCachedMetrics();
	}
	
	@Override
	public void addFilter(IQueryFilter filter) {
		this.grf.merge(filter);
		this.clearCachedMetrics();
	}

	@Override
	public void setFilter(GenRowFilters filter) {
		Set<String> allColsUsed = filter.getAllFilteredColumns();
		this.grf.removeColumnFilters(allColsUsed);
		this.grf.merge(filter);
		this.clearCachedMetrics();
	}

	@Override
	public boolean unfilter(String columnHeader) {
		boolean foundFiltersToRemove = this.grf.removeColumnFilter(columnHeader);
		if(foundFiltersToRemove) {
			this.clearCachedMetrics();
		}
		return foundFiltersToRemove;
	}

	@Override
	public boolean unfilter() {
		if(!this.grf.isEmpty()) {
			this.grf.removeAllFilters();
			this.clearCachedMetrics();
			return true;
		}
		return false;
	}
	
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////

	/*
	 * Caching methods
	 */	
	
	protected void saveMeta(CachePropFileFrameObject cf, String folderDir, String fileName, Cipher cipher) throws IOException {
		// save frame metadata
		String metaFileName = folderDir + DIR_SEPARATOR + "METADATA__" + fileName + ".owl";
		this.metaData.save(metaFileName, cipher);
		cf.setFrameMetaCacheLocation(metaFileName);
		
		// save the frame filters
		List<IQueryFilter> filters = this.grf.getFilters();
		if(!filters.isEmpty()) {
			String frameStateFileName = folderDir + DIR_SEPARATOR + "FRAME_STATE__" + fileName + ".json";
			Writer writer = null;
			if(cipher != null) {
				writer = new OutputStreamWriter(new CipherOutputStream(new FileOutputStream(new File(Utility.normalizePath(frameStateFileName))), cipher));
			} else {
				writer = new OutputStreamWriter(new FileOutputStream(new File(Utility.normalizePath(frameStateFileName))));
			}
			JsonWriter jWriter = new JsonWriter(writer);
			GenRowFiltersAdapter adapter = new GenRowFiltersAdapter();
			try {
				adapter.write(jWriter, this.grf);
			} catch (IOException e) {
				throw new IOException("Error occurred trying to save filter state on frame");
			} finally {
				jWriter.close();
			}
			cf.setFrameStateCacheLocation(frameStateFileName);
		}
		
		// save frame type
		String frameType = this.getClass().getName();
		cf.setFrameType(frameType);
		
		// save frame name
		cf.setFrameName(this.frameName);
	}
	
	protected void openCacheMeta(CachePropFileFrameObject cf, Cipher cipher) {
		// set the frame name
		this.frameName = cf.getFrameName();
		
		//load owl meta
		this.metaData = new OwlTemporalEngineMeta(cf.getFrameMetaCacheLocation(), cipher);
		syncHeaders();
		
		String frameStateFileName = cf.getFrameStateCacheLocation();
		if(frameStateFileName != null) {
			Reader reader = null;
			try {
				if(cipher != null) {
					reader = new InputStreamReader(new CipherInputStream(new FileInputStream(new File(frameStateFileName)), cipher));
				} else {
					reader = new InputStreamReader(new FileInputStream(new File(frameStateFileName)));
				}
				JsonReader jReader = new JsonReader(reader);
				GenRowFiltersAdapter adapter = new GenRowFiltersAdapter();
				this.grf = adapter.read(jReader);
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
			}
		}
	}
	
	@Override
	public void close() {
		this.metaData.close();
		this.isClosed = true;
		logger.debug("Successfully dropped frame metadata");
	}
	
	@Override
	public boolean isClosed() {
		return this.isClosed;
	}
	
	@Override
	protected void finalize() throws Throwable {
		logger.info("ITableDataFrame " + this.frameName + " is being gc'd");
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * The land of deprecated methods
	 * 
	 * We only have these so we dont have compilation errors in unused classes...
	 * hopefully when we get to do some major clean up of playsheets/routines, we can get rid of them
	 * 
	 */
	
	@Override
	public List<Object[]> getData() {
		// get a flat QS
		// which contains all the selectors 
		// and all the joins as inner 
		SelectQueryStruct qs = this.metaData.getFlatTableQs(false);
		// add the frame filters
		qs.mergeImplicitFilters(this.grf);
		
		List<Object[]> data = new ArrayList<Object[]>();
		IRawSelectWrapper it = null;
		try {
			it = this.query(qs);
			while(it.hasNext()) {
				data.add(it.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return data;
	}
	
	@Override
	@Deprecated
	public IRawSelectWrapper iterator() {
		// get a flat QS
		// which contains all the selectors 
		// and all the joins as inner 
		SelectQueryStruct qs = this.metaData.getFlatTableQs(false);
		// add the frame filters
		qs.mergeImplicitFilters(this.grf);
		
		try {
			return this.query(qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnName, List<String> attributeUniqueHeaderName) {
		int numSelectors = attributeUniqueHeaderName.size();
		List<SemossDataType> dataTypes = new Vector<SemossDataType>();
		Double[] max = new Double[numSelectors];
		Double[] min = new Double[numSelectors];
		
		for (int i = 0; i < numSelectors; i++) {
			String uniqueHeader = this.metaData.getUniqueNameFromAlias(attributeUniqueHeaderName.get(i));
			if(uniqueHeader == null) {
				uniqueHeader = attributeUniqueHeaderName.get(i);
			}
			SemossDataType dataType = this.metaData.getHeaderTypeAsEnum(uniqueHeader);
			dataTypes.add(dataType);
			if(dataType == SemossDataType.INT || dataType == SemossDataType.DOUBLE) {
				max[i] = getMax(uniqueHeader);
				min[i] = getMin(uniqueHeader);
			}
		}

		ScaledUniqueFrameIterator iterator = new ScaledUniqueFrameIterator(this, columnName, max, min, dataTypes, attributeUniqueHeaderName);
		return iterator;
	}
	
	@Override
	public int getUniqueInstanceCount(String columnName) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector count = new QueryFunctionSelector();
		count.setDistinct(true);
		count.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
		QueryColumnSelector inner = new QueryColumnSelector();
		if(columnName.contains("__")) {
			String[] split = columnName.split("__");
			inner.setTable(split[0]);
			inner.setColumn(split[1]);
		} else {
			inner.setTable(columnName);
			inner.setColumn(null);
		}
		count.addInnerSelector(inner);
		qs.addSelector(count);
		IRawSelectWrapper it = null;
		try {
			it = query(qs);
			while(it.hasNext()) {
				Object numUnique = it.next().getValues()[0];
				return ((Number) numUnique).intValue();
			}
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
		return 0;
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Cache query methods
	 */
	
	@Override
	public void clearQueryCache() {
		this.queryCache.clear();
	}
	
	@Override
	public void cacheQuery(CachedIterator it) {
		if(it.hasNext()) {	
			queryCache.put(it.getQuery(), it);
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Even worse... deprecated DataMakerComponent stuff
	 */
	
	@Override
	@Deprecated
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<String, String>();
//		reactorNames.put(PKQLReactor.DATA_FRAME_HEADER.toString(), "prerna.sablecc.DataFrameHeaderReactor");
//		reactorNames.put(PKQLEnum.COL_RENAME, "prerna.sablecc.ColRenameReactor");
//		reactorNames.put(PKQLEnum.REMOTE_RDBMS_QUERY_API, "prerna.sablecc.RemoteRdbmsQueryApiReactor");
		return reactorNames;
	}
		
	@Override
	@Deprecated
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms) {
		logger.info("We are processing " + transforms.size() + " pre transformations");
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(this);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}

	@Override
	@Deprecated
	public void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame) {
		logger.info("We are processing " + transforms.size() + " post transformations");
		// if other data frames present, create new array with this at position 0
		IDataMaker[] extendedArray = new IDataMaker[]{this};
		if(dataFrame.length > 0) {
			extendedArray = new IDataMaker[dataFrame.length + 1];
			extendedArray[0] =  this;
			for(int i = 0; i < dataFrame.length; i++) {
				extendedArray[i+1] = dataFrame[i];
			}
		}
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(extendedArray);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
			//			this.join(dataFrame, transform.getOptions().get(0).getSelected()+"", transform.getOptions().get(1).getSelected()+"", 1.0, (IAnalyticRoutine)transform);
			//			LOGGER.info("welp... we've got our new table... ");
		}
	}

	@Override
	@Deprecated
	public Map<String, Object> getDataMakerOutput(String... selectors) {
		Hashtable retHash = new Hashtable();
		if(selectors.length == 0) {
			List<Object[]> retVector = new Vector<Object[]>();
			String[] headers = new String[1];
			if(this.qsNames != null && this.qsNames.length > 0) {
				//				retVector = this.getData();
				headers = this.qsNames;
			}
			retHash.put("data", retVector);
			retHash.put("headers", headers);
		} else {
			long startTime = System.currentTimeMillis();

			Vector<Object[]> retVector = new Vector<Object[]>();
			Map<String, Object> options = new HashMap<String, Object>();
			options.put(TinkerFrame.SELECTORS, Arrays.asList(selectors));
			options.put(TinkerFrame.DE_DUP, true);
			//			Iterator<Object[]> iterator = this.iterator(options);
			//			while(iterator.hasNext()) {
			//				retVector.add(iterator.next());
			//			}
			retHash.put("data", retVector);
			retHash.put("headers", selectors);

			logger.info("Collected Raw Data: "+(System.currentTimeMillis() - startTime));
		}
		return retHash;
	}
	
	@Deprecated
	public String getFilterString() {
		return "";
	}
	
	// need to be overridden by specific frame
	public Object querySQL(String query) {
		throw new IllegalArgumentException("Method not implemented for frame = " + this.getClass().getSimpleName());
	}

	// need to be overridden by specific frame
	public Object queryCSV(String query) {
		throw new IllegalArgumentException("Method not implemented for frame = " + this.getClass().getSimpleName());
	}

	// need to be overridden by specific frame
	public Object queryJSON(String query) {
		throw new IllegalArgumentException("Method not implemented for frame = " + this.getClass().getSimpleName());
	}

	// need to be overridden by specific frames
	public String createVarFrame() {
		throw new IllegalArgumentException("Method not implemented for frame = " + this.getClass().getSimpleName());
	}

//	@Override
//	@Deprecated
//	public List<Object> processActions(DataMakerComponent dmc, List<ISEMOSSAction> actions, IDataMaker... dataMaker) {
//		LOGGER.info("We are processing " + actions.size() + " actions");
//		List<Object> outputs = new ArrayList<Object>();
//		for(ISEMOSSAction action : actions){
//			action.setDataMakers(this);
//			action.setDataMakerComponent(dmc);
//			outputs.add(action.runMethod());
//		}
//		algorithmOutput.addAll(outputs);
//		return outputs;
//	}
//
//	@Override
//	@Deprecated
//	public List<Object> getActionOutput() {
//		return this.algorithmOutput;
//	}

	/*@Override
	@Deprecated
	public void performAnalyticTransformation(IAnalyticTransformationRoutine routine) throws RuntimeException {
		routine.runAlgorithm(this);
	}

	@Override
	@Deprecated
	public void performAnalyticAction(IAnalyticActionRoutine routine) throws RuntimeException {
		routine.runAlgorithm(this);
	}*/

}
