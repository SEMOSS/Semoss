package prerna.ds.shared;

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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.GenRowFilters;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryAggregationEnum;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryMathSelector;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc2.om.QueryFilter;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

public abstract class AbstractTableDataFrame implements ITableDataFrame {

	public static final String LIMIT = "limit";
	public static final String OFFSET = "offset";
	public static final String SELECTORS = "selectors";
	public static final String SORT_BY = "sortColumn";
	public static final String SORT_BY_DIRECTION = "sortDirection";
	public static final String DE_DUP = "dedup";
	public static final String TEMPORAL_BINDINGS = "temporalBindings";
	public static final String IGNORE_FILTERS = "ignoreFilters";

	// the meta data for the frame
	protected OwlTemporalEngineMeta metaData;

	// the header names persisted on the frame
	// this is taken from the frame
	// but it doesn't include prim keys
	protected String[] qsNames;
	
	// so that we do not need to re-execute on the frame multiple times
	// to determine if a header has duplicates or not
	protected Map<String, Boolean> uniqueColumnCache = new HashMap<String, Boolean>();
	
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
		this.qsNames = this.metaData.getFrameSelectors().toArray(new String[]{});
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
	public Boolean isUniqueColumn(String columnName) {
		if(!this.uniqueColumnCache.containsKey(columnName)) {
			this.uniqueColumnCache.put(columnName, calculateIsUnqiueColumn(columnName));
		}
		return this.uniqueColumnCache.get(columnName);
	}
	
	protected Boolean calculateIsUnqiueColumn(String columnName) {
		// This reactor checks for duplicates
		boolean isUnique = false;

		// for a simple table
		// all we need to do is the following:
		// compare the count of a column
		// to the unique count of a column
		
		// calculate the count of a column
		QueryStruct2 qs1 = new QueryStruct2();
		{
			QueryMathSelector countSelector = new QueryMathSelector();
			countSelector.setMath(QueryAggregationEnum.COUNT);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if(columnName.contains("__")) {
				String[] split = columnName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(columnName);
				innerSelector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
			}
			countSelector.setInnerSelector(innerSelector);
			qs1.addSelector(countSelector);
		}
		Iterator<IHeadersDataRow> nRowIt = query(qs1);
		long nRow = ((Number) nRowIt.next().getValues()[0]).longValue();

		// calculate the unique count of a column
		QueryStruct2 qs2 = new QueryStruct2();
		{
			QueryMathSelector uniqueCountSelector = new QueryMathSelector();
			uniqueCountSelector.setMath(QueryAggregationEnum.UNIQUE_COUNT);
			uniqueCountSelector.setDistinct(true);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if(columnName.contains("__")) {
				String[] split = columnName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(columnName);
				innerSelector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
			}
			uniqueCountSelector.setInnerSelector(innerSelector);
			qs2.addSelector(uniqueCountSelector);
		}
		Iterator<IHeadersDataRow> uniqueNRowIt = query(qs2);
		long uniqueNRow = ((Number) uniqueNRowIt.next().getValues()[0]).longValue();

		// if they are not equal, we have duplicates!
		isUnique = (long) nRow == (long) uniqueNRow;
		return isUnique;
	}
	
	/**
	 * Clear caching of any data on the frame
	 */
	@Override
	public void clearCachedInfo() {
		// right now, we only cache the duplicates
		this.uniqueColumnCache.clear();
	}

	////////////////////////////////////////////////////////////////////////////

	@Override
	public void renameColumn(String oldColumnHeader, String newColumnHeader) {
		//		metaData.setVertexAlias(oldColumnHeader, newColumnHeader);
		//		
		//		List<String> fullNames = this.metaData.getColumnNames();
		//    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
	}

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
		DATA_TYPES dataType = null;
		if(uniqueName.contains("__")) {
			dataType = this.metaData.getHeaderTypeAsEnum(uniqueName, uniqueName.split("__")[0]);
		} else {
			dataType = this.metaData.getHeaderTypeAsEnum(uniqueName, null);
		}
		return dataType.equals(IMetaData.DATA_TYPES.NUMBER);
	}

	@Override
	public String[] getColumnHeaders() {
		return this.metaData.getFrameColumnNames().toArray(new String[]{});
	}
	
	@Override
	public String[] getQsHeaders() {
		return getSelectors().toArray(new String[]{});
	}

	@Override
	public Object[] getColumn(String columnHeader) {
		QueryColumnSelector colSelector = new QueryColumnSelector();
		if(columnHeader.contains("__")) {
			String[] split = columnHeader.split("__");
			colSelector.setTable(split[0]);
			colSelector.setColumn(split[1]);
		} else {
			colSelector.setTable(columnHeader);
			colSelector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
		}
		
		QueryStruct2 qs = new QueryStruct2();
		qs.addSelector(colSelector);
		// dont forget about filters
		qs.setFilters(this.grf);
		Iterator<IHeadersDataRow> it = query(qs);
		
		List<Object> values = new ArrayList<Object>();
		while(it.hasNext()) {
			values.add(it.next().getValues()[0]);
		}
		
		return values.toArray();
	}
	
	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		QueryColumnSelector colSelector = new QueryColumnSelector();
		if(columnHeader.contains("__")) {
			String[] split = columnHeader.split("__");
			colSelector.setTable(split[0]);
			colSelector.setColumn(split[1]);
		} else {
			colSelector.setTable(columnHeader);
			colSelector.setTable(null);
		}
		
		QueryStruct2 qs = new QueryStruct2();
		qs.addSelector(colSelector);
		// dont forget about filters
		qs.setFilters(this.grf);
		Iterator<IHeadersDataRow> it = query(qs);
		
		List<Double> values = new ArrayList<Double>();
		while(it.hasNext()) {
			values.add( ((Number) it.next().getValues()[0]).doubleValue());
		}
		
		return values.toArray(new Double[]{});
	}

	@Override
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

	@Override
	public Double getMax(String columnHeader) {
		String uniqueColName = this.metaData.getUniqueNameFromAlias(columnHeader);
		if(uniqueColName == null) {
			uniqueColName = columnHeader;
		}
		if (this.metaData.getHeaderTypeAsEnum(uniqueColName) == IMetaData.DATA_TYPES.NUMBER) {
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if(uniqueColName.contains("__")) {
				String[] split = uniqueColName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(uniqueColName);
				innerSelector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
			}

			QueryMathSelector mathSelector = new QueryMathSelector();
			mathSelector.setInnerSelector(innerSelector);
			mathSelector.setMath(QueryAggregationEnum.MAX);

			QueryStruct2 mathQS = new QueryStruct2();
			mathQS.addSelector(mathSelector);
			// dont forget to add the current frame filters!
			mathQS.setFilters(this.grf);

			Iterator<IHeadersDataRow> it = query(mathQS);
			while(it.hasNext()) {
				return ((Number) it.next().getValues()[0]).doubleValue();
			}
		}
		return null;
	}
	
	@Override
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
	
	@Override
	public Double getMin(String columnHeader) {
		String uniqueColName = this.metaData.getUniqueNameFromAlias(columnHeader);
		if(uniqueColName == null) {
			uniqueColName = columnHeader;
		}
		if (this.metaData.getHeaderTypeAsEnum(uniqueColName) == IMetaData.DATA_TYPES.NUMBER) {
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if(uniqueColName.contains("__")) {
				String[] split = uniqueColName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(uniqueColName);
				innerSelector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
			}
			QueryMathSelector mathSelector = new QueryMathSelector();
			mathSelector.setInnerSelector(innerSelector);
			mathSelector.setMath(QueryAggregationEnum.MIN);

			QueryStruct2 mathQS = new QueryStruct2();
			mathQS.addSelector(mathSelector);
			// dont forget to add the current frame filters!
			mathQS.setFilters(this.grf);

			Iterator<IHeadersDataRow> it = query(mathQS);
			while(it.hasNext()) {
				return ((Number) it.next().getValues()[0]).doubleValue();
			}
		}
		return null;
	}

	public List<String> getSelectors() {
		List<String> selectors = new ArrayList<String>();
		if(this.qsNames == null || this.qsNames.length == 0) {
			this.qsNames = this.metaData.getFrameSelectors().toArray(new String[]{});
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

	public void resetDataId() {
		this.dataId = BigInteger.valueOf(0);
	}

	public String getTableName()
	{
		return null;
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
	public void addFilter(GenRowFilters filter) {
		this.grf.merge(filter);
		this.uniqueColumnCache.clear();
		this.clearCachedInfo();
	}
	
	@Override
	public void addFilter(QueryFilter filter) {
		this.grf.merge(filter);
		this.clearCachedInfo();
	}

	@Override
	public void setFilter(GenRowFilters filter) {
		Set<String> allColsUsed = filter.getAllFilteredColumns();
		this.grf.removeColumnFilters(allColsUsed);
		this.grf.merge(filter);
		this.clearCachedInfo();
	}

	@Override
	public boolean unfilter(String columnHeader) {
		boolean foundFiltersToRemove = this.grf.removeColumnFilter(columnHeader);
		if(foundFiltersToRemove) {
			this.clearCachedInfo();
		}
		return foundFiltersToRemove;
	}

	@Override
	public boolean unfilter() {
		if(!this.grf.isEmpty()) {
			this.grf.removeAllFilters();
			this.clearCachedInfo();
			return true;
		}
		return false;
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
		QueryStruct2 qs = this.metaData.getFlatTableQs();
		// add the frame filters
		qs.mergeFilters(this.grf);
		
		Iterator<IHeadersDataRow> it = this.query(qs);
		List<Object[]> data = new ArrayList<Object[]>();
		while(it.hasNext()) {
			data.add(it.next().getValues());
		}
		return data;
	}
	
	@Override
	public Iterator<IHeadersDataRow> iterator() {
		// get a flat QS
		// which contains all the selectors 
		// and all the joins as inner 
		QueryStruct2 qs = this.metaData.getFlatTableQs();
		// add the frame filters
		qs.mergeFilters(this.grf);
		
		return this.query(qs);
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnName, List<String> attributeUniqueHeaderName) {
		int numSelectors = attributeUniqueHeaderName.size();
		List<IMetaData.DATA_TYPES> dataTypes = new Vector<IMetaData.DATA_TYPES>();
		Double[] max = new Double[numSelectors];
		Double[] min = new Double[numSelectors];
		
		for (int i = 0; i < numSelectors; i++) {
			String uniqueHeader = this.metaData.getUniqueNameFromAlias(attributeUniqueHeaderName.get(i));
			if(uniqueHeader == null) {
				uniqueHeader = attributeUniqueHeaderName.get(i);
			}
			DATA_TYPES dataType = this.metaData.getHeaderTypeAsEnum(uniqueHeader);
			dataTypes.add(dataType);
			if(dataType == DATA_TYPES.NUMBER) {
				max[i] = getMax(uniqueHeader);
				min[i] = getMin(uniqueHeader);
			}
		}

		ScaledUniqueFrameIterator iterator = new ScaledUniqueFrameIterator(this, columnName, max, min, dataTypes, attributeUniqueHeaderName);
		return iterator;
	}
	
	@Override
	public int getUniqueInstanceCount(String columnName) {
		QueryStruct2 qs = new QueryStruct2();
		QueryMathSelector count = new QueryMathSelector();
		count.setDistinct(true);
		count.setMath(QueryAggregationEnum.UNIQUE_COUNT);
		QueryColumnSelector inner = new QueryColumnSelector();
		if(columnName.contains("__")) {
			String[] split = columnName.split("__");
			inner.setTable(split[0]);
			inner.setColumn(split[1]);
		} else {
			inner.setTable(columnName);
			inner.setColumn(null);
		}
		count.setInnerSelector(inner);
		qs.addSelector(count);
		Iterator<IHeadersDataRow> it = query(qs);
		while(it.hasNext()) {
			Object numUnique = it.next().getValues()[0];
			return ((Number) numUnique).intValue();
		}
		return 0;
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
		reactorNames.put(PKQLReactor.DATA_FRAME_HEADER.toString(), "prerna.sablecc.DataFrameHeaderReactor");
		reactorNames.put(PKQLEnum.COL_RENAME, "prerna.sablecc.ColRenameReactor");
		reactorNames.put(PKQLEnum.REMOTE_RDBMS_QUERY_API, "prerna.sablecc.RemoteRdbmsQueryApiReactor");
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
