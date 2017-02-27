package prerna.ds;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticActionRoutine;
import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectStatement;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.ArrayUtilityMethods;

public abstract class AbstractTableDataFrame implements ITableDataFrame {
	
	private static final Logger LOGGER = LogManager.getLogger(AbstractTableDataFrame.class.getName());

	// types of comparators
	public enum Comparator {
		EQUAL, LESS_THAN, GREATER_THAN, LESS_THAN_EQUAL, GREATER_THAN_EQUAL, NOT_EQUAL, IS_NULL, IS_NOT_NULL;
	}
	
	public enum VALUE {
		NULL;
	}
	
	public static final String LIMIT = "limit";
	public static final String OFFSET = "offset";
	public static final String SELECTORS = "selectors";
	public static final String SORT_BY = "sortColumn";
	public static final String SORT_BY_DIRECTION = "sortDirection";
	public static final String DE_DUP = "dedup";
	public static final String TEMPORAL_BINDINGS = "temporalBindings";
	public static final String IGNORE_FILTERS = "ignoreFilters";

	// the meta data for the frame
	protected IMetaData metaData;
	
	// the header names persisted on the frame
	// this is taken from the frame
	// but it doesn't include prim keys
	protected String[] headerNames;
	
	// TODO: once actions are moved to PKQL, won't need to keep this
	// examples include: numerical_correlation, classification, matrix_regression, association_learning
	// we keep a set of algorithm outputs on the frame
	protected List<Object> algorithmOutput = new Vector<Object>();
	
	// this is used to determine the list of columns to skip 
	protected List<String> columnsToSkip = new Vector<String>(); //make a set?
	
	// the user id of the user who executed to create this frame
	// this has a lot of use for the specific implementation of H2Frame
	// H2Frame determines the schema to add the in-memory tables based on the userId
	protected String userId;
	
	// used to determine if the data id has been altered
	// this is only being updated when logic goes through pkql
	protected BigInteger dataId = BigInteger.valueOf(0);
	
	///////////////////////// merge edge hash methods ///////////////////////////////////////
	
	/*
	 * There are two main types of merge edge hash methods
	 * 1) merge edge hash for new data being added to a frame
	 * 		-> this is used when adding a new column, i.e. when a group by result is added to the frame
	 * 2) merge edge hash for new data coming from an engine
	 * 		-> this does a lot more as it loads engine specific properties onto the metadata
	 */
	@Override
	public void renameColumn(String oldColumnHeader, String newColumnHeader) {
		metaData.setVertexAlias(oldColumnHeader, newColumnHeader);
		
		List<String> fullNames = this.metaData.getColumnNames();
    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
	}
	
	@Override
	/**
	 * Merges the inputed edge hash with the existing metadata in the frame
	 * @param edgeHash						The edge hash to merge into the existing meta data
	 * @param dataTypeMap					The data type for each entry in the edgeHash
	 */
	public void mergeEdgeHash(Map<String, Set<String>> edgeHash, Map<String, String> dataTypeMap) {
		// combine the new values into the meta data
		TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash);

		// store the data types for the new columns added
		// sets any missing data type to be a string
		mergeDataTypeMap(dataTypeMap);
	
		// update the list of header names inside the data frame
    	List<String> fullNames = this.metaData.getColumnNames();
    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
	}
	
	/**
	 * Merges the inputed edge hash with the existing metadata in the frame
	 * @param edgeHash						The edge hash to merge into the existing meta data
 	 * @param node2ValueHash				The name for each entry in the edgeHash for how it will be called in the data frame
	 * @param dataTypeMap					The data type for each entry in the edgeHash
	 */
	public void mergeEdgeHash(Map<String, Set<String>> edgeHash, Map<String, String> node2ValueHash, Map<String, String> dataTypeMap) {
		// this will combine the new values into the meta data
		// but it will also use the node2ValueHash to have define a specific definition for how
		// the column is defined within the frame
		// this is important for H2Frame since column names have character restrictions
		TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash, node2ValueHash);

		// store the data types for the new columns added
		// sets any missing data type to be a string
		mergeDataTypeMap(dataTypeMap);
	
		// update the list of header names inside the data frame
    	List<String> fullNames = this.metaData.getColumnNames();
    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
	}
	
	/**
	 * Store the data types associated with new headers that are created during mergeEdgeHash
	 * @param dataTypeMap
	 */
	public void mergeDataTypeMap(Map<String, String> dataTypeMap) {
		// sets any missing data type to be a string
		if(dataTypeMap != null) {
			for(String key : dataTypeMap.keySet()) {
				String type = dataTypeMap.get(key);
				if(type == null) type = "STRING";
				this.metaData.storeDataType(key, type);
			}
		}
	}

	@Override
	/**
	 * Merges the inputed edge hash with the existing metadata in the frame
	 * @param edgeHash						The edge hash to merge into the existing meta data. The edge hash contains the 
	 * 										query struct names for the query input
	 * 										Example edge hash is:
	 * 										{ Title -> [Title__Movie_Budget, Studio] } ; where Movie_Budget is a property on Title
	 * @param engine						The engine where the columns in the edge hash came from
	 * @param joinCols						The join columns for the merging
	 * 										This enables that we can declare columns to be equivalent between the existing frame
	 * 										and those we are going to add to the frame via the merge without them needing to be 
	 * 										exact matches
	 * @return								Return a map array containing the following
	 * 										index 0: this map contains a clean version of the edgeHash. the clean version is the 
	 * 											edge hash contains all the logical names (display names) as defined by the engine.
	 * 										index 1: this map contains the logical name (matching that in the clean edge hash at
	 * 											index 0 of the map array) pointing to the unique name of the column within the 
	 * 											metadata
	 */
	public Map[] mergeQSEdgeHash(Map<String, Set<String>> edgeHash, IEngine engine, Vector<Map<String, String>> joinCols, Map<String, Boolean> makeUniqueNameMap) {
		// this method handles all the complexity of adding the new headers, adding the engine properties, and returning the 
		// map array which is utilized by ImportDataReactor to add data onto the frame
		Map[] ret =  TinkerMetaHelper.mergeQSEdgeHash(this.metaData, edgeHash, engine, joinCols, makeUniqueNameMap);

		// update the list of header names inside the data frame
    	List<String> fullNames = this.metaData.getColumnNames();
    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
    	
    	// return the map array
    	return ret;
	}
	
	///////////////////////// end merge edge hash methods ///////////////////////////////////////

	

	/////////////////////// connect type methods ///////////////////////////////////
	
	/*
	 * The connect type method are just wrappers around the mergeEdgeHash methods
	 * This just makes it easier to have simple inputs that creates the necessary edge maps
	 * to merge instead of creating those maps in multiple pieces of the code...
	 * this is used within the generic ColAddReactor and frame specific variant
	 */

	@Override
	public void connectTypes(String[] outTypes, String inType, Map<String, String> dataTypeMap) {
		if(outTypes.length == 1) {
			connectTypes(outTypes[0], inType, dataTypeMap);
		} else {
			Map<String, Set<String>> edgeHash = new HashMap<>();
			
			//point each outType to the prim key
			String metaPrimKey = TinkerMetaHelper.getMetaPrimaryKeyName(outTypes);
			Set<String> primSet = new HashSet<>(1);
			primSet.add(metaPrimKey);
			for(String outType : outTypes) {
				edgeHash.put(outType, primSet);
			}
			
			//point the prim key to the intype
			Set<String> set = new HashSet<>();
			set.add(inType);
			edgeHash.put(metaPrimKey, set);
			
			//merge edgehash
			mergeEdgeHash(edgeHash, dataTypeMap);
		}
	}
	
	@Override
	public void connectTypes(String outType, String inType, Map<String, String> dataTypeMap) {
		Map<String, Set<String>> edgeHash = new HashMap<>();
		Set<String> set = new HashSet<>();
		set.add(inType);
		edgeHash.put(outType, set);
		mergeEdgeHash(edgeHash, dataTypeMap);
	}
	
	/////////////////////// end connect type methods ///////////////////////////////////

	
	@Override
	public void setDerivedColumn(String uniqueName, boolean isDerived) {
		this.metaData.setDerived(uniqueName, isDerived);
	}
	
	@Override
	public void setDerviedCalculation(String uniqueName, String calculationName) {
		this.metaData.setDerivedCalculation(uniqueName, calculationName);
	}
	
	@Override
	public void setDerivedUsing(String uniqueName, String... otherUniqueNames) {
		this.metaData.setDerivedUsing(uniqueName, otherUniqueNames);
	}
	
	@Override
	public IMetaData.DATA_TYPES getDataType(String uniqueName){
		return this.metaData.getDataType(uniqueName);
	}

	@Override
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms) {
		LOGGER.info("We are processing " + transforms.size() + " pre transformations");
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(this);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}
	
	@Override
	public void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame) {
		LOGGER.info("We are processing " + transforms.size() + " post transformations");
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
	public Map<String, Object> getDataMakerOutput(String... selectors) {
		Hashtable retHash = new Hashtable();
		if(selectors.length == 0) {
			List<Object[]> retVector = new Vector<Object[]>();
			String[] headers = new String[1];
			if(this.headerNames != null && this.headerNames.length > 0) {
				retVector = this.getData();
				headers = this.headerNames;
			}
			retHash.put("data", retVector);
			retHash.put("headers", headers);
		} else {
			long startTime = System.currentTimeMillis();
			
			Vector<Object[]> retVector = new Vector<Object[]>();
			Map<String, Object> options = new HashMap<String, Object>();
			options.put(TinkerFrame.SELECTORS, Arrays.asList(selectors));
			options.put(TinkerFrame.DE_DUP, true);
			Iterator<Object[]> iterator = this.iterator(options);
			while(iterator.hasNext()) {
				retVector.add(iterator.next());
			}
			retHash.put("data", retVector);
			retHash.put("headers", selectors);

			LOGGER.info("Collected Raw Data: "+(System.currentTimeMillis() - startTime));
		}
		return retHash;
	}

	@Override
	public List<Object> processActions(DataMakerComponent dmc,
			List<ISEMOSSAction> actions, IDataMaker... dataMaker) {
		LOGGER.info("We are processing " + actions.size() + " actions");
		List<Object> outputs = new ArrayList<Object>();
		for(ISEMOSSAction action : actions){
			action.setDataMakers(this);
			action.setDataMakerComponent(dmc);
			outputs.add(action.runMethod());
		}
		algorithmOutput.addAll(outputs);
		return outputs;
	}
	
	@Override
	public List<Object> getActionOutput() {
		return this.algorithmOutput;
	}

	@Override
	public Map<String, Set<String>> getEdgeHash() {
		return this.metaData.getEdgeHash();
	}
	
	
	@Override
	public String getAliasForUniqueName(String metaNodeName) {
		return this.metaData.getAliasForUniqueName(metaNodeName);
	}
	
	@Override
	public void modifyColumnName(String existingName, String newName) {
		this.metaData.modifyUniqueName(existingName, newName);
		// rename column in header names
		for(int i = 0; i < this.headerNames.length; i++){
			String name = this.headerNames[i];
			if(name.equals(existingName)){
				this.headerNames[i] = newName;
				break;
			}
		}
	}
	
	@Override
	public void addEngineForColumnName(String columnName, String engineName) {
		this.metaData.addEngineForUniqueName(columnName, engineName);
	}
	
	@Override
	public Set<String> getEnginesForUniqueName(String sub){
		return this.metaData.getEnginesForUniqueName(sub);
	}
	
	@Override
	public Map<String, String> getProperties(){
		return this.metaData.getProperties();
	}
	
	public String getPhysicalUriForNode(String string, String engineName){
		return this.metaData.getPhysicalUriForNode(string, engineName);
	}
	
	public List<Map<String, Object>> getTableHeaderObjects(){
		return this.metaData.getTableHeaderObjects();
	}

	@Override
	public void performAnalyticTransformation(IAnalyticTransformationRoutine routine) throws RuntimeException {
		routine.runAlgorithm(this);
	}

	@Override
	public void performAnalyticAction(IAnalyticActionRoutine routine) throws RuntimeException {
		routine.runAlgorithm(this);
	}

	@Override
	public Double getEntropyDensity(String columnHeader) {
		double entropyDensity = 0;
		
		if(isNumeric(columnHeader)) {
			//TODO: need to make barchart class better
			Double[] dataRow = getColumnAsNumeric(columnHeader);
			int numRows = dataRow.length;
			Hashtable<String, Object>[] bins = null;
			BarChart chart = new BarChart(dataRow);
			
			if(chart.isUseCategoricalForNumericInput()) {
				chart.calculateCategoricalBins("NaN", true, true);
				chart.generateJSONHashtableCategorical();
				bins = chart.getRetHashForJSON();
			} else {
				chart.generateJSONHashtableNumerical();
				bins = chart.getRetHashForJSON();
			}
			
			double entropy = 0;
			int i = 0;
			int uniqueValues = bins.length;
			for(; i < uniqueValues; i++) {
				int count = (int) bins[i].get("y");
				if(count != 0) {
					double prob = (double) count / numRows;
					entropy += prob * StatisticsUtilityMethods.logBase2(prob);
				}
			}
			entropyDensity = (double) entropy / uniqueValues;
			
		} else {
			Map<String, Integer> uniqueValuesAndCount = getUniqueValuesAndCount(columnHeader);
			Integer[] counts = uniqueValuesAndCount.values().toArray(new Integer[]{});
			
			// if only one value, then entropy is 0
			if(counts.length == 1) {
				return entropyDensity;
			}
			
			double entropy = 0;
			double sum = StatisticsUtilityMethods.getSum(counts);
			int index;
			for(index = 0; index < counts.length; index++) {
				double val = counts[index];
				if(val != 0) {
					double prob = val / sum;
					entropy += prob * StatisticsUtilityMethods.logBase2(prob);
				}
			}
			entropyDensity = entropy / uniqueValuesAndCount.keySet().size();
		}
		
		return -1.0 * entropyDensity;
	}

	@Override
	public Integer[] getUniqueInstanceCount() {
		List<String> selectors = getSelectors();
		Integer[] counts = new Integer[selectors.size()];
		for(int i = 0; i < counts.length; i++) {
			counts[i] = getUniqueInstanceCount(selectors.get(i));
		}
		return counts;
//		GraphTraversal<Vertex, Map<Object, Object>> gt = g.traversal().V().group().by(Constants.TYPE).by(__.count());
//		Integer [] instanceCount = null;
//		if(gt.hasNext())
//		{
//			Map<Object, Object> output = gt.next();
//			instanceCount = new Integer[headerNames.length];
//			for(int levelIndex = 0;levelIndex < headerNames.length;levelIndex++)
//				instanceCount[levelIndex] = ((Long)output.get(headerNames[levelIndex])).intValue();
//		}
//		return instanceCount;
	}
	
	@Override
	public Iterator<List<Object[]>> uniqueIterator(String columnHeader) {
		return null;
	}

	@Override
	public Iterator<Object[]> scaledIterator() {
		return null;
	}

	@Override
	public boolean[] isNumeric() {
		String[] headers = getColumnHeaders();
		int size = headers.length;
		boolean[] isNumeric = new boolean[size];
		for(int i = 0; i < size; i++) {
			isNumeric[i] = isNumeric(headers[i]);
		}
		return isNumeric;
	}
	
	@Override
	public boolean isNumeric(String uniqueName) {
		DATA_TYPES dataType = this.metaData.getDataType(uniqueName);
		return dataType.equals(IMetaData.DATA_TYPES.NUMBER);
	}

	@Override
	public String[] getColumnHeaders() {
		if(this.headerNames == null || this.headerNames.length == 0) {
			List<String> uniqueNames = this.metaData.getColumnNames();
			headerNames = uniqueNames.toArray(new String[uniqueNames.size()]);
		}
		if(columnsToSkip != null && !columnsToSkip.isEmpty()) {
			List<String> headers = new ArrayList<String>();
			headers.addAll(Arrays.asList(headerNames));
			headers.removeAll(columnsToSkip);
			return headers.toArray(new String[]{});
		}
		return headerNames;
	}
	
	@Override
	public String[] getColumnAliasName() {
		List<String> uniqueNames = this.metaData.getColumnAliasName();
		String[] headerValues = uniqueNames.toArray(new String[uniqueNames.size()]);
		return headerValues;
	}

	@Override
	public Object[] getColumn(String columnHeader) {
		return null;
	}

	@Override
	public Map<String, Integer> getUniqueValuesAndCount(String columnHeader) {
		Map<String, Integer> counts = new Hashtable<String, Integer>();
		/*List<String> columnsToSkip = new Vector<String>();
		for(String header : headerNames) {
			if(!header.equals(columnHeader)) {
				columnsToSkip.add(header);
			}
		}
		
		Iterator<Object[]> it = iterator();
		while(it.hasNext()) {
			Object[] row = it.next();
			if(counts.containsKey(row[0] + "")) {
				int newCount = counts.get(row[0] + "") + 1;
				counts.put(row[0] + "", newCount);
			} else {
				counts.put(row[0] + "", 1);
			}
		}*/
		
		Iterator<Object[]> it = iterator();
		int tgtIndex = Arrays.asList(headerNames).indexOf(columnHeader);
		while(it.hasNext()) {
			Object[] row = it.next();
			String key = row[tgtIndex] + "";
			if(counts.containsKey(key)) {
				int newCount = counts.get(key) + 1;
				counts.put(key, newCount);
			} else {
				counts.put(key, 1);
			}
		}
		
		return counts;
	}

	@Override
	public List<Object[]> getData() {
		long startTime = System.currentTimeMillis();
		
		Vector<Object[]> retVector = new Vector<>();
		Iterator<Object[]> iterator = this.iterator();
		if(iterator != null) {
			while(iterator.hasNext()) {
				retVector.add(iterator.next());
			}
		}
			
		LOGGER.info("Collected All Data in : " +(System.currentTimeMillis() - startTime));
		return retVector;
	}

	@Override
	public List<Object[]> getScaledData(List<String> exceptionColumns) {
		return null;
	}

	@Override
	public boolean isEmpty() {
		Iterator it = this.iterator();
		if(it != null) {
			return !it.hasNext();
		}
		// assume if the iterator cannot be created that the frame is empty
		return true;
	}

	@Override
	public void setColumnsToSkip(List<String> columnHeaders) {
		if(columnHeaders != null)
			this.columnsToSkip = columnHeaders;
	}

	@Override
	public void addRow(ISelectStatement statement) {
		addRow(statement.getPropHash());
	}

	@Override
	public void addRow(Map<String, Object> rowData) {
		for(String key : rowData.keySet()) {
			if(!ArrayUtilityMethods.arrayContainsValue(headerNames, key)) {
				LOGGER.error("Column name " + key + " does not exist in current tree");
			}
		}
		
		Object [] rowArr = new Object[headerNames.length];
		for(int index = 0; index < headerNames.length; index++) {
			if(rowData.containsKey(headerNames[index])) {
				rowArr[index] = getParsedValue(rowData.get(headerNames[index]));
			}
		}
		// not handling empty at this point
		addRow(rowArr);
	}

	protected Object getParsedValue(Object value) {
		Object node = null;

		if(value == null) {
		
		} else if(value instanceof Integer) {
			node = ((Number)value).intValue();
		} else if(value instanceof Number) {
			node = ((Number)value).doubleValue();
		} else if(value instanceof String) {
			node = (String)value;
		} else {
			node = value.toString();
		}
		
		return node;
	}

	@Override
	public Double[] getMax() {
		int size = headerNames.length;
		Double[] max = new Double[size];
		for(int i = 0; i < size; i++) {
			if(isNumeric(headerNames[i])) {
				max[i] = getMax(headerNames[i]);
			}
		}
		
		return max;
	}
	
	@Override
	public Double[] getMin() {
		int size = headerNames.length;
		Double[] min = new Double[size];
		for(int i = 0; i < size; i++) {
			if(isNumeric(headerNames[i])) {
				min[i] = getMin(headerNames[i]);
			}
		}
		
		return min;
	}

	public List<String> getSelectors() {
		List<String> selectors = new ArrayList<String>();
		if(this.headerNames == null || this.headerNames.length == 0) {
			this.headerNames = this.metaData.getColumnNames().toArray(new String[]{});
		}
		if(this.headerNames != null) {
			for(int i = 0; i < headerNames.length; i++) {
				if(!columnsToSkip.contains(headerNames[i])) {
					selectors.add(headerNames[i]);
				}
			}
		} 
		return selectors;
	}	
	
	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<String, String>();
		reactorNames.put(PKQLReactor.VAR.toString(), "prerna.sablecc.VarReactor");
		reactorNames.put(PKQLReactor.INPUT.toString(), "prerna.sablecc.InputReactor");
		reactorNames.put(PKQLReactor.DATA_FRAME_HEADER.toString(), "prerna.sablecc.DataFrameHeaderReactor");
		reactorNames.put(PKQLEnum.COL_RENAME, "prerna.sablecc.ColRenameReactor");
		reactorNames.put(PKQLEnum.TINKER_QUERY_API, "prerna.sablecc.TinkerQueryApiReactor");
		return reactorNames;
	}
	
	@Override
	/**
	 * Set the user id for the user who created this frame instance
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	@Override
	/**
	 * Return the user id for the user who created this frame instance
	 */
	public String getUserId() {
		return this.userId;
	}

	@Override
	/**
	 * Used to update the data id when data has changed within the frame
	 */
	public void updateDataId() {
		this.dataId = this.dataId.add(BigInteger.valueOf(1));
	}
	
	@Override
	/**
	 * Returns the current data id
	 */
	public int getDataId() {
		return this.dataId.intValue();
	}
	
	@Override
	public int getNumRows() {
		Iterator<Object[]> iterator = this.iterator();
		int count = 0;
		while(iterator.hasNext()) {
			count++;
			iterator.next();
		}
		return count;
	}
	
	public void resetDataId() {
		this.dataId = BigInteger.valueOf(0);
	}
	
	// TODO
	public Iterator<IHeadersDataRow> query(String query)
	{
		return null;
	}

	public Iterator<IHeadersDataRow> query(QueryStruct queryStruct) {
		return null;
	}
	
	public String getTableName()
	{
		return null;
	}
}
