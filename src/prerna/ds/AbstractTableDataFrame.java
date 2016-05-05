package prerna.ds;

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
import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.ArrayUtilityMethods;

public abstract class AbstractTableDataFrame implements ITableDataFrame {
	
	private static final Logger LOGGER = LogManager.getLogger(AbstractTableDataFrame.class.getName());

	protected IMetaData metaData;
	protected String[] headerNames;
	protected List<Object> algorithmOutput = new Vector<Object>();
	protected List<String> columnsToSkip = new Vector<String>(); //make a set?
	
	@Override
	public Map<String, Set<String>> createPrimKeyEdgeHash(String[] headers) {
		return TinkerMetaHelper.createPrimKeyEdgeHash(headers);
	}

	@Override
	public void mergeEdgeHash(Map<String, Set<String>> primKeyEdgeHash, Map<String, String> dataTypeMap) {
		// combine the new values into the tinker meta data
		TinkerMetaHelper.mergeEdgeHash(this.metaData, primKeyEdgeHash);

		// also store the data types associated with the new headers that are created
		if(dataTypeMap != null) {
			for(String key : dataTypeMap.keySet()) {
				String type = dataTypeMap.get(key);
				if(type == null) type = "STRING";
				this.metaData.storeDataType(key, type);
			}
		}
	
		// update the list of header names inside the data frame
    	List<String> fullNames = this.metaData.getColumnNames();
    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
	}
	

	public void mergeEdgeHash(IMetaData metaData2, Map<String, Set<String>> primKeyEdgeHash, Map<String, String> node2ValueHash, Map<String, String> dataTypeMap) {
		// combine the new values into the tinker meta data, BUT ALSO use the values defined
		// this is extremely important for RDBMS
		TinkerMetaHelper.mergeEdgeHash(this.metaData, primKeyEdgeHash, node2ValueHash);

		// also store the data types associated with the new headers that are created
		if(dataTypeMap != null) {
			for(String key : dataTypeMap.keySet()) {
				String type = dataTypeMap.get(key);
				if(type == null) type = "STRING";
				this.metaData.storeDataType(key, type);
			}
		}
	
		// update the list of header names inside the data frame
    	List<String> fullNames = this.metaData.getColumnNames();
    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
	}

	@Override
	public void addMetaDataTypes(String[] headers, String[] types) {
		this.metaData.storeDataTypes(headers, types);
	}

	@Override
	public Map[] mergeQSEdgeHash(Map<String, Set<String>> edgeHash, IEngine engine, Vector<Map<String, String>> joinCols) {
		Map[] ret =  TinkerMetaHelper.mergeQSEdgeHash(this.metaData, edgeHash, engine, joinCols);

    	List<String> fullNames = this.metaData.getColumnNames();
    	this.headerNames = fullNames.toArray(new String[fullNames.size()]);
    	
    	return ret;
	}

	/**
	 * 
	 * @param outTypes
	 * @param inType
	 * 
	 * use this method to say connect all my outTypes to an inType, use for multiColumnJoin
	 */
	@Override
	public void connectTypes(String[] outTypes, String inType, Map<String, String> dataTypeMap) {
		if(outTypes.length == 1) {
			connectTypes(outTypes[0], inType, dataTypeMap);
		} else {
			Map<String, Set<String>> edgeHash = new HashMap<>();
			
			//point each outType to the prim key
			String metaPrimKey = getMetaPrimaryKeyName(outTypes);
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
	
	/**
	 * 
	 * @param nodes
	 * @return
	 */
	public String getMetaPrimaryKeyName(String... nodes) {
		Arrays.sort(nodes);
		String primKey = "";
		for(String node : nodes) {
			primKey += node + TinkerFrame.primKeyDelimeter;
		}
		return primKey;
	}

	@Override
	public void connectTypes(String outType, String inType, Map<String, String> dataTypeMap) {
		Map<String, Set<String>> edgeHash = new HashMap<>();
		Set<String> set = new HashSet<>();
		set.add(inType);
		edgeHash.put(outType, set);
		mergeEdgeHash(edgeHash, dataTypeMap);
	}
	
	@Override
	public void setDerivedColumn(String uniqueName, boolean isDerived) {
		this.metaData.setDerived(uniqueName, isDerived);
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
			retHash.put("data", this.getRawData());
			retHash.put("headers", this.headerNames);
		} else {
			long startTime = System.currentTimeMillis();
			
			Vector<Object[]> retVector = new Vector<Object[]>();
			Map<String, Object> options = new HashMap<String, Object>();
			options.put(TinkerFrame.SELECTORS, Arrays.asList(selectors));
			options.put(TinkerFrame.DE_DUP, true);
			Iterator<Object[]> iterator = this.iterator(true, options);
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
	public void undoJoin() {
		// TODO Auto-generated method stub
		//Do we need this?
	}

	@Override
	public void append(ITableDataFrame table) {
		// TODO Auto-generated method stub
	}

	@Override
	public void undoAppend() {
		// TODO Auto-generated method stub
		//Do we need this?
	}

	@Override
	public void performAnalyticTransformation(IAnalyticTransformationRoutine routine) throws RuntimeException {
		ITableDataFrame newTable = routine.runAlgorithm(this);
		if(newTable != null) {
			this.join(newTable, newTable.getColumnHeaders()[0], newTable.getColumnHeaders()[0], 1, new ExactStringMatcher());
		}
	}

	@Override
	public List<String> getMostSimilarColumns(ITableDataFrame table, double confidenceThreshold, IAnalyticRoutine routine) {
		return null;
	}
	
	@Override
	public void performAnalyticAction(IAnalyticActionRoutine routine) throws RuntimeException {
		routine.runAlgorithm(this);
	}

	@Override
	public void undoAction() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Double getEntropy(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getEntropy() {
		// TODO Auto-generated method stub
		return null;
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
	public Double[] getEntropyDensity() {
		// TODO Auto-generated method stub
		return null;
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
	public Double getStandardDeviation(String columnHeader) {
		return null;
	}

	@Override
	public Double[] getStandardDeviation() {
		return null;
	}

	@Override
	public void binNumericColumn(String column) {
		
	}

	@Override
	public void binNumericalColumns(String[] columns) {
		
	}

	@Override
	public void binAllNumericColumns() {
		
	}

	@Override
	public Iterator<List<Object[]>> uniqueIterator(String columnHeader,	boolean getRawData) {
		return null;
	}

	@Override
	public Iterator<Object[]> standardizedIterator(boolean getRawData) {
		return null;
	}

	@Override
	public Iterator<Object[]> scaledIterator(boolean getRawData) {
		return null;
	}

	@Override
	public Iterator<List<Object[]>> standardizedUniqueIterator(String columnHeader, boolean getRawData) {
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
	public int getNumCols() {
		return this.getColumnHeaders().length;
	}

	@Override
	public int getColCount(int rowIdx) {
		return 0;
	}
	
	@Override
	public Object[] getColumn(String columnHeader) {
		return null;
	}

	@Override
	public Object[] getRawColumn(String columnHeader) {
		return null;
	}

	@Override
	public Map<String, Integer> getUniqueValuesAndCount(String columnHeader) {
		Map<String, Integer> counts = new Hashtable<String, Integer>();
		List<String> columnsToSkip = new Vector<String>();
		for(String header : headerNames) {
			if(!header.equals(columnHeader)) {
				columnsToSkip.add(header);
			}
		}
		
		Iterator<Object[]> it = iterator(false);
		while(it.hasNext()) {
			Object[] row = it.next();
			if(counts.containsKey(row[0] + "")) {
				int newCount = counts.get(row[0] + "") + 1;
				counts.put(row[0] + "", newCount);
			} else {
				counts.put(row[0] + "", 1);
			}
		}
		
		return counts;
	}

	@Override
	public Map<String, Map<String, Integer>> getUniqueColumnValuesAndCount() {
		Map<String, Map<String, Integer>> counts = new Hashtable<String, Map<String, Integer>>();
		for(String header : headerNames) {
			counts.put(header, getUniqueValuesAndCount(header));
		}
		return counts;
	}

	@Override
	public void refresh() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeDuplicateRows() {
		//don't think this is needed
	}

	@Override
	public void removeRow(int rowIdx) {
		//unsure
	}

	@Override
	public void removeValue(String value, String rawValue, String level) {
		//do able
		//find the value, delete that node and all nodes connecting to that which have no other connections, repeat for deleted nodes
	}

	@Override
	public ITableDataFrame[] splitTableByColumn(String columnHeader) {
		return null;
		//should be 'relatively' simple to do
	}

	@Override
	public ITableDataFrame[] splitTableByRow(int rowIdx) {
		return null;
	}

	@Override
	public List<Object[]> getData() {
		
		Vector<Object[]> retVector = new Vector<>();
		Iterator<Object[]> iterator = this.iterator(false);
		while(iterator.hasNext()) {
			retVector.add(iterator.next());
		}
		return retVector;
	}

	@Override
	public List<Object[]> getAllData() {
		return null;
		//needed?
	}

	@Override
	public List<Object[]> getScaledData() {
		return null;
	}

	@Override
	public List<Object[]> getScaledData(List<String> exceptionColumns) {
		return null;
	}

	@Override
	public List<Object[]> getRawData() {
		
		long startTime = System.currentTimeMillis();
		
		Vector<Object[]> retVector = new Vector<>();
		Iterator<Object[]> iterator = this.iterator(true);
//		int count = 0;
		while(iterator.hasNext()) {
			retVector.add(iterator.next());
//			System.out.println("added row " + count++);
		}
		
		LOGGER.info("Collected Raw Data: "+(System.currentTimeMillis() - startTime));
		return retVector;
	}
	
	@Override
	public boolean isEmpty() {
		return !this.iterator(false).hasNext();
	}

	@Override
	public void setColumnsToSkip(List<String> columnHeaders) {
		if(columnHeaders != null)
			this.columnsToSkip = columnHeaders;
	}

	@Override
	public Object[] getFilteredUniqueRawValues(String columnHeader) {
		return null;
	}
	
	@Override
	public void addRow(ISelectStatement statement) {
		addRow(statement.getPropHash(), statement.getRPropHash());
	}

	@Override
	public void addRow(Map<String, Object> rowCleanData, Map<String, Object> rowRawData) {

		for(String key : rowCleanData.keySet()) {
			if(!ArrayUtilityMethods.arrayContainsValue(headerNames, key)) {
				LOGGER.error("Column name " + key + " does not exist in current tree");
			}
		}
		
		Object [] rowRawArr = new Object[headerNames.length];
		Object [] rowCleanArr = new Object[headerNames.length];
		for(int index = 0; index < headerNames.length; index++) {
			if(rowRawData.containsKey(headerNames[index])) {
				rowRawArr[index] = rowRawData.get(headerNames[index]).toString();
				rowCleanArr[index] = getParsedValue(rowCleanData.get(headerNames[index]));
			}
		}
		// not handling empty at this point
		addRow(rowCleanArr, rowRawArr);
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

	@Override
	public Object[] getUniqueRawValues(String columnHeader) {
		Iterator<Object> uniqIterator = this.uniqueValueIterator(columnHeader, true, false);
		Vector <Object> uniV = new Vector<Object>();
		while(uniqIterator.hasNext())
			uniV.add(uniqIterator.next());

		return uniV.toArray();
	}

	public List<String> getSelectors() {
		List<String> selectors = new ArrayList<String>();
		for(int i = 0; i < headerNames.length; i++) {
			if(!columnsToSkip.contains(headerNames[i])) {
				selectors.add(headerNames[i]);
			}
		}
		return selectors;
	}
}
