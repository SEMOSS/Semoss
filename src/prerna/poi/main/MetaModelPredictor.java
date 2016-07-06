package prerna.poi.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TableDataFrameFactory;
import prerna.ds.H2.H2Frame;
import prerna.util.ArrayUtilityMethods;

/**
 * This class is used to build a suggested metamodel from a file, currently only works with CSV 
 * The logic was taken from the equivalent front end code that performed the same task
 */
public class MetaModelPredictor {

	ITableDataFrame frame;
	int limit = 500;
	Map<String, Boolean> columnPropMap;
	List<Object[]> data;
	Map<String, Set<String>> matches;
	int[] processOrder; //order in which to process columns 
	Map<String, String> dataTypeMap;
	
	//return data
	Map<String, List<String>> allowableDataTypes;
	private Map<String, List<Map<String, String>>> propFileData = new HashMap<>();
	
	
	public MetaModelPredictor() {
		frame = new H2Frame();
	}
	
	public MetaModelPredictor(String fileName, Map<String, Map<String, String>> dataTypeMap, String delimeter) {
		//use the file name
		//TODO : limit to first 500 rows and use CSVHelper to read data\
		//Migrate away from frame since it is unnecessary overhead
		Map<String, String> mainColMap = new HashMap<>();
		mainColMap.put("CSV", null);
		frame = TableDataFrameFactory.generateDataFrameFromFile(fileName, delimeter, "H2", dataTypeMap, mainColMap);
		this.dataTypeMap = dataTypeMap.get("CSV");
	}
	
	/**
	 * 
	 */
	public void predictMetaModel() {
		String[] columnHeaders = frame.getColumnHeaders();
		
		//need to sort columns by number of unique instances
		
		//track which columns should be properties
		matches = new HashMap<>(columnHeaders.length);
		populateData();
		populateProcessOrder();
		populateColumnPropMap();
//		populateAllowableTypes();
		
		//run comparisons for strings
		for(int i = 0; i < columnHeaders.length; i++) {
			
			DATA_TYPES datatype = frame.getDataType(columnHeaders[i]);
			if(datatype.equals(DATA_TYPES.STRING)) {
				//run all comparisons
				runAllComparisons(columnHeaders, i);
			}
		}
		
		
		//run comparisons for non strings
		for(int i = 0; i < columnHeaders.length; i++) {
			DATA_TYPES datatype = frame.getDataType(columnHeaders[i]);
			if(!datatype.equals(DATA_TYPES.STRING)) {
				//run all comparisons
				runAllComparisons(columnHeaders, i);
			}
		}
		
		List<Map<String, String>> initList1 = new ArrayList<>();
		List<Map<String, String>> initList2 = new ArrayList<>();
		this.propFileData.put("propFileRel", initList1);
		this.propFileData.put("propFileNodeProp", initList2);
		
		for(String subject : matches.keySet()) {
			Set<String> set = matches.get(subject);
			for(String object : set) {
				DATA_TYPES dataType = frame.getDataType(object);
				Map<String, String> predMap = new HashMap<>();
				if(!dataType.equals(DATA_TYPES.STRING)) {
					String predHolder = subject + "_" + object;
					predMap.put("sub", subject);
					predMap.put("pred", predHolder);
					predMap.put("obj", object);
					this.propFileData.get("propFileRel").add(predMap);
				} else {
					predMap.put("sub", subject);
					predMap.put("prop", object);
					predMap.put("dataType", dataTypeMap.get(object));
					this.propFileData.get("propFileNodeProp").add(predMap);
				}
			}
		}
	}
	
	public Map<String, List<Map<String, String>>> getMetaModelData() {
//		Map<String, Object> retMap = new HashMap<>(2);
//		retMap.put("propFileData"this.propFileData);
		return propFileData;
	}

	/**
	 * 
	 * @param columnHeaders - the column headers in the csv
	 * @param firstColIndex - the column which we are comparing to other columns
	 */
	private void runAllComparisons(String[] columnHeaders, int firstColIndex) {
		
		for(int i = 0; i < columnHeaders.length; i++) {
			
			//don't compare a column to itself
			if(i == firstColIndex) continue;
			
			String firstColumn = columnHeaders[firstColIndex];
			String secondColumn = columnHeaders[i];
			
			
			//need to make sure second column does not have first column as a a property already
			if(!matches.containsKey(secondColumn) || !matches.get(secondColumn).contains(firstColumn)) {
				if(!columnPropMap.get(secondColumn) && compareCols(firstColIndex, i)) {
					//we have a match
					boolean useInverse = false;
					int firstColIndexInCSV = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, firstColumn);
					int secondColIndexInCSV = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, secondColumn);
					if(firstColIndexInCSV > secondColIndexInCSV) {
						//try to see if inverse order is better
						//but first, check to make sure the second column in not a double or date
						DATA_TYPES datatype = frame.getDataType(secondColumn);
						if(datatype.equals(DATA_TYPES.STRING)) {
							if(!columnPropMap.get(firstColumn) && compareCols(i, firstColIndex)) {
								//use reverse order
								useInverse = true;
								if(matches.containsKey(secondColumn)) {
									matches.get(secondColumn).add(firstColumn);
								} else {
									Set<String> set = new HashSet<String>(1);
									set.add(firstColumn);
									matches.put(secondColumn, set);
								}
								columnPropMap.put(firstColumn, true);
							}
						}
					}
					
					if(!useInverse) {
						if(matches.containsKey(firstColumn)) {
							matches.get(firstColumn).add(secondColumn);
						} else {
							Set<String> set = new HashSet<String>(1);
							set.add(secondColumn);
							matches.put(firstColumn, set);
						}
						columnPropMap.put(secondColumn, true);
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private boolean compareCols(int firstIndex, int secondIndex) {
		Map<Object, Object> values = new HashMap<>();
		for(Object[] row : data) {
			Object firstValue = row[firstIndex];
			Object secondValue = row[secondIndex];
			if(values.containsKey(firstValue)) {
				if(!values.get(firstValue).equals(secondValue)) {
					return false;
				}
			} else {
				values.put(firstValue, secondValue);
			}
		}
		return true;
	}
	
	/**
	 * collect the data that we will use to predict a meta model
	 */
	private void populateData() {
		data = new ArrayList<Object[]>(limit);
		Iterator<Object[]> it = frame.iterator(false);
		int count = 0;
		while(it.hasNext() && count < limit) {
			data.add(it.next());
			count++;
		}
	}
	
	/**
	 * process order is stored based on ascending order of unique instance count
	 */
	private void populateProcessOrder() {
		String[] columnHeaders = frame.getColumnHeaders();
		Integer[] instanceCount = frame.getUniqueInstanceCount();
		processOrder = new int[columnHeaders.length];
		//sort by instance count
		Map<Integer, String> sortedMap = new TreeMap<>();
		for(int i = 0; i < columnHeaders.length; i++) {
			sortedMap.put(instanceCount[i], columnHeaders[i]);
		}
		
		//store the index of column headers
		int i = 0;
		for(Integer key : sortedMap.keySet()) {
			String nextColumn = sortedMap.get(key);
			this.processOrder[i] = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, nextColumn);
			i++;
		}
	}
	
	/**
	 * initialize the column property map, each column defaulted to false
	 */
	private void populateColumnPropMap() {
		String[] columnHeaders = frame.getColumnHeaders();
		columnPropMap = new HashMap<>(columnHeaders.length);
		for(String header : columnHeaders) {
			columnPropMap.put(header, false);
		}
	}
	
	/**
	 * 
	 */
	private void populateAllowableTypes() {
		allowableDataTypes = new HashMap<>();
		String[] columnHeaders = frame.getColumnHeaders();
		for(String header : columnHeaders) {
			List<String> dataTypeList = new ArrayList<>(2);
			dataTypeList.add("STRING");
			
			DATA_TYPES dataType = frame.getDataType(header);
			if(!dataType.equals(DATA_TYPES.STRING)) {
				dataTypeList.add(dataType.toString());
			}
			allowableDataTypes.put(header, dataTypeList);
		}
	}
	//Use this to test metamodel predictor
	public static void main(String[] args) {
		
		String file = "C:\\Users\\rluthar\\Documents\\Movie_Data.csv";
		String delimiter = ",";
		
		Map<String, Map<String, String>> headerMap = new HashMap<>();
		Map<String, String> innerMap = new HashMap<>();
		innerMap.put("Nominated", "STRING");
		innerMap.put("Title", "STRING");
		innerMap.put("Genre", "STRING");
		innerMap.put("Studio", "STRING");
		innerMap.put("Director", "STRING");
		innerMap.put("MovieBudget", "NUMBER");
		innerMap.put("RevenueInternational", "NUMBER");
		innerMap.put("RottenTomatoesCritics", "NUMBER");
		innerMap.put("RottenTomatoesAudience", "NUMBER");
		headerMap.put("CSV", innerMap);
		
		MetaModelPredictor predictor = new MetaModelPredictor(file, headerMap, delimiter);
		predictor.predictMetaModel();
		for(String key : predictor.matches.keySet()) {
			System.out.print(key+"  ");
			System.out.println(predictor.matches.get(key).toString());
		}
		System.out.println(predictor.columnPropMap.toString());
		
		System.out.println("\nMetaModelPredictor Test Complete");
	}
}
