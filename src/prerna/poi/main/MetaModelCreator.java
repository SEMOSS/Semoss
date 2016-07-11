package prerna.poi.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Properties;




import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TableDataFrameFactory;
import prerna.ds.H2.H2Frame;
import prerna.util.ArrayUtilityMethods;

/**
 * This class is used to build a suggested metamodel from a file, currently only works with CSV 
 * The logic was taken from the equivalent front end code that performed the same task
 */
public class MetaModelCreator {

	ITableDataFrame frame;
	int limit = 500;
	Map<String, Boolean> columnPropMap;
	List<Object[]> data;
	Map<String, Set<String>> matches;
	int[] processOrder; //order in which to process columns 
	Map<String, String> dataTypeMap;
	String propFile;
	CreatorMode mode;
	//return data
	Map<String, List<String>> allowableDataTypes;
	private Map<String, List<Map<String, Object>>> propFileData = new HashMap<>();
	
	public enum CreatorMode {AUTO, PROP, TABLE};
	
	public MetaModelCreator() {
		frame = new H2Frame();
	}
	
	public MetaModelCreator(String fileName, Map<String, Map<String, String>> dataTypeMap, String delimeter, CreatorMode setting) {
		//use the file name
		//TODO : limit to first 500 rows and use CSVHelper to read data\
		//Migrate away from frame since it is unnecessary overhead
		if(setting != CreatorMode.PROP) {
			Map<String, String> mainColMap = new HashMap<>();
			mainColMap.put("CSV", null);
			frame = TableDataFrameFactory.generateDataFrameFromFile(fileName, delimeter, "H2", dataTypeMap, mainColMap);
		}
		this.dataTypeMap = dataTypeMap.get("CSV");
		this.mode = setting;
	}
	

	/**
	 * 
	 * @param propFile
	 */
	public void addPropFile(String propFile) {
		this.propFile = propFile;
	}
	
	/**
	 * 
	 */
	public void constructMetaModel() throws Exception{		
		switch (this.mode) {
		case AUTO: {
			autoGenerateMetaModel(); break;
		}
		
		case PROP: {
			generateMetaModelFromProp(); break;
		}
		
		case TABLE: {
			generateTableMetaModel(); break;
		}
			
		default: break;
		}
	}
	
	/**
	 * auto generate the meta model
	 */
	private void autoGenerateMetaModel() {
		String[] columnHeaders = frame.getColumnHeaders();
		
		//need to sort columns by number of unique instances
		
		//track which columns should be properties
		matches = new HashMap<>(columnHeaders.length);
		populateData();
		populateProcessOrder();
		populateColumnPropMap();
		
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
		
		List<Map<String, Object>> initList1 = new ArrayList<>();
		List<Map<String, Object>> initList2 = new ArrayList<>();
		this.propFileData.put("propFileRel", initList1);
		this.propFileData.put("propFileNodeProp", initList2);
		
		for(String subject : matches.keySet()) {
			Set<String> set = matches.get(subject);
			for(String object : set) {
				DATA_TYPES dataType = frame.getDataType(object);
				Map<String, Object> predMap = new HashMap<>();
				
				String[] subjectArr = {subject};
				String[] objectArr = {object};
				
				if(dataType.equals(DATA_TYPES.STRING)) {
					String predHolder = subject + "_" + object;
					predMap.put("sub", subjectArr);
					predMap.put("pred", predHolder);
					predMap.put("obj", objectArr);
					this.propFileData.get("propFileRel").add(predMap);
				} else {
					predMap.put("sub", subjectArr);
					predMap.put("prop", objectArr);
					predMap.put("dataType", dataType.toString());
					this.propFileData.get("propFileNodeProp").add(predMap);
				}
			}
		}
	}
	
	/**
	 * 
	 */
	private void generateMetaModelFromProp() throws Exception{
		//TODO: check if prop file headers and csv headers match
		if(this.propFile != null) {
			try {
				InputStream input = new FileInputStream(propFile);
				Properties prop = new Properties();
				prop.load(input);
				
				List<Map<String, Object>> initList1 = new ArrayList<>();
				List<Map<String, Object>> initList2 = new ArrayList<>();
				this.propFileData.put("propFileRel", initList1);
				this.propFileData.put("propFileNodeProp", initList2);
				
				if(prop.containsKey("RELATION")) {
					
					String relationText = prop.getProperty("RELATION");
					String[] relations = relationText.split(";");
					
					for(String relation : relations) {
						
						String[] components = relation.split("@");
						String[] subjectArr = {components[0].trim()};
						String predicate = components[1];
						String[] objectArr = {components[2].trim()};
						
						Map<String, Object> predMap = new HashMap<>();
						predMap.put("sub", subjectArr);
						predMap.put("pred", predicate);
						predMap.put("obj", objectArr);
						this.propFileData.get("propFileRel").add(predMap);
					}
				}
				
				if(prop.containsKey("NODE_PROP")) {
					String nodePropText = prop.getProperty("NODE_PROP");
					String[] nodeProps = nodePropText.split(";");
					
					for(String nodeProp : nodeProps) {
						
						String[] components = nodeProp.split("%");
						String[] subjectArr = {components[0].trim()};
						String[] objectArr = {components[1].trim()};
						
						Map<String, Object> predMap = new HashMap<>();
						predMap.put("sub", subjectArr);
						predMap.put("prop", objectArr);
						predMap.put("dataType", null);
						this.propFileData.get("propFileNodeProp").add(predMap);
					}
				} 
				
				if(prop.containsKey("DISPLAY_NAME")) {
					String displayNameText = prop.getProperty("DISPLAY_NAME");
					if(displayNameText != null && !displayNameText.isEmpty()) {
						List<Map<String, Object>> displayList = new ArrayList<>();
						this.propFileData.put("itemDisplayName", displayList);
						
						
						String[] displayNames = displayNameText.split(";");
						for(String displayName : displayNames) {
							String[] display = displayName.split(":");
							String[] selectedDisplayNameArr = {display[1]};
							String[] selectedPropertyArr;
							String[] selectedNodeArr;
							if(display[0].contains("%")) {
								String[] prop_node = display[0].split("%");
								selectedNodeArr = new String[]{prop_node[0]};
								selectedPropertyArr = new String[]{prop_node[1]};
							} else {
								selectedPropertyArr = selectedNodeArr = new String[]{display[0]};
							}
							
							Map<String, Object> displayMap = new HashMap<>();
							
							displayMap.put("selectedNode", selectedNodeArr);
							displayMap.put("selectedProperty", selectedPropertyArr);
							displayMap.put("selectedDisplayName", selectedDisplayNameArr);
						}
					}
					//itemDisplayName -> [{selectedNode: [title], selectedProperty: [title], selectedDisplayName : [moovietitles]}, {selectedNode : title, selectedProperty : budget}]
				} 
				
				if(prop.containsKey("RELATION_PROP")) {
					String relationProp = prop.getProperty("RELATION_PROP");
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
		
	}
	
	/**
	 * generate a metamodel in which the column with the most unique instances is the primary key and all other columns are properties of that column
	 */
	private void generateTableMetaModel() {
		String[] columnHeaders = frame.getColumnHeaders();
		populateData();
		populateProcessOrder();
		
		List<Map<String, Object>> initList1 = new ArrayList<>();
		List<Map<String, Object>> initList2 = new ArrayList<>();
		this.propFileData.put("propFileRel", initList1);
		this.propFileData.put("propFileNodeProp", initList2);
		
		boolean findPrimKey = true;
		String subject = null;
		while(findPrimKey) {
			subject = columnHeaders[processOrder[0]];
			if(frame.getDataType(subject).equals(DATA_TYPES.STRING)) {
				findPrimKey = false;
			}
		}
		
		//if all columns are numbers use the first number
		if(subject == null) {
			subject = columnHeaders[processOrder[0]];
		}
		
		for(int i = 1; i < processOrder.length; i++) {
			String object = columnHeaders[processOrder[i]];
			DATA_TYPES dataType = frame.getDataType(object);
			Map<String, Object> predMap = new HashMap<>();
			
			String[] subjectArr = {subject};
			String[] objectArr = {object};
			
			predMap.put("sub", subjectArr);
			predMap.put("prop", objectArr);
			predMap.put("dataType", dataType.toString());
			this.propFileData.get("propFileNodeProp").add(predMap);
		}
	}
	
	public Map<String, List<Map<String, Object>>> getMetaModelData() {
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
		int i = sortedMap.keySet().size() - 1;
		for(Integer key : sortedMap.keySet()) {
			String nextColumn = sortedMap.get(key);
			this.processOrder[i] = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, nextColumn);
			i--;
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
	
//	/**
//	 * 
//	 */
//	private void populateAllowableTypes() {
//		allowableDataTypes = new HashMap<>();
//		String[] columnHeaders = frame.getColumnHeaders();
//		for(String header : columnHeaders) {
//			List<String> dataTypeList = new ArrayList<>(2);
//			dataTypeList.add("STRING");
//			
//			DATA_TYPES dataType = frame.getDataType(header);
//			if(!dataType.equals(DATA_TYPES.STRING)) {
//				dataTypeList.add(dataType.toString());
//			}
//			allowableDataTypes.put(header, dataTypeList);
//		}
//	}
	
	//Use this to test metamodel creator
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
		
		MetaModelCreator predictor = new MetaModelCreator(file, headerMap, delimiter, CreatorMode.AUTO);
		try {
			predictor.constructMetaModel();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(String key : predictor.matches.keySet()) {
			System.out.print(key+"  ");
			System.out.println(predictor.matches.get(key).toString());
		}
		System.out.println(predictor.columnPropMap.toString());
		
		System.out.println("\nMetaModelCreator Test Complete");
	}

}
