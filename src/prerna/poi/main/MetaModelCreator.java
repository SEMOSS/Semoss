package prerna.poi.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Properties;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.util.ArrayUtilityMethods;

/**
 * This class is used to build a suggested metamodel from a file, currently only works with CSV 
 * The logic was taken from the equivalent front end code that performed the same task
 */
public class MetaModelCreator {

//	ITableDataFrame frame;
	String[] columnHeaders;
	int limit = 500;
	Map<String, Boolean> columnPropMap;
	List<String[]> data;
	Map<String, Set<String>> matches;
	int[] processOrder; //order in which to process columns 
	Map<String, String> dataTypeMap;
	String propFile;
	CreatorMode mode;
	int endRow = 2;
	int startRow = 2;
	
	//return data
	Map<String, List<String>> allowableDataTypes;
	private Map<String, List<Map<String, Object>>> propFileData = new HashMap<>();
	
	public enum CreatorMode {AUTO, PROP, TABLE};
	
	public MetaModelCreator() {
	}
	
	public MetaModelCreator(String fileName, Map<String, Map<String, String>> dataTypeMap, String delimiter, CreatorMode setting) {
		//use the file name
		//TODO : limit to first 500 rows and use CSVHelper to read data
		//Migrate away from frame since it is unnecessary overhead
		this.dataTypeMap = dataTypeMap.get("CSV");
		this.mode = setting;
		this.data = new ArrayList<>(500);
		
		
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delimiter.charAt(0));
		helper.parse(fileName);
		
		String[] headers = this.dataTypeMap.keySet().toArray(new String[]{});
		headers = helper.orderHeadersToGet(headers);
		this.columnHeaders = headers;
		
		String [] cells = null;
		int count = 0;
		while((cells = helper.getNextRow()) != null) {
			if(count < limit) {
				data.add(cells);
			}
			count++;
		}
		this.endRow = count;
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
		
		//need to sort columns by number of unique instances
		
		//track which columns should be properties
		matches = new HashMap<>(columnHeaders.length);
//		populateData();
		populateProcessOrder();
		populateColumnPropMap();
		
		//run comparisons for strings
		for(int i = 0; i < columnHeaders.length; i++) {
			
//			DATA_TYPES datatype = frame.getDataType(columnHeaders[i]);
			String datatype = this.dataTypeMap.get(columnHeaders[i]).toUpperCase();
			if(datatype.equals("STRING")) {
				//run all comparisons
				runAllComparisons(columnHeaders, i);
			}
		}
		
		//run comparisons for non strings
		for(int i = 0; i < columnHeaders.length; i++) {
//			DATA_TYPES datatype = frame.getDataType(columnHeaders[i]);
			String datatype = this.dataTypeMap.get(columnHeaders[i]).toUpperCase();
			if(!datatype.equals("STRING")) {
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
//				DATA_TYPES dataType = frame.getDataType(object);
				String dataType = this.dataTypeMap.get(object).toUpperCase();
				Map<String, Object> predMap = new HashMap<>();
				
				String[] subjectArr = {subject};
				String[] objectArr = {object};
				
				if(dataType.equals("STRING")) {
					String predHolder = subject + "_" + object;
					predMap.put("sub", subjectArr);
					predMap.put("pred", predHolder);
					predMap.put("obj", objectArr);
					this.propFileData.get("propFileRel").add(predMap);
				} else {
					predMap.put("sub", subjectArr);
					predMap.put("prop", objectArr);
					predMap.put("dataType", dataType);
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
//			String[] columnHeaders = frame.getColumnHeaders();
			try {
				InputStream input = new FileInputStream(propFile);
				Properties prop = new Properties();
				prop.load(input);
				
				List<Map<String, Object>> initList1 = new ArrayList<>();
				List<Map<String, Object>> initList2 = new ArrayList<>();
				this.propFileData.put("propFileRel", initList1);
				this.propFileData.put("propFileNodeProp", initList2);
				
				//Parses text written as:
				//RELATION	Title@BelongsTo@Genre;Title@DirectedBy@Director;Title@DirectedAt@Studio;
				if(prop.containsKey("RELATION")) {
					
					String relationText = prop.getProperty("RELATION");
					String[] relations = relationText.split(";");
					
					for(String relation : relations) {
						
						String[] components = relation.split("@");
						String subject = components[0].trim();
						String object = components[2].trim();
						if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, subject)) {
							throw new NoSuchFieldException("CSV does not contain header : "+subject+".  Please update RELATION in .prop file");
						}
						
						if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, object)) {
							throw new NoSuchFieldException("CSV does not contain header : "+object+".  Please update RELATION in .prop file");
						}
						
						String[] subjectArr = {subject};
						String predicate = components[1];
						String[] objectArr = {object};
						
						Map<String, Object> predMap = new HashMap<>();
						predMap.put("sub", subjectArr);
						predMap.put("pred", predicate);
						predMap.put("obj", objectArr);
						this.propFileData.get("propFileRel").add(predMap);
					}
				}
				
				//Parses text written as :
				//NODE_PROP	Title%RevenueDomestic;Title%RevenueInternational;Title%MovieBudget;Title%RottenTomatoesCritics;Title%RottenTomatoesAudience;Title%Nominated;
				if(prop.containsKey("NODE_PROP")) {
					String nodePropText = prop.getProperty("NODE_PROP");
					String[] nodeProps = nodePropText.split(";");
					
					for(String nodeProp : nodeProps) {
						
						String[] components = nodeProp.split("%");
						
						String subject = components[0].trim();
						String object = components[1].trim();
						
						if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, subject)) {
							throw new NoSuchFieldException("CSV does not contain header : "+subject+".  Please update NODE_PROP in .prop file");
						}
						
						if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, object)) {
							throw new NoSuchFieldException("CSV does not contain header : "+object+".  Please update NODE_PROP in .prop file");
						}
						
						String[] subjectArr = {subject};
						String[] objectArr = {object};
						
						Map<String, Object> predMap = new HashMap<>();
						predMap.put("sub", subjectArr);
						predMap.put("prop", objectArr);
						
						String dataType = this.dataTypeMap.get(object);
//						if(dataType.equals("DOUBLE")) {
//							dataType = "NUMBER";
//						}
						predMap.put("dataType", dataType);
						this.propFileData.get("propFileNodeProp").add(predMap);
					}
				} 
				
				//Parses text written as : 
				//DISPLAY_NAME	Title:Moovie_Title;Title%RevenueInternational:InternationalRevenueMovie;Title%Nominated:MoovieNominated;Studio:Stoodio;
				if(prop.containsKey("DISPLAY_NAME")) {
					String displayNameText = prop.getProperty("DISPLAY_NAME");
					if(displayNameText != null && !displayNameText.isEmpty()) {
						List<Map<String, Object>> displayList = new ArrayList<>();
						this.propFileData.put("itemDisplayName", displayList);
						
						
						String[] displayNames = displayNameText.split(";");
						for(String displayName : displayNames) {
							String[] display = displayName.split(":");
							String[] selectedDisplayNameArr = {display[1].trim()};
							String[] selectedPropertyArr;
							String[] selectedNodeArr;
							if(display[0].contains("%")) {
								String[] prop_node = display[0].split("%");
								
								String subject = prop_node[0].trim();
								String object = prop_node[1].trim(); 
								
								if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, subject)) {
									throw new NoSuchFieldException("CSV does not contain header : "+subject+".  Please update DISPLAY_NAME in .prop file");
								}
								
								if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, object)) {
									throw new NoSuchFieldException("CSV does not contain header : "+object+".  Please update DISPLAY_NAME in .prop file");
								}
								
								selectedNodeArr = new String[]{subject};
								selectedPropertyArr = new String[]{object};
							} else {
								
								String subject = display[0].trim();
								if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, subject)) {
									throw new NoSuchFieldException("CSV does not contain header : "+subject+".  Please update DISPLAY_NAME in .prop file");
								}
								selectedPropertyArr = selectedNodeArr = new String[]{subject};
							}
							
							Map<String, Object> displayMap = new HashMap<>();
							
							displayMap.put("selectedNode", selectedNodeArr);
							displayMap.put("selectedProperty", selectedPropertyArr);
							displayMap.put("selectedDisplayName", selectedDisplayNameArr);
						}
					}
					//itemDisplayName -> [{selectedNode: [title], selectedProperty: [title], selectedDisplayName : [moovietitles]}, {selectedNode : title, selectedProperty : budget}]
				} 
				
				if(prop.containsKey("START_ROW")) {
					String startRow = prop.getProperty("START_ROW");
					try {
						this.startRow = Integer.parseInt(startRow);
					} catch(NumberFormatException e) {
						this.startRow = 2;
					}
				}
				
				if(prop.containsKey("END_ROW")) {
					String endRow = prop.getProperty("END_ROW");
					try {
						this.endRow = Integer.parseInt(endRow);
					} catch(NumberFormatException e) {
						
					}
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
//		String[] columnHeaders = frame.getColumnHeaders();
//		populateData();
		populateProcessOrder();
		
		List<Map<String, Object>> initList1 = new ArrayList<>();
		List<Map<String, Object>> initList2 = new ArrayList<>();
		this.propFileData.put("propFileRel", initList1);
		this.propFileData.put("propFileNodeProp", initList2);
		
		boolean findPrimKey = true;
		String subject = null;
		while(findPrimKey) {
			subject = columnHeaders[processOrder[0]];
			if(this.dataTypeMap.get(subject).equals("STRING")) {
				findPrimKey = false;
			}
		}
		
		//if all columns are numbers use the first number
		if(subject == null) {
			subject = columnHeaders[processOrder[0]];
		}
		
		for(int i = 1; i < processOrder.length; i++) {
			String object = columnHeaders[processOrder[i]];
//			DATA_TYPES dataType = frame.getDataType(object);
			String dataType = this.dataTypeMap.get(object);
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
	
	public int getEndRow() {
		return this.endRow;
	}
	
	public int getStartRow() {
		return this.startRow;
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
//						DATA_TYPES datatype = frame.getDataType(secondColumn);
						String datatype = dataTypeMap.get(secondColumn).toUpperCase();
						if(datatype.equals("STRING")) {
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
//	private void populateData() {
//		data = new ArrayList<Object[]>(limit);
//		Iterator<Object[]> it = frame.iterator(false);
//		int count = 0;
//		while(it.hasNext() && count < limit) {
//			data.add(it.next());
//			count++;
//		}
//	}
	
	/**
	 * process order is stored based on ascending order of unique instance count
	 */
	private void populateProcessOrder() {
//		String[] columnHeaders = frame.getColumnHeaders();
		Integer[] instanceCount = new Integer[columnHeaders.length];
		
		Map<String, Set<String>> instanceCountMap = new HashMap<>();
		
		for(String header : columnHeaders) {
			instanceCountMap.put(header, new HashSet<>());
		}
		
		for(String[] row : data) {
			for(int i = 0; i < row.length; i++) {
				instanceCountMap.get(columnHeaders[i]).add(row[i]);
			}
		}
		
		for(int i = 0; i < columnHeaders.length; i++) {
			instanceCount[i] = instanceCountMap.get(columnHeaders[i]).size();
		}
		
		
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
//		String[] columnHeaders = frame.getColumnHeaders();
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
