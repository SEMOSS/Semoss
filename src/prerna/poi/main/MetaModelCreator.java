package prerna.poi.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.util.ArrayUtilityMethods;

/**
 * This class is used to build a suggested metamodel from a file, currently only works with CSV 
 * The logic was taken from the equivalent front end code that performed the same task
 */
public class MetaModelCreator {

	private CSVFileHelper helper;
	
	// column headers
	private String[] columnHeaders;
	private Map<String, SemossDataType> dataTypeMap;
	private Map<String, String> additionalDataTypeMap;

	private int[] processOrder; //order in which to process columns 
	private CreatorMode mode;

	//start and end row to read, won't be less than 2
	private int endRow = Integer.MAX_VALUE; 
	private int startRow = 2;
	
	//the instance data we will use for prediction
	private List<String[]> data;
	private int limit = 500;

	//AUTO GENERATE META MODEL VARIABLES
	private Map<String, Set<String>> matches;
	private Map<String, Boolean> columnPropMap;

	//PROP FILE VARIABLE
	private Properties propMap;
	private Hashtable<String, String> additionalInfo = new Hashtable<String, String>();

	private Map<String, List<Map<String, Object>>> propFileData = new HashMap<>();
	//*************************************************

	// AUTO = auto generate/predict a metamodel
	// PROP = generate a meta model from a prop file
	
	public enum CreatorMode {AUTO, PROP};

	public MetaModelCreator(CSVFileHelper helper, CreatorMode setting) {
		this.helper = helper;
		
		this.mode = setting;
		this.columnHeaders = helper.getHeaders();

		this.dataTypeMap = new LinkedHashMap<String, SemossDataType>();
		this.additionalDataTypeMap = new LinkedHashMap<String, String>();

		Object[][] dataTypes = helper.predictTypes();
		
		int size = columnHeaders.length;
		for(int colIdx = 0; colIdx < size; colIdx++) {
			Object[] prediction = dataTypes[colIdx];
			dataTypeMap.put(columnHeaders[colIdx], (SemossDataType) prediction[0]);
			if(prediction[1] != null) {
				additionalDataTypeMap.put(columnHeaders[colIdx], (String) prediction[1]);
			}
		}
	}
	
	public MetaModelCreator(CSVFileHelper helper, CreatorMode setting, String propFile) {
		InputStream input = null;
		try {
			input = new FileInputStream(propFile);
			propMap = new Properties();
			propMap.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		this.mode = setting;
		this.columnHeaders = helper.getHeaders();
		int size = columnHeaders.length;

		this.dataTypeMap = new LinkedHashMap<String, SemossDataType>();
		
		// use the prop file to get the data types
		for(int colIdx = 0; colIdx < size; colIdx++) {
			String dataType = propMap.getProperty( (colIdx + 1) + "");
			if(dataType == null) {
				dataType = "STRING";
			}
			dataTypeMap.put(columnHeaders[colIdx], SemossDataType.convertStringToDataType(dataType));
		}
	}

	/**
	 * 
	 */
	public void constructMetaModel() throws Exception{		
		switch (this.mode) {
		case AUTO: {
			genData(); autoGenerateMetaModel(); break;
		}

		case PROP: {
			generateMetaModelFromProp(); break;
		}

		default: break;
		
		}
	}
	
	private void genData() {
		// if the mode is not set
		// it means we are dealing with the new flat table
		this.data = new ArrayList<>(500);

		String [] cells = null;
		int count = 1;
		while((cells = helper.getNextRow()) != null) {
			if(count <= limit) {
				data.add(cells);
			}
			count++;
		}
		this.endRow = count;
	}

	/**
	 * auto generate the meta model
	 */
	private void autoGenerateMetaModel() {
		//need to sort columns by number of unique instances
		//track which columns should be properties
		matches = new HashMap<>(columnHeaders.length);
		populateProcessOrder();
		populateColumnPropMap();

		//run comparisons for strings
		for(int i = 0; i < columnHeaders.length; i++) {
			SemossDataType datatype = this.dataTypeMap.get(columnHeaders[i]);
			if(datatype == SemossDataType.STRING) {
				//run all comparisons
				runAllComparisons(columnHeaders, i);
			}
		}

		//run comparisons for non strings
		for(int i = 0; i < columnHeaders.length; i++) {
			SemossDataType datatype = this.dataTypeMap.get(columnHeaders[i]);
			if(datatype != SemossDataType.STRING) {
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
				SemossDataType datatype = this.dataTypeMap.get(object);
				Map<String, Object> predMap = new HashMap<>();

				String[] subjectArr = {subject};
				String[] objectArr = {object};

				if(datatype == SemossDataType.STRING) {
					String predHolder = subject + "_" + object;
					predMap.put("sub", subjectArr);
					predMap.put("pred", predHolder);
					predMap.put("obj", objectArr);
					this.propFileData.get("propFileRel").add(predMap);
				} else {
					predMap.put("sub", subjectArr);
					predMap.put("prop", objectArr);
					predMap.put("dataType", datatype);
					this.propFileData.get("propFileNodeProp").add(predMap);
				}
			}
		}
	}

	/**
	 * Generates the Meta model data based on the definition of the prop file
	 */
	private void generateMetaModelFromProp() throws Exception {
		if(propMap == null) {
			return;
		}

		List<Map<String, Object>> relList = new ArrayList<>();
		List<Map<String, Object>> nodePropList = new ArrayList<>();
		this.propFileData.put("propFileRel", relList);
		this.propFileData.put("propFileNodeProp", nodePropList);

		// loop through everything in the prop file
		// if it is a special key, we will do some processing
		// otherwise, we just add it as it
		for(Object propKey : propMap.keySet()) {
			String propKeyS = propKey.toString().trim();

			//Parses text written as:
			//RELATION	Title@BelongsTo@Genre;Title@DirectedBy@Director;Title@DirectedAt@Studio;
			if(propKeyS.equals("RELATION")) {
				String relationText = propMap.getProperty(propKeyS).trim();
				if(!relationText.isEmpty()) {

					String[] relations = relationText.split(";");

					for(String relation : relations) {

						String[] components = relation.split("@");
						String subject = components[0].trim();
						String object = components[2].trim();

						// do some header checks
						if(subject.contains("+")) {
							String[] subSplit = subject.split("\\+");
							for(String sub : subSplit) {
								if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, sub)) {
									throw new NoSuchFieldException("CSV does not contain header : "+sub+".  Please update RELATION in .prop file");
								}
							}
						} else {
							if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, subject)) {
								throw new NoSuchFieldException("CSV does not contain header : "+subject+".  Please update RELATION in .prop file");
							}
						}

						if(object.contains("+")) {
							String[] objSplit = object.split("\\+");
							for(String obj : objSplit) {
								if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, obj)) {
									throw new NoSuchFieldException("CSV does not contain header : "+obj+".  Please update RELATION in .prop file");
								}
							}
						} else {
							if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, object)) {
								throw new NoSuchFieldException("CSV does not contain header : "+object+".  Please update RELATION in .prop file");
							}
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
			}

			//Parses text written as :
			//NODE_PROP	Title%RevenueDomestic;Title%RevenueInternational;Title%MovieBudget;Title%RottenTomatoesCritics;Title%RottenTomatoesAudience;Title%Nominated;
			else if(propKeyS.equals("NODE_PROP")) {
				String nodePropText = propMap.getProperty(propKeyS).trim();
				if(!nodePropText.isEmpty()) {
					String[] nodeProps = nodePropText.split(";");

					for(String nodeProp : nodeProps) {

						String[] components = nodeProp.split("%");

						String subject = components[0].trim();
						String object = components[1].trim();

						// do some header checks
						if(subject.contains("+")) {
							String[] subSplit = subject.split("\\+");
							for(String sub : subSplit) {
								if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, sub)) {
									throw new NoSuchFieldException("CSV does not contain header : "+sub+".  Please update RELATION in .prop file");
								}
							}
						} else {
							if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, subject)) {
								throw new NoSuchFieldException("CSV does not contain header : "+subject+".  Please update RELATION in .prop file");
							}
						}

						if(object.contains("+")) {
							String[] objSplit = object.split("\\+");
							for(String obj : objSplit) {
								if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, obj)) {
									throw new NoSuchFieldException("CSV does not contain header : "+obj+".  Please update RELATION in .prop file");
								}
							}
						} else {
							if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, object)) {
								throw new NoSuchFieldException("CSV does not contain header : "+object+".  Please update RELATION in .prop file");
							}
						}

						String[] subjectArr = {subject};
						String[] objectArr = {object};

						Map<String, Object> predMap = new HashMap<>();
						predMap.put("sub", subjectArr);
						predMap.put("prop", objectArr);

						SemossDataType dataType = this.dataTypeMap.get(object);
						predMap.put("dataType", dataType);
						this.propFileData.get("propFileNodeProp").add(predMap);
					}
				}
			} 

			else if(propKeyS.equals("START_ROW")) {
				String startRow = propMap.getProperty(propKeyS);
				try {
					this.startRow = Integer.parseInt(startRow);
				} catch(NumberFormatException e) {
					this.startRow = 2;
				}
			}

			else if(propKeyS.equals("END_ROW")) {
				String endRow = propMap.getProperty(propKeyS);
				try {
					this.endRow = Integer.parseInt(endRow);
				} catch(NumberFormatException e) {

				}
			}
			
			else if(propKeyS.equals("RELATION_PROP")) {
				// do nothing, we don't do anything with relationship props at the moment
			}

			else {
				// WE WANT TO IGNORE SOME THINGS THAT WE DO NOT COUNT AS ADDITIONAL PROPERTIES
				// THIS IS BECAUSE WE WRITE THE DATA TYPES IN THE PROP FILE
				// 1) if the key is NUM_COLUMNS
				if(propKeyS.equals("NUM_COLUMNS")) {
					continue;
				}
				// 2) if the value is STRING, NUMBER, or DATE
				String value = propMap.getProperty(propKeyS);
				if(value.equals("STRING") || value.equals("NUMBER") || value.equals("DATE")) {
					continue;
				}
				additionalInfo.put(propKeyS, propMap.getProperty(propKeyS));
			}
		}
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
						SemossDataType dataType = dataTypeMap.get(secondColumn);
						if(dataType == SemossDataType.STRING) {
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
		Map<Integer, List<String>> sortedMap = new TreeMap<>();
		for(int i = 0; i < columnHeaders.length; i++) {
			if(sortedMap.containsKey(instanceCount[i])) {
				sortedMap.get(instanceCount[i]).add(columnHeaders[i]);
			} else {
				List<String> list = new ArrayList<>(3);
				list.add(columnHeaders[i]);
				sortedMap.put(instanceCount[i], list);
			}
		}

		//store the index of column headers
		int i = columnHeaders.length - 1;
		for(Integer key : sortedMap.keySet()) {
			List<String> nextColumns = sortedMap.get(key);
			for(String column : nextColumns) {
				this.processOrder[i] = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnHeaders, column);
				i--;
			}
		}
	}

	/**
	 * initialize the column property map, each column defaulted to false
	 */
	private void populateColumnPropMap() {
		columnPropMap = new HashMap<>(columnHeaders.length);
		for(String header : columnHeaders) {
			columnPropMap.put(header, false);
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

	public Map<String, String> getAdditionalInfo() {
		return this.additionalInfo;
	}
	
	public Map<String, SemossDataType> getDataTypeMap() {
		return this.dataTypeMap;
	}

	public Map<String, String> getAdditionalDataTypeMap() {
		return this.additionalDataTypeMap;
	}
	
	//////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////
	
	
	//Use this to test metamodel creator
	public static void main(String[] args) {

		String file = "C:\\Users\\rluthar\\Documents\\Movie_Data.csv";
		char delimiter = ',';

		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delimiter);
		helper.parse(file);

		MetaModelCreator predictor = new MetaModelCreator(helper, CreatorMode.AUTO);
		try {
			predictor.constructMetaModel();
		} catch (Exception e) {
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
