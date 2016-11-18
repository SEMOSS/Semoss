//package prerna.ds;
//
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//import org.apache.poi.ss.usermodel.DateUtil;
//import org.apache.poi.xssf.usermodel.XSSFCell;
//import org.apache.poi.xssf.usermodel.XSSFRow;
//import org.apache.poi.xssf.usermodel.XSSFSheet;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.test.TestUtilityMethods;
//import prerna.util.ArrayUtilityMethods;
//import prerna.util.DIHelper;
//
//public class MutualInformation {
//
//	private static final String delimiter = ":::";
//	private static final int logBase = 2;
//	private static String folderPath;
//	
//	public static void main(String[] args) {
//
//		TestUtilityMethods.loadDIHelper();
//		folderPath = DIHelper.getInstance().getProperty("BaseFolder");
//		folderPath = folderPath + "\\test\\";
//		FileInputStream file = null;
//		XSSFWorkbook workbook = null;
//		try {
//			file = new FileInputStream(new File(folderPath + "MovieDB.xlsx"));
//			workbook = new XSSFWorkbook(file);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		BTreeDataFrame table = (BTreeDataFrame) createBTreeFromExcel(workbook, "MovieData");
//		
//		
//		//Indicate Columns of Interest Here - MUST be exact
//		//******************************
//		
//		//Put two columns here that you need to compare
//		String column1 = "Nominated";
//		String column2 = "Genre";
//		
//		//Put n conditional columns here
//		String conditionalColumn1 = "Studio";
//		String conditionalColumn2 = "Director";
//		//******************************
//		
//		/*
//		 * Arguments are 
//		 * 	1. table - do not change
//		 * 	2. first column of two to compare
//		 * 	3. second column of two to compare
//		 * 	4. add any number of conditional columns after second column below
//		 * 
//		 * Ex: 
//		 * 		independenceValue = getIndependenceValue(table, column1, column2, conditionalColumn1);
//		 * 		independenceValue = getIndependenceValue(table, column1, column2, conditionalColumn1, conditionalColumn2);
//		 * 		independenceValue = getIndependenceValue(table, column1, column2, conditionalColumn1, conditionalColumn2, conditionalColumn3);
//		 * 		etc.
//		 * */
//		double independenceValue = getIndependenceValue(table, column1, column2, conditionalColumn1);
//		System.out.println("Calculated Value: "+independenceValue);
//	}
//	
//	public static double getIndependenceValue(BTreeDataFrame table, String column1, String column2) {
//		
//		double independenceValue = 0.0;
//		Map<String, Integer> column1Count = getUniqueValuesAndCount(table, column1);
//		Map<String, Integer> column2Count = getUniqueValuesAndCount(table, column2);
//		String[] columnHeaders = table.getColumnHeaders();
//		
//		int index1 = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, column1);
//		int index2 = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, column2);
//		double numRows = (double)table.getNumRows();
//		
//		TreeNode root = table.getBuilder().nodeIndexHash.get(column1);
//		UniqueBTreeIterator iterator = new UniqueBTreeIterator(root, false);
//		
//		while(iterator.hasNext()) {
//			List<Object[]> nextSet = iterator.next();
//			Map<String, Integer> countMap = new HashMap<String, Integer>();
//			for(int i = 0; i < nextSet.size(); i++) {
//				Object[] nextRow = nextSet.get(i);
//				String key = nextRow[index1].toString()+delimiter+nextRow[index2].toString();
//				if(countMap.containsKey(key)) {
//					int count = countMap.get(key);
//					countMap.put(key, ++count);
//				} else {
//					countMap.put(key, 1);
//				}
//			}
//			
//			double probabilityAB;
//			double probabilityA;
//			double probabilityB;
//			
//			for(String key : countMap.keySet()) {
//				probabilityAB = (double)countMap.get(key)/numRows;
//				String[] values = key.split(delimiter);
//				String A = values[0];
//				String B = values[1];
//				probabilityA = (double)column1Count.get(A)/numRows;
//				probabilityB = (double)column2Count.get(B)/numRows;
//				
//				double currentValue = calculateMutualValue(probabilityAB, probabilityA, probabilityB, logBase); 
//				independenceValue += currentValue;
//				
//				System.out.println(column1+": Probability of "+A+": "+probabilityA);
//				System.out.println(column2+": Probability of "+B+": "+probabilityB);
//				System.out.println("Probability of ("+A+", "+B+"):"+probabilityAB);
//				System.out.println("Current Calculation: "+ currentValue);
//				System.out.println("current Sum: "+independenceValue);
//				System.out.println("\n");
//			}
//		}
//		
//		System.out.println("Calculation complete between column "+column1+" and "+column2);
//		return independenceValue;
//	}
//
//	public static double getIndependenceValue(BTreeDataFrame table, String column1, String column2, String... conditionalColumns) {
//		
//		double independenceValue = 0.0;
//		Map<String, Integer> CountAC = getUniqueValuesAndCount(table, column1, conditionalColumns);
//		Map<String, Integer> CountBC = getUniqueValuesAndCount(table, column2, conditionalColumns);
//		Map<String, Integer> CountC = getUniqueValuesAndCount(table, conditionalColumns);
//		Map<String, Integer> CountABC = getUniqueValuesAndCount(table, column1, column2, conditionalColumns);
//		
//		int totalRows = table.getNumRows();
//		
//		
//		for(String ABC : CountABC.keySet()) {
//			String[] keys = ABC.split(delimiter);
//			String A = delimiter+keys[1];
//			String B = delimiter+keys[2];
//			String C = "";
//			for(int i = 3; i < keys.length; i++) {
//				C = C+delimiter+keys[i];
//			}
//	
//			double probabilityABC  = (double)CountABC.get(ABC)/(double)totalRows;
//			double probabilityAC  = (double)CountAC.get(A+C)/(double)totalRows;
//			double probabilityBC  = (double)CountBC.get(B+C)/(double)totalRows;
//			double probabilityC  = (double)CountC.get(C)/(double)totalRows;
//
//			double currentCalculation = calculateConditionalMutualValue(probabilityC, probabilityABC, probabilityAC, probabilityBC);
//			independenceValue += currentCalculation;
//			
//			String[] stringC = C.split(delimiter);
//			String printC = Arrays.toString(stringC);
//			System.out.println(Arrays.toString(conditionalColumns)+": Probability of "+printC+": "+probabilityC);
//			System.out.println(column1+": Probability of "+keys[1]+" given "+printC+": "+probabilityAC);
//			System.out.println(column2+": Probability of "+keys[2]+" given "+printC+": "+probabilityBC);
//			System.out.println("Probability of ("+Arrays.toString(keys)+"):"+probabilityABC);
//			System.out.println("Current Calculation: "+ currentCalculation);
//			System.out.println("current Sum: "+independenceValue);
//			System.out.println("\n");
//		}
//		
//		System.out.println("Calculation complete between column "+column1+" and "+column2+" with conditional columns "+Arrays.toString(conditionalColumns));
//		return independenceValue;
//	}
//	
//	private static double calculateMutualValue(double probabilityAB, double probabilityA, double probabilityB, double logBase) {
//		double lnLog = Math.log(probabilityAB/(probabilityA * probabilityB));
//		double lnLog2 = Math.log(2);
//		double logValue = lnLog/lnLog2;
//		
//		return probabilityAB * logValue;
//	}
//	
//	//use this to calculate conditional probabilities
//	private static double calculateConditionalMutualValue(double probabilityC, double probabilityABC, double probabilityAC, double probabilityBC) {
//		double product = (probabilityC*probabilityABC)/(probabilityAC*probabilityBC);
//		double lnLog = Math.log(product);
//		double lnLog2 = Math.log(2);
//		double logValue = lnLog/lnLog2;
//		
//		return probabilityABC * logValue;
//	}
//	
//	private static Map<String, Integer> getUniqueValuesAndCount(BTreeDataFrame table, String columnHeader) {
//		Object[] countColumn = table.getColumn(columnHeader); //get all the objects within the column
//		Map<String, Integer> returnHash = new HashMap<>(); //initiate new HashMap
//		
//		int count;
//	
//		for (int i = 0; i < countColumn.length; i++) {    //loop until column ends
//			String currentKey = countColumn[i].toString();
//			if (returnHash.containsKey(currentKey)) {    //if the specific value in the object is already in a HashMap:
//				count = returnHash.get(currentKey)+1;
//				returnHash.put(currentKey, count);    //add to HashMap
//			} else {    //if specific value isn't already in a HashMap:
//				returnHash.put(currentKey, 1);    //add to HashMap
//			}
//		}
//
//		return returnHash;
//	}
//
//	
//	private static Map<String, Integer> getUniqueValuesAndCount(BTreeDataFrame table, String column1, String column2, String...columnHeader) {
//		String[] tableHeaders = table.getColumnHeaders();
//		
//		//get the index values for the columns we need counts for
//		int[] indexArray = new int[columnHeader.length+2];
//		int count = 0;
//		for(int i = 2; i < indexArray.length; i++) {
//			indexArray[i] = ArrayUtilityMethods.arrayContainsValueAtIndex(tableHeaders, columnHeader[count]);
//			count++;
//		}
//		Arrays.sort(indexArray);
//		
//		indexArray[0] = ArrayUtilityMethods.arrayContainsValueAtIndex(tableHeaders, column1);
//		indexArray[1] = ArrayUtilityMethods.arrayContainsValueAtIndex(tableHeaders, column2);
//		
//		return getUniqueValuesAndCount(table, indexArray);
//	}
//	
//	private static Map<String, Integer> getUniqueValuesAndCount(BTreeDataFrame table, String column1, String...columnHeader) {
//		String[] tableHeaders = table.getColumnHeaders();
//		
//		//get the index values for the columns we need counts for
//		int[] indexArray = new int[columnHeader.length+1];
//		int count = 0;
//		for(int i = 1; i < indexArray.length; i++) {
//			indexArray[i] = ArrayUtilityMethods.arrayContainsValueAtIndex(tableHeaders, columnHeader[count]);
//			count++;
//		}
//		Arrays.sort(indexArray);
//		
//		indexArray[0] = ArrayUtilityMethods.arrayContainsValueAtIndex(tableHeaders, column1);
//		return getUniqueValuesAndCount(table, indexArray);
//	}
//	
//	private static Map<String, Integer> getUniqueValuesAndCount(BTreeDataFrame table, String...columnHeader) {
//		String[] tableHeaders = table.getColumnHeaders();
//		
//		//get the index values for the columns we need counts for
//		int[] indexArray = new int[columnHeader.length];
//		int count = 0;
//		for(int i = 0; i < indexArray.length; i++) {
//			indexArray[i] = ArrayUtilityMethods.arrayContainsValueAtIndex(tableHeaders, columnHeader[count]);			
//			count++;
//		}
//		Arrays.sort(indexArray);
//		return getUniqueValuesAndCount(table, indexArray);
//	}
//	
//	//Get the counts for conditional probability
//	private static Map<String, Integer> getUniqueValuesAndCount(BTreeDataFrame table, int[] indexArray) {
//		Map<String, Integer> returnHash = new HashMap<>();
//		List<Object[]> data = table.getData();
//		
//		int count;
//		for(int i = 0; i < data.size(); i++) {
//			Object[] nextRow = data.get(i);
//			String currentKey = "";
//			
//			for(int x = 0; x < indexArray.length; x++) {
//				int index = indexArray[x];
//				currentKey = currentKey+delimiter+nextRow[index].toString();
//			}
//			
//			if (returnHash.containsKey(currentKey)) {    //if the specific value in the object is already in a HashMap:
//				count = returnHash.get(currentKey)+1;
//				returnHash.put(currentKey, count);    //add to HashMap
//			} else {    //if specific value isn't already in a HashMap:
//				returnHash.put(currentKey, 1);    //add to HashMap
//			}
//		}
//		
//		return returnHash;
//	}
//	
//	// make the BTree
//	private static ITableDataFrame createBTreeDataFrame(List<Object[]> values, String[] columnHeader) {
//		BTreeDataFrame table1 = new BTreeDataFrame(columnHeader);
//		for (Object[] value : values) {
//			table1.addRow(value);
//		}
//		return table1;
//	}
//
//	private static ITableDataFrame createBTreeFromExcel(XSSFWorkbook workbook, String worksheet) {
//		XSSFSheet sheet = workbook.getSheet(worksheet);
//		int numRows = sheet.getLastRowNum();
//		XSSFRow r = sheet.getRow(0);
//		int numCols = r.getLastCellNum();
//		String[] headers = new String[numCols];
//		// loop through all columns in first row
//		for (int i = 0; i < numCols; i++) {
//			XSSFCell cell = r.getCell(i);
//			headers[i] = cell.toString();
//		}
//
//		// loop through all other rows
//		List<Object[]> listValues = new ArrayList<Object[]>();
//		for (int i = 1; i <= numRows; i++) {
//			r = sheet.getRow(i);
//			Object[] values = new Object[numCols];
//			// loop through all columns in specific row
//			for (int j = 0; j < numCols; j++) {
//				XSSFCell cell = r.getCell(j);
//				// retrieving row in specific object type s.t. numbers/dates are
//				// added as double/date object
//				if (cell != null) {
//					int cellType = cell.getCellType();
//					if (cellType == XSSFCell.CELL_TYPE_BLANK) {
//						values[j] = "";
//					} else if (cellType == XSSFCell.CELL_TYPE_NUMERIC) {
//						if (DateUtil.isCellDateFormatted(cell)) {
//							values[j] = cell.getDateCellValue();
//						} else {
//							values[j] = cell.getNumericCellValue();
//						}
//					} else {
//						values[j] = cell.getStringCellValue();
//					}
//				}
//			}
//			listValues.add(values);
//		}
//		return createBTreeDataFrame(listValues, headers);
//	}
//}
