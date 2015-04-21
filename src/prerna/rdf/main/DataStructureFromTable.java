package prerna.rdf.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.algorithm.impl.AlgorithmDataFormatter;
import prerna.math.CalculateEntropy;
import prerna.util.Utility;

public class DataStructureFromTable {

	private static ArrayList<Object[]> table = new ArrayList<Object[]>();
	private static String[] headers;
	private static String[] columnTypes;

	public static void main(String[] args) {

		///////////////////////////////////////////////////
		//TODO: change to correct file location when testing
		String loc = "C:\\Users\\mahkhalil\\Desktop\\ConstructingDataStructure.xlsx";
//		String loc = "C:\\Users\\mahkhalil\\Desktop\\FY16 BTA List.xlsx";
		///////////////////////////////////////////////////

		readExcelFile(loc);
		//		GenerateEntropyDensity eDensity = new GenerateEntropyDensity(table,true);
		//		double[] entropyArr = eDensity.generateEntropy();
		//		String[] columnTypes = eDensity.getColumnTypes();

		long startT = System.currentTimeMillis();
		
		int[] uniqueCounts = getUniqueCounts(table);

//		System.out.println(Arrays.toString(headers));
//		System.out.println(Arrays.toString(uniqueCounts));

		int i = 0;
		int numCol = headers.length;
		int[] indexFromMaxToMin = new int[numCol];
		for(; i < numCol; i++) {
			indexFromMaxToMin[i] = i;
		}
		boolean change = true;
		while(change) {
			change = false;
			i = 0;
			for(; i < numCol-1; i++) {
				if(uniqueCounts[i] > uniqueCounts[i+1]) {
					change = true;
					int valTemp = uniqueCounts[i];
					uniqueCounts[i] = uniqueCounts[i+1];
					uniqueCounts[i+1] = valTemp;

					int indexTemp = indexFromMaxToMin[i];
					indexFromMaxToMin[i] = indexFromMaxToMin[i+1];
					indexFromMaxToMin[i+1] = indexTemp;
				}
			}
		}

//		System.out.println(Arrays.toString(uniqueCounts));
//		for(i = 0; i < numCol; i++) {
//			System.out.print(headers[indexFromMaxToMin[i]] + ", ");
//		}
//		System.out.println("");
		
		
		boolean[] colAlreadyProperty = new boolean[numCol];
		Hashtable<String, ArrayList<String>> matches = new Hashtable<String, ArrayList<String>>();

		i = 0;
		for(; i < numCol; i++) {
			//ignore numerical values
			if(!columnTypes[indexFromMaxToMin[i]].equals("STRING") && !columnTypes[indexFromMaxToMin[i]].equals("INTEGER")) {
				continue;
			}
			int j = 0;
			for(; j < numCol; j++) {
				if(i != j) {
					int firstCol = indexFromMaxToMin[i];
					int secondCol = indexFromMaxToMin[j];
					String firstColName = headers[firstCol];
					String secondColName = headers[secondCol];
					System.out.println("Comparing " + firstColName + " with " + secondColName);
					
					if(compareCols(firstCol,secondCol) && !colAlreadyProperty[secondCol]) {
						// need to make sure secondCol does not have firstCol as property already
						if(matches.containsKey(secondColName) && matches.get(secondColName).contains(firstColName)) {
							// false alarm
							continue;
						}
						
						System.out.println("MATCH!!!");
						boolean useInverse = false;
						if(i > j) {
							System.out.println("TRY TESTING REVERSE ORDER TO GO IN ORDER OF TABLE INPUT...");
							if(compareCols(secondCol, firstCol) && !colAlreadyProperty[firstCol]) {
								System.out.println("USE REVERSE ORDER!!!");
								useInverse = true;
								// if inverse also works, take the value from the most left as the concept
								if(matches.containsKey(secondColName)) {
									matches.get(secondColName).add(firstColName);
								} else {
									ArrayList<String> list = new ArrayList<String>();
									list.add(firstColName);
									matches.put(secondColName, list);
								}
								colAlreadyProperty[indexFromMaxToMin[i]] = true;
							}
						}
						if(!useInverse) {
							if(matches.containsKey(firstColName)) {
								matches.get(firstColName).add(secondColName);
							} else {
								ArrayList<String> list = new ArrayList<String>();
								list.add(secondColName);
								matches.put(firstColName, list);
							}
							colAlreadyProperty[indexFromMaxToMin[j]] = true;
						}
					}
				}
			}
		}
		
		i = 0;
		for(; i < numCol; i++) {
			if(!matches.containsKey(headers[i]) && (columnTypes[i].equals("STRING") || columnTypes[i].equals("INTEGER")) ) {
				matches.put(headers[i], new ArrayList<String>());
			}
		}

		System.out.println("\nResults!!!");
		for(String s : matches.keySet()) {
			System.out.println(s + ":" + matches.get(s));
		}
		
		long endT = System.currentTimeMillis();
		System.out.println("\nTime(s) to run algorithm (excluding reading the excel) = " + (endT - startT)/1000);
	}

	private static int[] getUniqueCounts(ArrayList<Object[]> data) {
		Object[][] newData = AlgorithmDataFormatter.manipulateValues(data, true);
		columnTypes = determineColumnTypes(data);
	
		int i = 0;
		int numRow = newData.length;
		int[] uniqueCounts = new int[numRow];
		for(; i < numRow; i++) {
			CalculateEntropy e = new CalculateEntropy();
			e.setDataArr(newData[i]);
			e.addDataToCountHash();
			uniqueCounts[i] = 	e.getCountHash().keySet().size();
		}

		return uniqueCounts;	
	}
	
	public static String[] determineColumnTypes(ArrayList<Object []> list) {
		int numCols = list.get(0).length;
		String[] columnTypes = new String[numCols];
		//iterate through columns
		for(int j = 0; j < numCols; j++) {
			columnTypes[j] = determineColumnType(list,j);
		}
		return columnTypes;
	}
	
	private static String determineColumnType(ArrayList<Object []> list,int column) {
		//iterate through rows
		int numCategorical = 0;
		int numDouble = 0;
		int numInteger = 0;
		int numDate = 0;
		int numSimpleDate = 0;
		String type;
		for(int i = 0; i < list.size(); i++) {
			Object[] dataRow = list.get(i);
			if(dataRow[column] != null && !dataRow[column].toString().equals("")) {
				String colEntryAsString = dataRow[column].toString();
				if(!colEntryAsString.isEmpty()) {
					type = Utility.processType(colEntryAsString);
					if(type.equals("STRING")) {
						numCategorical++;
					}else if(type.equals("DOUBLE")) {
						numDouble++;
					} else if(type.equals("INTEGER")) {
						numInteger++;
					} else if(type.equals("DATE")) {
						numDate++;
					}else {
						numSimpleDate++;
					}
				}
			}
		}
		if(numDouble > numCategorical && numDouble > numInteger && numDouble > numDate && numDouble > numSimpleDate ) {
			return "DOUBLE";
		} else if(numInteger > numCategorical && numInteger > numDouble && numInteger > numDate && numInteger > numSimpleDate ) {
			return "INTEGER";
		} else if(numDate > numCategorical && numDate > numDouble && numDate > numInteger && numDate > numSimpleDate ) {
			return "DATE";
		} else if(numSimpleDate > numCategorical && numSimpleDate > numDouble && numSimpleDate > numDate && numSimpleDate > numInteger ) {
			return "SIMPLEDATE";
		} else {
			return "STRING";
		}
	}

	public static boolean compareCols(int mainCol, int otherCol) {
		Hashtable<String, String> values = new Hashtable<String, String>();
		int i = 0;
		int size = table.size();

		for(; i < size; i++) {
			Object[] row = table.get(i);
			String val1 = row[mainCol].toString();
			String val2 = row[otherCol].toString();
			if(values.containsKey(val1)) {
				if(!val2.equals(values.get(val1))) {
					System.out.println("NO MATCH! " + headers[mainCol] + " specific instance, " + val1 + ", has 2 different values for " + headers[otherCol] + " which equals " + values.get(val1) + " and " + val2);
					return false;
				}
			} else {
				values.put(val1, val2);
			}
		}

		return true;
	}

	public static void readExcelFile(String loc) {
		FileInputStream is = null;
		try {
			is = new FileInputStream(loc);
			XSSFWorkbook wb = new XSSFWorkbook(is);
			XSSFSheet xs = wb.getSheetAt(0);

			// get header row and store values
			XSSFRow headerRow = xs.getRow(0);
			int numCol = headerRow.getLastCellNum();
			headers = new String[numCol];
			int i = 0;
			for(; i < numCol; i++) {
				if(headerRow.getCell(i) != null) {
					headers[i] = headerRow.getCell(i).getStringCellValue().replaceAll("\n+|\r+", "");
				} else {
					headers[i] = "FIX NO NAME COLUMN";
				}
			}

			// loop through and store all values
			i = 1;
			int numRow = xs.getLastRowNum();
			for(; i <= numRow; i++) {
				XSSFRow currRow = xs.getRow(i);
				Object[] row = new Object[numCol];
				if(currRow != null) {
					int j = 0;
					for(; j < numCol; j++) {
						if(currRow.getCell(j) != null) {
							XSSFCell cell = currRow.getCell(j);
							if(cell.getCellType() == XSSFCell.CELL_TYPE_NUMERIC) {
								if (DateUtil.isCellDateFormatted(cell)) {
									Date date = (Date) cell.getDateCellValue();
									row[j] = date;
								} else {
									Double dbl = new Double(cell.getNumericCellValue());
									row[j] = dbl;
								}
							} else if(cell.getCellType() == XSSFCell.CELL_TYPE_STRING) {
								row[j] = cell.getStringCellValue();
							} else {
								row[j] = "";
							}
						} else {
							row[j] = "";
						}
					}
					table.add(row);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
