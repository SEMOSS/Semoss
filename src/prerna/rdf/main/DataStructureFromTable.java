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

public class DataStructureFromTable {

	private static ArrayList<Object[]> table = new ArrayList<Object[]>();
	private static String[] headers;
	private static String[] columnTypes;

	public static void main(String[] args) {

		///////////////////////////////////////////////////
		//TODO: change to correct file location when testing
//		String loc = "C:\\Users\\mahkhalil\\Desktop\\ConstructingDataStructure.xlsx";
		String loc = "C:\\Users\\mahkhalil\\Desktop\\FY16 BTA List.xlsx";
		///////////////////////////////////////////////////

		readExcelFile(loc);
		//		GenerateEntropyDensity eDensity = new GenerateEntropyDensity(table,true);
		//		double[] entropyArr = eDensity.generateEntropy();
		//		String[] columnTypes = eDensity.getColumnTypes();

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

		boolean[] usedCol = new boolean[numCol];
		Hashtable<String, ArrayList<String>> matches = new Hashtable<String, ArrayList<String>>();

		i = 0;
		for(; i < numCol; i++) {
			//ignore numerical values
			if(!columnTypes[indexFromMaxToMin[i]].equals("STRING")) {
				continue;
			}
			int j = 0;
			for(; j < numCol; j++) {
				if(i != j && !usedCol[indexFromMaxToMin[j]]) {
					if(compareCols(indexFromMaxToMin[i],indexFromMaxToMin[j])) {
						boolean useInverse = false;
						if(compareCols(indexFromMaxToMin[j],indexFromMaxToMin[i])) {
							if(i > j) {
								useInverse = true;
								// if inverse also works, take the value from the most left as the concept
								if(matches.containsKey(headers[indexFromMaxToMin[j]])) {
									matches.get(headers[indexFromMaxToMin[j]]).add(headers[indexFromMaxToMin[i]]);
								} else {
									ArrayList<String> list = new ArrayList<String>();
									list.add(headers[indexFromMaxToMin[i]]);
									matches.put(headers[indexFromMaxToMin[j]], list);
								}
							}
							usedCol[indexFromMaxToMin[i]] = true;
						}
						if(!useInverse) {
							if(matches.containsKey(headers[indexFromMaxToMin[i]])) {
								matches.get(headers[indexFromMaxToMin[i]]).add(headers[indexFromMaxToMin[j]]);
							} else {
								ArrayList<String> list = new ArrayList<String>();
								list.add(headers[indexFromMaxToMin[j]]);
								matches.put(headers[indexFromMaxToMin[i]], list);
							}
							usedCol[indexFromMaxToMin[j]] = true;
						}
						
					}
				}
			}
		}
		
		i = 0;
		for(; i < numCol; i++) {
			if(!matches.containsKey(headers[i])) {
				matches.put(headers[i], new ArrayList<String>());
			}
		}

		System.out.println(matches);
	}

	private static int[] getUniqueCounts(ArrayList<Object[]> data) {
		Object[][] newData = AlgorithmDataFormatter.manipulateValues(data, true);
		columnTypes = AlgorithmDataFormatter.determineColumnTypes(data);
	

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
					headers[i] = headerRow.getCell(i).getStringCellValue();
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
