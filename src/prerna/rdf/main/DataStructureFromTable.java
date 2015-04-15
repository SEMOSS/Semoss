package prerna.rdf.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;

import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.algorithm.learning.similarity.GenerateEntropyDensity;

public class DataStructureFromTable {

	private static ArrayList<Object[]> table = new ArrayList<Object[]>();
	private static String[] headers;

	public static void main(String[] args) {

		///////////////////////////////////////////////////
		//TODO: change to correct file location when testing
		String loc = "C:\\Users\\mahkhalil\\Desktop\\ConstructingDataStructure.xlsx";
		///////////////////////////////////////////////////

		readExcelFile(loc);

		GenerateEntropyDensity eDensity = new GenerateEntropyDensity(table,true);
		double[] entropyArr = eDensity.generateEntropy();
		String[] columnTypes = eDensity.getColumnTypes();
		
		System.out.println(Arrays.toString(headers));
		System.out.println(Arrays.toString(entropyArr));
		
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
				if(entropyArr[i] < entropyArr[i+1]) {
					change = true;
					double valTemp = entropyArr[i];
					entropyArr[i] = entropyArr[i+1];
					entropyArr[i+1] = valTemp;
					
					int indexTemp = indexFromMaxToMin[i];
					indexFromMaxToMin[i] = indexFromMaxToMin[i+1];
					indexFromMaxToMin[i+1] = indexTemp;
				}
			}
		}
		
		System.out.println(Arrays.toString(entropyArr));
		for(i = 0; i < numCol; i++) {
			System.out.print(headers[indexFromMaxToMin[i]] + ", ");
		}
		System.out.println("");

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
				if(i != j && !usedCol[j]) {
					if(compareCols(indexFromMaxToMin[i],indexFromMaxToMin[j])) {
						if(matches.containsKey(headers[indexFromMaxToMin[i]])) {
							matches.get(headers[indexFromMaxToMin[i]]).add(headers[indexFromMaxToMin[j]]);
						} else {
							ArrayList<String> list = new ArrayList<String>();
							list.add(headers[indexFromMaxToMin[j]]);
							matches.put(headers[indexFromMaxToMin[i]], list);
						}
						usedCol[j] = true;
					}
				}
			}
		}
		
		System.out.println(matches);
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
