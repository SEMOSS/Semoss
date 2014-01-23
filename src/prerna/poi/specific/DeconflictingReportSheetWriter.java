/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.poi.specific;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Utility;

/**
 * Creates the Deconflicting/Missing ICD Data report
 * Used in conjunction with DeconflictingReportButtonListener 
 */
public class DeconflictingReportSheetWriter {

	/**
	 * Reads an excel workbook specified as a template to add information
	 * If no template is found, create a new workbook
	 * @param fileLoc 		String containing the file location to save the workbook
	 * @param hash 			Hashtable<String,ArrayList<ArrayList<String>>> containing the data to write to the excel workbook  
	 * @param readFileLoc	String containing the file location if a template is being used
	 * @param order			ArrayList<String> containing the order of the sheets to write in the workbook
	 */
	public void ExportLoadingSheets(String fileLoc, Hashtable<String, ArrayList<ArrayList<String>>> hash, String readFileLoc,ArrayList<String> order) {
		//if a report template exists, then create a workbook with these sheets
		XSSFWorkbook wb;
		if(readFileLoc!=null)
			try{
				wb = (XSSFWorkbook)WorkbookFactory.create(new File(readFileLoc));
			}catch(Exception e){
				Utility.showMessage("Warning! Could not find template workbook /n Creating a new workbook");
				wb = new XSSFWorkbook();
			}
		else{
			wb = new XSSFWorkbook();
		}
		for(String key: order){
			ArrayList<ArrayList<String>> sheetVector = hash.get(key);
			writeSheet(key, sheetVector, wb);
		}
		wb.setSheetHidden(wb.getSheetIndex("TAB-I"), true);
		XSSFFormulaEvaluator.evaluateAllFormulaCells(wb);
		Utility.writeWorkbook(wb, fileLoc);
	}

	/**
	 * Writes given information onto the sheet in the workbook 
	 * @param key 			String containing the name of the sheet to add the information
	 * @param sheetVector	ArrayList<ArrayList<String>> containing the information to write in the sheet
	 * @param workbook		XSSFWorkbook to get the sheet
	 */
	public void writeSheet(String key, ArrayList<ArrayList<String>> sheetVector, XSSFWorkbook workbook) {
		XSSFSheet worksheet = workbook.getSheet(key);
		//just print everything without formating or headers
		int rowCount=sheetVector.size();
		for (int row=0; row<rowCount;row++){
			XSSFRow row1 = worksheet.createRow(row);
			for (int col=0; col<sheetVector.get(row).size();col++) {
				XSSFCell cell = row1.createCell(col);
				if(sheetVector.get(row).get(col) != null) {
					{
						String val = sheetVector.get(row).get(col);
						if(val.startsWith("="))
							cell.setCellFormula(val.substring(1));
						else if(key.equals("TAB-A"))
							cell.setCellValue(sheetVector.get(row).get(col).replace("_"," ").replace("\"", ""));
						else
							cell.setCellValue(sheetVector.get(row).get(col).replace("\"", ""));
					}
				}
			}
		}
		workbook.setSheetHidden(workbook.getSheetIndex(key),true);
	}
}
