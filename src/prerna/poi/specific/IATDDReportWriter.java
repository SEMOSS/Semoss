/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.poi.specific;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class IATDDReportWriter {
	
	static final Logger logger = LogManager.getLogger(IndividualSystemTransitionReportWriter.class.getName());
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private XSSFWorkbook wb;
	private String selectedParam = "";
	private static String fileLoc = "";
	private static String templateLoc = "";

	public Hashtable<String,XSSFCellStyle> myStyles;
	
	public IATDDReportWriter(){}
	
	/**
	 * Creates a new workbook, sets the file location, and creates the styles to be used.
	 * @param systemName		String containing the name of the system to create the report for
	 */
	public void makeWorkbook(String selectedParam, String templateName) {
		this.selectedParam = selectedParam;
		
		fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "export" + DIR_SEPARATOR + "Reports" + DIR_SEPARATOR;
		templateLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "export" + DIR_SEPARATOR + "Reports" + DIR_SEPARATOR + templateName;

		fileLoc += selectedParam + "_Catalog_Report.xlsx";
		wb = new XSSFWorkbook();

		//if a report template exists, then create a copy of the template, otherwise create a new workbook
		if(templateLoc!=null)
			try{
				wb = (XSSFWorkbook) WorkbookFactory.create(new File(templateLoc));
			}
		catch(Exception e){
			wb = new XSSFWorkbook();
		}

		makeStyles(wb);
	}

	/**
	 * Writes the workbook to the file location.
	 */
	public boolean writeWorkbook() {
		boolean success = false;
		try {
			wb.setForceFormulaRecalculation(true);
			Utility.writeWorkbook(wb, fileLoc);
			success = true;
		} catch (Exception ex) {
			success = false;
			logger.error(Constants.STACKTRACE, ex);
		}
		return success;
	}

	public static String getFileLoc() {
		return fileLoc;
	}
	
	public void writeCatalogSheet(String sheetName, HashMap<String,Object> result, String[] headers) {
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>) result.get("data");
		
		for (int row = 0; row < dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			
			XSSFRow rowToWriteOn = sheetToWriteOver.getRow(row + 1);

			for (int col = 0; col < resultRowValues.length; col++) {
				XSSFCell cellToWriteOn = rowToWriteOn.getCell(col);
				if(resultRowValues[col] instanceof String) {
					String stringToWrite = ((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," ").replaceAll("~","/");
					if(stringToWrite.length()==0||stringToWrite.equals("NA"))
						stringToWrite = "TBD";
					cellToWriteOn.setCellValue(stringToWrite);
				}
			}			
		}
	    
	    wb.setPrintArea(0, 0, 3, 0, dataList.size());
	}
		
	/**
	 * Creates a cell border style in an Excel workbook
	 * @param wb 		Workbook to create the style
	 * @return style	XSSFCellStyle containing the format
	 */
	private static XSSFCellStyle createBorderedStyle(Workbook wb) {
		XSSFCellStyle style = (XSSFCellStyle)wb.createCellStyle();
		style.setBorderRight(BorderStyle.THIN);
		style.setRightBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderBottom(BorderStyle.THIN);
		style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderLeft(BorderStyle.THIN);
		style.setLeftBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderTop(BorderStyle.THIN);
		style.setTopBorderColor(IndexedColors.BLACK.getIndex());
		return style;
	}

	/**
	 * Creates a cell format style for an excel workbook
	 * @param workbook 	XSSFWorkbook to create the format
	 */
	private void makeStyles(XSSFWorkbook workbook) {
		myStyles = new Hashtable<String,XSSFCellStyle>();
		XSSFCellStyle headerStyle = createBorderedStyle(workbook);
		Font boldFont = workbook.createFont();
		boldFont.setBold(true);
		boldFont.setColor(IndexedColors.WHITE.getIndex());
		boldFont.setFontHeightInPoints((short) 10);
		headerStyle.setFont(boldFont);
		headerStyle.setAlignment(HorizontalAlignment.CENTER);
		headerStyle.setVerticalAlignment(VerticalAlignment.TOP);
		headerStyle.setFillForegroundColor(new XSSFColor(new java.awt.Color(54, 96, 146)));
		headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		myStyles.put("headerStyle",headerStyle);

		Font normalFont = workbook.createFont();
		normalFont.setFontHeightInPoints((short) 10);

		XSSFCellStyle normalStyle = createBorderedStyle(workbook);
		normalStyle.setWrapText(true);
		normalStyle.setFont(normalFont);
		normalStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		myStyles.put("normalStyle",normalStyle);

		Font boldBodyFont = workbook.createFont();
		boldBodyFont.setFontHeightInPoints((short) 10);
		boldBodyFont.setBold(true);

		XSSFCellStyle boldStyle = createBorderedStyle(workbook);
		boldStyle.setWrapText(true);
		boldStyle.setFont(boldBodyFont);
		boldStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		myStyles.put("boldStyle",boldStyle);
		
		XSSFCellStyle accountStyle = createBorderedStyle(workbook);
		accountStyle.setFont(boldBodyFont);
		accountStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		XSSFDataFormat df = wb.createDataFormat();
		accountStyle.setDataFormat(df.getFormat("$* #,##0.00"));
		myStyles.put("accountStyle", accountStyle);
	}

}
