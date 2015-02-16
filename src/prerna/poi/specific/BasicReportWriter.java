/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.util.CellReference;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Utility;

/**
 *This will write a basic list report to an excel file with basic table formatting.
 */
public class BasicReportWriter {

	static final Logger logger = LogManager.getLogger(BasicReportWriter.class.getName());
	public Hashtable<String,XSSFCellStyle> myStyles;
	XSSFWorkbook wb;
	String fileLoc;
	
	/**
	 * Retrieves the table list to output from the processor and creates an excel file with a single table sheet.
	 * @param fileLoc			String containing the file location to write the report to
	 * @param sheetName			String containing the name for the sheet.
	 * @param headersList		List containing all the names of columns to put in the report
	 * @param resultList		List containing the values for the table to output
	 */
	public void exportReport(String fileLoc, String sheetName, ArrayList<String> headersList, ArrayList<Object[]> resultList) {
		makeWorkbook(fileLoc);
		writeListSheet(sheetName,headersList,resultList);
		writeWorkbook();

	}
	
	/**
	 * Creates a new workbook, sets the file location, and creates the styles to be used.
	 */
	public void makeWorkbook(String fileLoc)
	{
		this.fileLoc = fileLoc;
		wb = new XSSFWorkbook();
		makeStyles(wb);
	}
	
	/**
	 * Writes the workbook to the file location.
	 */
	public void writeWorkbook()
	{
		wb.setForceFormulaRecalculation(true);
		Utility.writeWorkbook(wb, fileLoc);
	}
	
	/**
	 * Retrieves the table list to output from the processor and creates an excel file with a single table sheet.
	 * @param headersList		List containing all the names of columns to put in the report
	 * @param rowList			List containing the keys to the hash in the desired order of the rows
	 * @param hash				Hashtable containing a hashtable for each row 
	 */
	public ArrayList<Object[]> makeListFromHash(ArrayList<String> headersList, ArrayList<String> rowList,Hashtable hash) {
		ArrayList<Object[]> masterList = new ArrayList<Object[]>();
		for (int rowInd=0; rowInd<rowList.size();rowInd++){
			Object[] rowArray = new Object[headersList.size()];
			String sys = rowList.get(rowInd);
			rowArray[0] = sys;
			if(hash.containsKey(sys)){
				Hashtable sysHash = (Hashtable) hash.get(sys);
				for (int col=1; col<headersList.size();col++) {
					String varName = headersList.get(col);
					if(sysHash.containsKey(varName))
					{
						Object val = sysHash.get(varName);
						rowArray[col] = val;
					}
				}
			}
			masterList.add(rowArray);
		}
		return masterList;
	}

	/**
	 * Writes the data from the queries to the sheet specified in list format
	 * @param wb				XSSFWorkbook containing the sheet to populate
	 * @param sysList			List containing all the systems in alphabetical order
	 * @param headersList		List containing all the names of variables to put in the report
	 * @param result			ArrayList containing the output of the query
	 */
	public void writeListSheet(String sheetName, ArrayList<String> headersList, ArrayList<Object[]> resultList){

		//make the header rows with special format
		XSSFSheet worksheet = wb.createSheet(sheetName);
		XSSFRow row0 = worksheet.createRow(0);
		for (int col=0; col<headersList.size();col++){

			XSSFCell cell = row0.createCell(col);
			cell.setCellValue(headersList.get(col).replaceAll("_", " "));
			cell.setCellStyle((XSSFCellStyle)myStyles.get("headerStyle"));
		}
		//fill in data
		for (int row=0; row<resultList.size();row++){
			XSSFRow row1 = worksheet.createRow(row+1);
			Object[] resultRow = resultList.get(row);
			for(int colInd=0;colInd<resultRow.length;colInd++)
			{
				XSSFCell cell = row1.createCell(colInd);
				cell.setCellStyle((XSSFCellStyle)myStyles.get("normalStyle"));
				Object val = resultRow[colInd];
				if(val instanceof Double)
					cell.setCellValue((Double)val);
				else if(val instanceof Integer)
					cell.setCellValue((Integer)val);
				else if(val!=null)
					cell.setCellValue(((String)val).replace("\"", "").replace("_", " "));
				
			}
		}
		//resize the columns
		if(headersList.size()>0)
			for(int col=0; col<headersList.size();col++)
				worksheet.setColumnWidth(col, 256*35);
		
		worksheet.createFreezePane(0, 1);
		String columnLetter = CellReference.convertNumToColString(headersList.size());
		worksheet.setAutoFilter(CellRangeAddress.valueOf("A1:"+columnLetter+ resultList.size()+1 ));

	}
	
	
	/**
	 * Creates a cell boarder style in an Excel workbook
	 * @param wb 		Workbook to create the style
	 * @return style	XSSFCellStyle containing the format
	 */
	private static XSSFCellStyle createBorderedStyle(Workbook wb){
		XSSFCellStyle style = (XSSFCellStyle)wb.createCellStyle();
		style.setBorderRight(CellStyle.BORDER_THIN);
		style.setRightBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderLeft(CellStyle.BORDER_THIN);
		style.setLeftBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderTop(CellStyle.BORDER_THIN);
		style.setTopBorderColor(IndexedColors.BLACK.getIndex());
		return style;
	}

	/**
	 * Creates a cell format style for an excel workbook
	 * @param workbook 	XSSFWorkbook to create the format
	 */
	private void makeStyles(XSSFWorkbook workbook)
	{
		myStyles = new Hashtable<String,XSSFCellStyle>();
		XSSFCellStyle headerStyle = createBorderedStyle(workbook);
		Font boldFont = workbook.createFont();
		boldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
		boldFont.setColor(IndexedColors.WHITE.getIndex());
		boldFont.setFontHeightInPoints((short) 10);
		headerStyle.setFont(boldFont);
		headerStyle.setAlignment(CellStyle.ALIGN_CENTER);
		headerStyle.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		headerStyle.setFillForegroundColor(new XSSFColor(new java.awt.Color(54, 96, 146)));
		headerStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);
		myStyles.put("headerStyle",headerStyle);
	
		Font normalFont = workbook.createFont();
		normalFont.setFontHeightInPoints((short) 10);
	
		XSSFCellStyle normalStyle = createBorderedStyle(workbook);
		normalStyle.setWrapText(true);
		normalStyle.setFont(normalFont);
		normalStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		myStyles.put("normalStyle",normalStyle);
	
		Font boldBodyFont = workbook.createFont();
		boldBodyFont.setFontHeightInPoints((short) 10);
		boldBodyFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
	
		XSSFCellStyle boldStyle = createBorderedStyle(workbook);
		boldStyle.setWrapText(true);
		boldStyle.setFont(boldBodyFont);
		boldStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		myStyles.put("boldStyle",boldStyle);
	}

}
