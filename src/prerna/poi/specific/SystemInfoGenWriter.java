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

import java.util.ArrayList;
import java.util.Hashtable;

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
 *This will take the information from the SystemInfoGenProcessor and write a system info report to an excel file
 */
public class SystemInfoGenWriter {

	Logger logger = Logger.getLogger(getClass());
	public Hashtable<String,XSSFCellStyle> myStyles;

	/**
	 * Retrieves the query results from the SystemInfoGenProcessor and creates the System Info Report
	 * @param fileLoc			String containing the file location to write the report to
	 * @param sysList			List containing all the systems in alphabetical order
	 * @param headersList		List containing all the names of variables to put in the report
	 * @param systemInfoHash	Hashtable containing all the query results from the SystemInfoGenProcessor
	 */
	public void exportSystemInfoReport(String fileLoc, ArrayList<String> sysList,ArrayList<String> headersList, Hashtable systemInfoHash) {
		XSSFWorkbook wb =new XSSFWorkbook();
		writeListSheet(wb,sysList,headersList,systemInfoHash);
		wb.setForceFormulaRecalculation(true);
		Utility.writeWorkbook(wb, fileLoc);

	}

	/**
	 * Writes the data from the queries to the sheet specified in list format
	 * @param wb				XSSFWorkbook containing the sheet to populate
	 * @param sysList			List containing all the systems in alphabetical order
	 * @param headersList		List containing all the names of variables to put in the report
	 * @param result			ArrayList containing the output of the query
	 */
	public void writeListSheet(XSSFWorkbook wb, ArrayList<String> sysList, ArrayList<String> headersList, Hashtable result){
		
		makeStyles(wb);
		//make the header rows with special format
		XSSFSheet worksheet = wb.createSheet("System Info");
		XSSFRow row0 = worksheet.createRow(0);
		for (int col=0; col<headersList.size();col++){

			XSSFCell cell = row0.createCell(col);
			cell.setCellValue(headersList.get(col).replaceAll("_", " "));
			cell.setCellStyle((XSSFCellStyle)myStyles.get("headerStyle"));
		}
		//fill in data
		for (int row=0; row<sysList.size();row++){
			XSSFRow row1 = worksheet.createRow(row+1);
			String sys = sysList.get(row);
			XSSFCell cell0 = row1.createCell(0);
			cell0.setCellValue(sys);
			cell0.setCellStyle((XSSFCellStyle)myStyles.get("normalStyle"));
			if(result.containsKey(sys)){
				Hashtable sysHash = (Hashtable) result.get(sys);
				for (int col=1; col<headersList.size();col++) {
					XSSFCell cell = row1.createCell(col);
					cell.setCellStyle((XSSFCellStyle)myStyles.get("normalStyle"));
					String varName = headersList.get(col);
					if(sysHash.containsKey(varName))
					{
						Object val = sysHash.get(varName);
						if(val instanceof Double)
							cell.setCellValue((Double)val);
						else
							cell.setCellValue(((String)val).replace("\"", "").replace("_", " "));
					}
				}
			
			}
			
		}
		//resize the columns
		if(headersList.size()>0)
			for(int col=0; col<headersList.size();col++)
				worksheet.setColumnWidth(col, 256*35);
		
		worksheet.createFreezePane(0, 1);
		String columnLetter = CellReference.convertNumToColString(headersList.size());
		worksheet.setAutoFilter(CellRangeAddress.valueOf("A1:"+columnLetter+ result.size()+1 ));

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
