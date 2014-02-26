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

import org.apache.poi.hssf.usermodel.DVConstraint;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDataValidation;
import org.apache.poi.xssf.usermodel.XSSFDataValidationConstraint;
import org.apache.poi.xssf.usermodel.XSSFDataValidationHelper;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Utility;

/**
 * Creates the source reports for vendors
 * Used in conjunction with SourceReportButtonListener
 */
public class ReportSheetWriter {

	public Hashtable<String,XSSFCellStyle> myStyles;
	public String RFPName="RFP1";
	public String[] supports = {"SupportLevel","CustomizationEffort","System","Comments"};
	public int supportsLength=supports.length;


	/**
	 * Creates a workbook containing one sheet with the data object to data element mappings
	 * @param dataFileLoc 	String containing the location to create the excel workbook
	 * @param list 			ArrayList<String[]> containing the data to export in the workbook
	 */
	public void exportDataWorkbook(String dataFileLoc,ArrayList<String[]> list)
	{
		XSSFWorkbook wb = new XSSFWorkbook();
		makeDataStyles(wb);
		XSSFSheet worksheet = wb.createSheet("Data->DataElement");

		int rowCount=list.size();
		for (int row=0; row<rowCount;row++){
			XSSFRow row1 = worksheet.createRow(row);
			for (int col=0; col<list.get(row).length;col++) {
				XSSFCell cell = row1.createCell(col);
				if(list.get(row)[col] != null) {
					String val = list.get(row)[col];
					if(row==0)
					{
						cell.setCellValue(val.replace("\"", "").replace("_", ""));
						cell.setCellStyle((XSSFCellStyle)myStyles.get("headerDataStyle"));
					}
					else
					{
						cell.setCellValue(val.replace("\"", ""));
						cell.setCellStyle((XSSFCellStyle)myStyles.get("normalDataStyle"));
					}
				}
			}
		}
		if(list.size()>0)
			for(int col=0; col<list.get(0).length;col++)
				worksheet.setColumnWidth(col, 256*35);

		Utility.writeWorkbook(wb, dataFileLoc);
		Utility.showMessage("Export successful: " + dataFileLoc);
	}

	/**
	 * Creates a workbook containing information regarding vendor selection
	 * @param fileLoc	 	String containing the location to create the excel workbook
	 * @param hash 			Hashtable<String,ArrayList<String[]>> containing all the information to write to the Excel workbook
	 * @param readFileLoc 	String containing the location of a template workbook to read
	 * @param order 		ArrayList<String> specifying the order of the sheets to create the workbook
	 */
	public void ExportLoadingSheets(String fileLoc, Hashtable<String, ArrayList<String[]>> hash, String readFileLoc,ArrayList<String> order) {
		//if a report template exists, then create a workbook with these sheets
		XSSFWorkbook wb;
		if(readFileLoc != null)
			try{
				wb = (XSSFWorkbook)WorkbookFactory.create(new File(readFileLoc));
			}catch(Exception e){
				Utility.showMessage("Warning! Could not find template workbook /n Creating a new workbook");
				wb = new XSSFWorkbook();
			}
		else
			wb = new XSSFWorkbook();
		makeStyles(wb);


		for(String key: order){
			ArrayList<String[]> sheetVector = hash.get(key);
			writeSheet(key, sheetVector, wb);
		}

		Utility.writeWorkbook(wb, fileLoc);
		Utility.showMessage("Export successful: " + fileLoc);
	}

	/**
	 * Writes a sheet in a specified Excel workbook
	 * @param key 			String containing the name of the sheet
	 * @param sheetVector	ArrayList<String[]> containing the information to write onto the sheet
	 * @param workbook		XSSFWorkbook to create the sheet
	 */
	public void writeSheet(String key, ArrayList<String[]> sheetVector, XSSFWorkbook workbook) {
		XSSFSheet worksheet = workbook.createSheet(key);
		Boolean addVendorSection = !key.contains("Defined");//&&!key.contains("EnterpriseEffect");
		int rowCount=sheetVector.size();
		for (int row=0; row<rowCount;row++){
			XSSFRow row1 = worksheet.createRow(row);
			for (int col=0; col<sheetVector.get(row).length;col++) {
				XSSFCell cell = row1.createCell(col);
				if(sheetVector.get(row)[col] != null) {
					String val = sheetVector.get(row)[col];
					if(row==0)
					{
						cell.setCellValue(sheetVector.get(row)[col].replace("\"", "").replace("_", ""));
						cell.setCellStyle((XSSFCellStyle)myStyles.get("headerStyle"));
					}
					else
					{
						cell.setCellValue(sheetVector.get(row)[col].replace("\"", "").replace("_", " "));
						cell.setCellStyle((XSSFCellStyle)myStyles.get("normalStyle"));
					}
				}
			}
			if(addVendorSection)
			{
				//if this is a sheet where vendor will input reponses, then make the headers and give cells borders
				if(row==0)
					for(int index=0;index<supportsLength;index++)
					{
						XSSFCell cell = row1.createCell(row1.getLastCellNum());
						cell.setCellValue(supports[index]);
						cell.setCellStyle((XSSFCellStyle)myStyles.get("headerStyleVendor"));
					}
				else
					for(int index=0;index<supportsLength;index++)
					{
						XSSFCell cell = row1.createCell(row1.getLastCellNum());
						cell.setCellValue("");
						cell.setCellStyle((XSSFCellStyle)myStyles.get("normalStyle"));
					}		

			}
		}
		if(sheetVector.size()>0)
			for(int col=0; col<sheetVector.get(0).length;col++)
				worksheet.setColumnWidth(col, 256*35);
		if(addVendorSection&&worksheet.getLastRowNum()>0)
		{
			//if this is a sheet where vendor will be putting responses, add the filter options, set the column widths, and add the drop down validation boxes
			worksheet.setAutoFilter(new CellRangeAddress(0,0,0,worksheet.getRow(0).getLastCellNum()-2));
			for(int col=0; col<5;col++)
				worksheet.setColumnWidth(worksheet.getRow(0).getLastCellNum()-1-col, 256*25);
			XSSFDataValidationHelper dvHelper = new XSSFDataValidationHelper(worksheet);
			XSSFDataValidationConstraint dvConstraint = (XSSFDataValidationConstraint)dvHelper.createExplicitListConstraint(new String[]{"Supports out of box","Supports with configuration","Supports with customization","Does not support"});
			CellRangeAddressList addressList = new CellRangeAddressList(1, worksheet.getLastRowNum(),sheetVector.get(0).length, sheetVector.get(0).length);
			XSSFDataValidation validation = (XSSFDataValidation)dvHelper.createValidation(dvConstraint, addressList);
			validation.setShowErrorBox(true);
			worksheet.addValidationData(validation);
			XSSFDataValidationHelper dvHelperCost = new XSSFDataValidationHelper(worksheet);
			XSSFDataValidationConstraint dvConstraintCost = (XSSFDataValidationConstraint)dvHelperCost.createNumericConstraint(DVConstraint.ValidationType.DECIMAL, DVConstraint.OperatorType.GREATER_OR_EQUAL, "0", "");
			CellRangeAddressList addressListCost = new CellRangeAddressList(1, worksheet.getLastRowNum(),sheetVector.get(0).length+1, sheetVector.get(0).length+1);
			XSSFDataValidation validationCost = (XSSFDataValidation)dvHelperCost.createValidation(dvConstraintCost, addressListCost);
			validationCost.setShowErrorBox(true);
			worksheet.addValidationData(validationCost);
		}

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

		XSSFCellStyle headerStyleVendor = createBorderedStyle(workbook);
		headerStyleVendor.setFont(boldFont);
		headerStyleVendor.setAlignment(CellStyle.ALIGN_CENTER);
		headerStyleVendor.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		headerStyleVendor.setFillForegroundColor(new XSSFColor(new java.awt.Color(0, 112, 192)));
		headerStyleVendor.setFillPattern(CellStyle.SOLID_FOREGROUND);
		headerStyleVendor.setWrapText(true);
		myStyles.put("headerStyleVendor",headerStyleVendor);

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

	/**
	 * Creates a cell format style for an excel workbook
	 * @param workbook 	XSSFWorkbook to create the format
	 */
	private void makeDataStyles(XSSFWorkbook workbook)
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
		myStyles.put("headerDataStyle",headerStyle);

		Font normalFont = workbook.createFont();
		normalFont.setFontHeightInPoints((short) 10);

		XSSFCellStyle normalStyle = createBorderedStyle(workbook);
		normalStyle.setWrapText(true);
		normalStyle.setFont(normalFont);
		normalStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		myStyles.put("normalDataStyle",normalStyle);

	}
}
