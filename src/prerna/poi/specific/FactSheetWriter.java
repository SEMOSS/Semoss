/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.poi.specific;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.extensions.XSSFHeaderFooter;

import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 *This will take the information from the fact sheet processor and write a fact sheet report to an excel file for a given system name
 */
public class FactSheetWriter {

	private static final Logger logger = LogManager.getLogger(FactSheetWriter.class.getName());

	public String systemName;

	/**
	 * Retrieves the query results for a given system from the fact sheet processor and creates the fact sheet report
	 * @param systemName		String containing the name of the system to create the fact sheet
	 * @param fileLoc			String containing the file location of the fact sheet
	 * @param templateFileLoc	String containing the location of the fact sheet template
	 * @param systemDataHash	Hashtable containing all the query results from the fact sheet processor for a given system
	 */
	public void exportFactSheets(String systemName, String fileLoc, String templateFileLoc, Hashtable systemDataHash) {
		XSSFWorkbook wb;
		this.systemName = systemName;
		//if a report template exists, then create a copy of the template, otherwise create a new workbook
		if(templateFileLoc!=null) {
			try {
				wb = (XSSFWorkbook) WorkbookFactory.create(new File(templateFileLoc));
			} catch (InvalidFormatException e) {
				wb = new XSSFWorkbook();
			} catch (IOException e) {
				wb = new XSSFWorkbook();
			} 
		} else {
			wb = new XSSFWorkbook();
		}

		//create an Arraylist of results for each query - retrieve results from Hashtable
		ArrayList systemSWResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_SW_QUERY);
		ArrayList systemHWResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_HW_QUERY);
		ArrayList maturityResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_MATURITY_QUERY);
		ArrayList icdResults = (ArrayList) systemDataHash.get(ConstantsTAP.LIST_OF_INTERFACES_QUERY);
		ArrayList dataResults = (ArrayList) systemDataHash.get(ConstantsTAP.DATA_PROVENANCE_QUERY);
		ArrayList bluResults = (ArrayList) systemDataHash.get(ConstantsTAP.BUSINESS_LOGIC_QUERY);
		ArrayList siteResults = (ArrayList) systemDataHash.get(ConstantsTAP.SITE_LIST_QUERY);
		ArrayList budgetResults = (ArrayList) systemDataHash.get(ConstantsTAP.BUDGET_QUERY);
		ArrayList sysSimResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYS_SIM_QUERY);
		ArrayList sysResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYS_QUERY);
		ArrayList pocResults = (ArrayList) systemDataHash.get(ConstantsTAP.POC_QUERY);
		ArrayList valueResults = (ArrayList) systemDataHash.get(ConstantsTAP.VALUE_QUERY);		
		ArrayList uniqueDataResults = (ArrayList) systemDataHash.get(ConstantsTAP.UNIQUE_DATA_PROVENANCE_QUERY);
		ArrayList uniqueBLUResults = (ArrayList) systemDataHash.get(ConstantsTAP.UNIQUE_BUSINESS_LOGIC_QUERY);
		ArrayList systemDescriptionResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_DESCRIPTION_QUERY);
		ArrayList systemHighlightsResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_HIGHLIGHTS_QUERY);
		ArrayList userTypesResults = (ArrayList) systemDataHash.get(ConstantsTAP.USER_TYPES_QUERY);
		ArrayList userInterfacesResults = (ArrayList) systemDataHash.get(ConstantsTAP.USER_INTERFACES_QUERY);
		//		ArrayList businessProcessResults = (ArrayList) systemDataHash.get(ConstantsTAP.BUSINESS_PROCESS_QUERY);
		ArrayList ppiResults = (ArrayList) systemDataHash.get(ConstantsTAP.PPI_QUERY);
		ArrayList capabilitiesSupportedResults = (ArrayList) systemDataHash.get(ConstantsTAP.CAPABILITIES_SUPPORTED_QUERY);

		//Find unique BLU/Data (for bolding)
		ArrayList uniqueDataSystems = new ArrayList();
		ArrayList uniqueData = new ArrayList();
		for (int i = 0; i<uniqueDataResults.size(); i++) {
			ArrayList row = (ArrayList) uniqueDataResults.get(i);
			if (((Double) row.get(2)).intValue() == 1) {
				uniqueDataSystems.add((String) row.get(0));
				uniqueData.add((String) row.get(1));
			}
		}				
		ArrayList uniqueBLUSystems = new ArrayList();
		ArrayList uniqueBLU = new ArrayList();
		for (int i = 0; i<uniqueBLUResults.size(); i++) {
			ArrayList row = (ArrayList) uniqueBLUResults.get(i);
			if (((Double) row.get(2)).intValue() == 1) {
				uniqueBLUSystems.add((String) row.get(0));
				uniqueBLU.add((String) row.get(1));
			}
		}		

		writeSystemOverviewSheet(wb, sysResults, valueResults, maturityResults, systemDescriptionResults, systemHighlightsResults, userTypesResults, userInterfacesResults, pocResults, capabilitiesSupportedResults, ppiResults);
		writeListOfInterfacesSheet(wb, icdResults);
		writeDataProvenanceSheet(wb, dataResults, uniqueDataSystems, uniqueData);
		writeBusinessLogicSheet(wb, bluResults, uniqueBLUSystems, uniqueBLU);	
		if (sysSimResults != null) {
			writeSystemSimilaritySheet(wb, sysSimResults, sysResults);
		}
		writeDeploymentStrategySheet(wb);
		writeSiteListSheet(wb, siteResults);
		writeBudgetSheet(wb, budgetResults);
		writeFeederSheet(wb, systemSWResults, systemHWResults);	
		wb.setForceFormulaRecalculation(true);
		wb.setSheetHidden(wb.getSheetIndex("Feeder"), true);
		Utility.writeWorkbook(wb, fileLoc);

	}

	/**
	 * Writes the System Overview Sheet in the workbook
	 * @param wb							XSSFWorkbook containing the System Overview Sheet to populate
	 * @param sysResults					ArrayList containing the top 5 similar system
	 * @param valueResults					ArrayList containing the similar system values
	 * @param maturityResults				ArrayList containing the system maturity
	 * @param systemDescriptionResults		ArrayList containing the system description
	 * @param systemHighlightsResults		ArrayList containing the system highlights
	 * @param userTypesResults				ArrayList containing the type of users
	 * @param userInterfacesResults			ArrayList containing the type of user interfaces
	 * @param pocResults					ArrayList containing the system point of contact
	 * @param capabilitiesSupportedResults	ArrayList containing the capabilities supported results
	 * @param ppiResults					ArrayList containing the system owner
	 */
	public void writeSystemOverviewSheet(XSSFWorkbook wb, ArrayList sysResults, ArrayList valueResults, ArrayList maturityResults, ArrayList systemDescriptionResults, ArrayList systemHighlightsResults, ArrayList userTypesResults, ArrayList userInterfacesResults, ArrayList pocResults, ArrayList capabilitiesSupportedResults, ArrayList ppiResults) {
		XSSFSheet sheetToWriteOver = wb.getSheet("System Overview");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(2);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
		writeHeader(wb, sheetToWriteOver);
		cellToWriteOn.setCellValue(systemName);
		rowToWriteOn = sheetToWriteOver.getRow(4);
		cellToWriteOn = rowToWriteOn.getCell(13);
		cellToWriteOn.setCellValue(DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()));	

		//System Description
		for (int i=0; i<systemDescriptionResults.size(); i++) {	
			ArrayList description = (ArrayList) systemDescriptionResults.get(i);
			for(int j = 0; j < description.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(3);
				cellToWriteOn = rowToWriteOn.getCell(1);
				String value = (String) description.get(j);
				cellToWriteOn.setCellValue((value.replaceAll("_", " ")).replaceAll("\"", ""));
			}
		}	

		//POC
		for (int i=0; i<pocResults.size(); i++) {	
			ArrayList poc = (ArrayList) pocResults.get(i);
			for(int j = 0; j < poc.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(4);
				cellToWriteOn = rowToWriteOn.getCell(7);
				String value = (String) poc.get(j);
				cellToWriteOn.setCellValue((value.replaceAll("_", " ")).replaceAll("\"",""));
			}
		}

		//PPI Owner
		String ppiConcat = "";
		if(ppiResults.size()>0)
			ppiConcat = (String)((ArrayList)ppiResults.get(0)).get(0);
		for(int i=1;i<ppiResults.size();i++)
		{
			ppiConcat +=", "+(String)((ArrayList)ppiResults.get(i)).get(0);
		}
		rowToWriteOn = sheetToWriteOver.getRow(4);
		cellToWriteOn = rowToWriteOn.getCell(3);
		cellToWriteOn.setCellValue((ppiConcat.replaceAll("_", " ")).replaceAll("\"",""));

		//System Highlights
		for (int i=0; i<systemHighlightsResults.size(); i++) {	
			ArrayList highlights = (ArrayList) systemHighlightsResults.get(i);			
			for (int j=0; j< highlights.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(16);
				if (j==0) //Number of Users
					cellToWriteOn = rowToWriteOn.getCell(1);
				if (j==1) //Number of Consoles
					cellToWriteOn = rowToWriteOn.getCell(3);
				if (j==2) //Availability - Required
					cellToWriteOn = rowToWriteOn.getCell(5);
				if (j==3) //Availability - Actual
					cellToWriteOn = rowToWriteOn.getCell(6);
				if (j==4) //Transactional/Intelligence
					cellToWriteOn = rowToWriteOn.getCell(7);
				if (j==5) //Daily Transactions
					cellToWriteOn = rowToWriteOn.getCell(9);
				if (j==6) //Date ATO Received
					cellToWriteOn = rowToWriteOn.getCell(12);
				if (j==7) //End of Support Date
					cellToWriteOn = rowToWriteOn.getCell(14);
				if (j==8) //Garrison/Theater
					cellToWriteOn = rowToWriteOn.getCell(16);	
				if (highlights.get(j) instanceof String) {
					String highlight = (String) highlights.get(j);
					highlight = highlight.replaceAll("\"", "");					
					if ( (j==6 || j==7) && ( (highlight.length() >= 10) && (!highlight.equals("TBD")) && (!highlight.equals("")) && (!highlight.equals("NA")) ) ) {
						cellToWriteOn.setCellValue(highlight.substring(0, 10));
					}
					else if(highlight.equals("NA")||highlight.equals("TBD"))
					{
						cellToWriteOn.setCellValue("Unknown");
					}
					else					
						cellToWriteOn.setCellValue(highlight);
				}
				if (highlights.get(j) instanceof Double) {
					double highlight = (Double) highlights.get(j);
					cellToWriteOn.setCellValue(highlight);
				}
			}
		}

		//System Value
		for (int i=0; i<maturityResults.size(); i++) {
			ArrayList maturity = (ArrayList) maturityResults.get(i);

			for (int j=0; j< maturity.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(j+8);
				cellToWriteOn = rowToWriteOn.getCell(1);
				double value = (Double) maturity.get(j);
				value = (double)Math.round(value * 100) / 100;
				cellToWriteOn.setCellValue(value);
				if (value == 0) cellToWriteOn.setCellValue("Unknown");							

			}
		}


		rowToWriteOn = sheetToWriteOver.getRow(20);
		cellToWriteOn = rowToWriteOn.getCell(10);
		cellToWriteOn.setCellValue((Integer)capabilitiesSupportedResults.get(0));
		cellToWriteOn = rowToWriteOn.getCell(13);
		cellToWriteOn.setCellValue((Integer)capabilitiesSupportedResults.get(1));
		cellToWriteOn = rowToWriteOn.getCell(15);
		cellToWriteOn.setCellValue((Integer)capabilitiesSupportedResults.get(2));

		//User Types
		String userTypes = "";
		rowToWriteOn = sheetToWriteOver.getRow(20);
		cellToWriteOn = rowToWriteOn.getCell(1);
		for (int i=0; i<userTypesResults.size(); i++) {	
			ArrayList row = (ArrayList) userTypesResults.get(i);
			for (int j=0; j< row.size(); j++) {
				if (userTypes.equals(""))
					userTypes = (String) row.get(j);
				else
					userTypes = userTypes + (", " + (String) row.get(j));
			}
		}
		cellToWriteOn.setCellValue(userTypes);

		//User Interfaces		
		for (int i=0; i<userInterfacesResults.size(); i++) {	
			ArrayList row = (ArrayList) userInterfacesResults.get(i);
			for (int j=0; j< row.size(); j++) {
				if ( ( (String) row.get(j)).equals("Web-Based") ) {
					rowToWriteOn = sheetToWriteOver.getRow(20);
					cellToWriteOn = rowToWriteOn.getCell(7);
					cellToWriteOn.setCellValue("X");
				}
				if ( ( (String) row.get(j)).equals("Mobile") ) {
					rowToWriteOn = sheetToWriteOver.getRow(21);
					cellToWriteOn = rowToWriteOn.getCell(7);
					cellToWriteOn.setCellValue("X");
				}
				if ( ( (String) row.get(j)).equals("PC__Application") ) {
					rowToWriteOn = sheetToWriteOver.getRow(22);
					cellToWriteOn = rowToWriteOn.getCell(7);
					cellToWriteOn.setCellValue("X");
				}
				if ( ( (String) row.get(j)).equals("Telephone") ) {
					rowToWriteOn = sheetToWriteOver.getRow(23);
					cellToWriteOn = rowToWriteOn.getCell(7);
					cellToWriteOn.setCellValue("X");
				}
				if ( ( (String) row.get(j)).equals("Other__Device") ) {
					rowToWriteOn = sheetToWriteOver.getRow(24);
					cellToWriteOn = rowToWriteOn.getCell(7);
					cellToWriteOn.setCellValue("X");
				}

			}
		}

		//Top 5 Similar Systems -- System Similarity
		rowToWriteOn = sheetToWriteOver.getRow(33);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(systemName);

		if (sysResults != null) {
			//System Names
			rowToWriteOn = sheetToWriteOver.getRow(32);
			for (int i=0; i<sysResults.size(); i++) {
				if (i==0) cellToWriteOn = rowToWriteOn.getCell(4);
				if (i==1) cellToWriteOn = rowToWriteOn.getCell(6);
				if (i==2) cellToWriteOn = rowToWriteOn.getCell(8);
				if (i==3) cellToWriteOn = rowToWriteOn.getCell(11);
				if (i==4) cellToWriteOn = rowToWriteOn.getCell(13);
				cellToWriteOn.setCellValue((String) sysResults.get(i));
			}
		}

		if (valueResults != null) {
			//values
			rowToWriteOn = sheetToWriteOver.getRow(33);
			for (int i=0; i<valueResults.size(); i++) {
				if (i==0) cellToWriteOn = rowToWriteOn.getCell(4);
				if (i==1) cellToWriteOn = rowToWriteOn.getCell(6);
				if (i==2) cellToWriteOn = rowToWriteOn.getCell(8);
				if (i==3) cellToWriteOn = rowToWriteOn.getCell(11);
				if (i==4) cellToWriteOn = rowToWriteOn.getCell(13);
				double value = (Double) valueResults.get(i);
				cellToWriteOn.setCellValue(value);
			}
		}

		//Application Health Grid
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Images" + System.getProperty("file.separator");
		String picFileName = systemName.replaceAll(":", "")+"_Health_Grid_Export.png";
		String picFileLoc = workingDir + folder + picFileName;
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(picFileLoc); //FileInputStream obtains input bytes from the image file
			byte[] bytes = IOUtils.toByteArray(inputStream); //Get the contents of an InputStream as a byte[].
			int pictureIdx = wb.addPicture(bytes, XSSFWorkbook.PICTURE_TYPE_PNG); //Adds a picture to the workbook

			CreationHelper helper = wb.getCreationHelper(); //Returns an object that handles instantiating concrete classes
			XSSFClientAnchor anchor = (XSSFClientAnchor) helper.createClientAnchor(); //Create an anchor that is attached to the worksheet
			anchor.setCol1(10); //select where to put the picture
			anchor.setRow1(7);
			logger.info(anchor.getAnchorType());

			anchor.setDx1((short)5*36000); 
			anchor.setDy1((short)5*36000); 

			Drawing drawing = sheetToWriteOver.createDrawingPatriarch(); //Creates the top-level drawing patriarch, specify sheet to draw on
			Picture pict = drawing.createPicture(anchor, pictureIdx); //Creates a picture
			pict.resize(); //Reset the image to the original size			
		} catch (FileNotFoundException e) {
			logger.error("Health Grid image not found for this system");
		} catch (IOException e) {
			logger.error("Health Grid image not found for this system");
		} finally {
			try {
				if(inputStream != null)
					inputStream.close();
			} catch (IOException e) {
				logger.error("Error closing input stream for image");
			}
		}
	}

	/**
	 * Write the List of Interfaces Sheet in the workbook
	 * @param wb		XSSFWorkbook containing the List of Interfaces Sheet to populate
	 * @param result	ArrayList containing the interface query results
	 */
	public void writeListOfInterfacesSheet (XSSFWorkbook wb, ArrayList result) {
		XSSFSheet sheetToWriteOver = wb.getSheet("List of Interfaces");
		XSSFRow rowToWriteOn;
		XSSFCellStyle upstreamStyle = wb.createCellStyle();	
		XSSFCellStyle downstreamStyle = wb.createCellStyle();	
		XSSFCellStyle defaultStyle = wb.createCellStyle();
		XSSFCellStyle replaceStyle = wb.createCellStyle();	
		XSSFCellStyle migrateStyle = wb.createCellStyle();
		XSSFFont font = wb.createFont();
		XSSFFont font1 = wb.createFont();
		XSSFFont font2 = wb.createFont();
		XSSFFont font3 = wb.createFont();
		XSSFFont defaultFont = wb.createFont();		
		HashSet uniqueInterfaces = new HashSet();
		HashSet uniqueSOAServices = new HashSet();
		HashSet uniqueDataObjects = new HashSet();
		HashSet uniqueSystemsInterfaced = new HashSet();
		HashSet uniqueDownStreamData = new HashSet();
		HashSet uniqueUpStreamData = new HashSet();
		HashSet uniqueProtocols = new HashSet();		
		HashSet ahltaUpstream = new HashSet(), chcsUpstream = new HashSet(), cisUpstream = new HashSet(), ahltaDownstream = new HashSet(), chcsDownstream = new HashSet(), cisDownstream = new HashSet(), cdrUpstream = new HashSet(), cdrDownstream = new HashSet();

		//Set Colors for Interface Type
		XSSFColor lightBlue = new XSSFColor();
		int r = 0, g = 112, b = 192;
		byte[] lightBlueByte = {(byte)r, (byte)g, (byte)b};
		lightBlue.setRgb(lightBlueByte);
		XSSFColor darkBlue = new XSSFColor();
		int rr = 0, gg = 32, bb = 96;
		byte[] darkBlueByte = {(byte)rr, (byte)gg, (byte)bb};
		darkBlue.setRgb(darkBlueByte);

		writeHeader(wb, sheetToWriteOver);

		//Set Fonts and Styles
		font.setFontHeightInPoints((short)10);
		font.setFontName("Calibri");
		font.setBold(true);
		font.setItalic(false);
		font1.setFontHeightInPoints((short)10);
		font1.setFontName("Calibri");
		font1.setBold(true);
		font1.setItalic(false);
		font2.setFontHeightInPoints((short)10);
		font2.setFontName("Calibri");
		font2.setBold(true);
		font2.setItalic(false);
		font3.setFontHeightInPoints((short)10);
		font3.setFontName("Calibri");
		font3.setBold(true);
		font3.setItalic(false);
		upstreamStyle.setFont(font);
		upstreamStyle.setBorderRight(CellStyle.BORDER_THIN);
		upstreamStyle.setBorderLeft(CellStyle.BORDER_THIN);
		upstreamStyle.setBorderTop(CellStyle.BORDER_THIN);
		upstreamStyle.setBorderBottom(CellStyle.BORDER_THIN);
		upstreamStyle.setAlignment(CellStyle.ALIGN_CENTER);
		upstreamStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		downstreamStyle.setFont(font);
		downstreamStyle.setBorderRight(CellStyle.BORDER_THIN);
		downstreamStyle.setBorderLeft(CellStyle.BORDER_THIN);
		downstreamStyle.setBorderTop(CellStyle.BORDER_THIN);
		downstreamStyle.setBorderBottom(CellStyle.BORDER_THIN);
		downstreamStyle.setAlignment(CellStyle.ALIGN_CENTER);
		downstreamStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		migrateStyle.setFont(font);
		migrateStyle.setBorderRight(CellStyle.BORDER_THIN);
		migrateStyle.setBorderLeft(CellStyle.BORDER_THIN);
		migrateStyle.setBorderTop(CellStyle.BORDER_THIN);
		migrateStyle.setBorderBottom(CellStyle.BORDER_THIN);
		migrateStyle.setAlignment(CellStyle.ALIGN_CENTER);
		migrateStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		replaceStyle.setFont(font);
		replaceStyle.setBorderRight(CellStyle.BORDER_THIN);
		replaceStyle.setBorderLeft(CellStyle.BORDER_THIN);
		replaceStyle.setBorderTop(CellStyle.BORDER_THIN);
		replaceStyle.setBorderBottom(CellStyle.BORDER_THIN);
		replaceStyle.setAlignment(CellStyle.ALIGN_CENTER);
		replaceStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		defaultStyle.setFont(defaultFont);
		defaultStyle.setBorderRight(CellStyle.BORDER_THIN);

		for (int i=0; i<result.size(); i++) {
			ArrayList row = (ArrayList) result.get(i);
			if(sheetToWriteOver.getLastRowNum()>=i+10)
				rowToWriteOn = sheetToWriteOver.getRow(i+10);
			else
			{
				rowToWriteOn = sheetToWriteOver.createRow(i+10);
				rowToWriteOn.setRowStyle(sheetToWriteOver.getRow(i+9).getRowStyle());
			}



			for (int j=0; j<row.size(); j++) {
				XSSFCell cellToWriteOn;
				if(rowToWriteOn.getLastCellNum()>=j+1&&sheetToWriteOver.getLastRowNum()<320)
					cellToWriteOn = rowToWriteOn.getCell(j+1);
				else if(j<=6)
				{
					//					if(j==0)
					//					{
					//						cellToWriteOn = rowToWriteOn.createCell(0);
					//						cellToWriteOn.setCellStyle(sheetToWriteOver.getRow(i+9).getCell(0).getCellStyle());
					//					}
					cellToWriteOn = rowToWriteOn.createCell(j+1);
					cellToWriteOn.setCellStyle(sheetToWriteOver.getRow(i+9).getCell(j+1).getCellStyle());
				}
				else
				{
					cellToWriteOn = rowToWriteOn.getCell(6);
				}
				if (j==0) {
					String interfaceType = ((String) row.get(j) ).replaceAll("\"", "");
					cellToWriteOn.setCellValue(interfaceType);
					if (row.get(2).toString().equals(systemName)&&row.get(3).toString().equals(systemName)) {
						cellToWriteOn = rowToWriteOn.getCell(1);
						downstreamStyle.setFont(font1);
						cellToWriteOn.setCellStyle(downstreamStyle);
						cellToWriteOn.setCellValue("Internal");
					}
				}
				if (j==1)
					uniqueInterfaces.add(row.get(j));
				if (j==2) {
					if (!row.get(j).toString().equals(systemName))
						uniqueSystemsInterfaced.add(row.get(j));
					if (row.get(j).toString().equals("AHLTA"))
						ahltaUpstream.add(row.get(1)); 
					if (row.get(j).toString().equals("CHCS"))
						chcsUpstream.add(row.get(1));
					if (row.get(j).toString().equals("CIS-Essentris"))
						cisUpstream.add(row.get(1));
					if (row.get(j).toString().equals("CDR"))
						cdrUpstream.add(row.get(1));
					if (row.get(j).toString().equals(systemName)&&!row.get(j+1).toString().equals(systemName)) {
						cellToWriteOn = rowToWriteOn.getCell(1);
						font1.setColor(lightBlue);
						downstreamStyle.setFont(font1);
						cellToWriteOn.setCellStyle(downstreamStyle);
					}
				}					
				if (j==3) {
					if (!row.get(j).toString().equals(systemName))
						uniqueSystemsInterfaced.add(row.get(j));
					if (row.get(j).toString().equals("AHLTA"))
						ahltaDownstream.add(row.get(1)); 
					if (row.get(j).toString().equals("CHCS"))
						chcsDownstream.add(row.get(1));
					if (row.get(j).toString().equals("CIS-Essentris"))
						cisDownstream.add(row.get(1));
					if (row.get(j).toString().equals("CDR"))
						cdrDownstream.add(row.get(1));
					if (row.get(j).toString().equals(systemName)&&!row.get(j-1).toString().equals(systemName)) {
						cellToWriteOn = rowToWriteOn.getCell(1);
						font.setColor(darkBlue);
						upstreamStyle.setFont(font);
						cellToWriteOn.setCellStyle(upstreamStyle);
					}
				}
				if (j==4) {
					uniqueDataObjects.add(row.get(j));
					if (row.get(2).toString().equals(systemName))
						uniqueDownStreamData.add(row.get(j));
					if (row.get(3).toString().equals(systemName))
						uniqueUpStreamData.add(row.get(j));
					cellToWriteOn = rowToWriteOn.getCell(j+1);
					String value = ((String) row.get(j) ).replaceAll("\"", "");
					cellToWriteOn.setCellValue(value.replaceAll("_", " ")  );
				}
				if ((j==6) && ( !((String)row.get(j)).equals("")))
					uniqueSOAServices.add(row.get(j));
				if (j==7)
					uniqueProtocols.add(row.get(j));
				if (j==1 || j==2 || j==3 || j==4 || j==6) {

					cellToWriteOn = rowToWriteOn.getCell(j+1);
					String value = ((String) row.get(j) ).replaceAll("\"", "");
					cellToWriteOn.setCellValue(value.replaceAll("_", " ")  );
					if (value.equals("")) cellToWriteOn.setCellValue("Unknown");
				}
				if (j==8) {
					String upStreamFurther = ((String)row.get(j)).replaceAll("\"", "");
					cellToWriteOn = rowToWriteOn.getCell(6);
					if (upStreamFurther.equals("none")) {
						font3.setColor(darkBlue);
						migrateStyle.setFont(font3);
						cellToWriteOn.setCellValue("Migrate");
						cellToWriteOn.setCellStyle(migrateStyle);
					}
					if (upStreamFurther.equals("exists")) {
						font2.setColor(lightBlue);
						replaceStyle.setFont(font2);
						cellToWriteOn.setCellValue("Replace");
						cellToWriteOn.setCellStyle(replaceStyle);
					}
				}
			}			
		}

		//Write Interface Overview Sheet - Number of Interfaces & SOA Services
		sheetToWriteOver = wb.getSheet("System Overview");
		rowToWriteOn = sheetToWriteOver.getRow(37);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(uniqueInterfaces.size());
		cellToWriteOn = rowToWriteOn.getCell(3);
		cellToWriteOn.setCellValue(uniqueSystemsInterfaced.size());
		cellToWriteOn = rowToWriteOn.getCell(5);
		cellToWriteOn.setCellValue(uniqueDataObjects.size());
		cellToWriteOn = rowToWriteOn.getCell(7);
		cellToWriteOn.setCellValue(uniqueSOAServices.size());
		cellToWriteOn = rowToWriteOn.getCell(9);
		cellToWriteOn.setCellValue(uniqueProtocols.size());

		//Write DHMSM Interfaces section	
		cellToWriteOn = rowToWriteOn.getCell(14);
		cellToWriteOn.setCellValue(ahltaUpstream.size());
		cellToWriteOn = rowToWriteOn.getCell(15);
		cellToWriteOn.setCellValue(chcsUpstream.size());
		cellToWriteOn = rowToWriteOn.getCell(16);
		cellToWriteOn.setCellValue(cisUpstream.size());
		cellToWriteOn = rowToWriteOn.getCell(17);
		cellToWriteOn.setCellValue(cdrUpstream.size());

		rowToWriteOn = sheetToWriteOver.getRow(38);
		cellToWriteOn = rowToWriteOn.getCell(14);
		cellToWriteOn.setCellValue(ahltaDownstream.size());
		cellToWriteOn = rowToWriteOn.getCell(15);
		cellToWriteOn.setCellValue(chcsDownstream.size());
		cellToWriteOn = rowToWriteOn.getCell(16);
		cellToWriteOn.setCellValue(cisDownstream.size());
		cellToWriteOn = rowToWriteOn.getCell(17);
		cellToWriteOn.setCellValue(cdrDownstream.size());	
	}	

	/**
	 * Writes the hidden Feeder tab to populate the pie charts in the System Overview Sheet
	 * @param wb		XSSFWorkbook containing the Feeder Sheet to populate
	 * @param swList	ArrayList containing the software life cycle results
	 * @param hwList	ArrayList containing the hardware life cycle results
	 */
	public void writeFeederSheet(XSSFWorkbook wb, ArrayList swList, ArrayList hwList) {
		XSSFSheet sheetToWriteOver = wb.getSheet("Feeder");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(1);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
		int TBDSWCount = 0, retiredSWCount = 0, sunsetSWCount = 0, supportedSWCount = 0, GASWCount = 0;
		int TBDHWCount = 0, retiredHWCount = 0, sunsetHWCount = 0, supportedHWCount = 0, GAHWCount = 0;

		for (int i = 0; i < swList.size(); i++) {
			String swLifeCycle = (String) swList.get(i);
			if (swLifeCycle.equals("TBD")) TBDSWCount++;
			if (swLifeCycle.equals("Retired_(Not_Supported)")) retiredSWCount++;
			if (swLifeCycle.equals("Sunset_(End_of_Life)")) sunsetSWCount++;
			if (swLifeCycle.equals("Supported")) supportedSWCount++;
			if (swLifeCycle.equals("GA_(Generally_Available)")) GASWCount++;				
		}
		for (int j = 0; j < hwList.size(); j++) {
			String hwLifeCycle = (String) hwList.get(j);
			if (hwLifeCycle.equals("TBD")) TBDHWCount++;
			if (hwLifeCycle.equals("Retired_(Not_Supported)")) retiredHWCount++;
			if (hwLifeCycle.equals("Sunset_(End_of_Life)")) sunsetHWCount++;
			if (hwLifeCycle.equals("Supported")) supportedHWCount++;
			if (hwLifeCycle.equals("GA_(Generally_Available)")) GAHWCount++;	
		}

		for (int k = 0; k < 5; k++) {
			rowToWriteOn = sheetToWriteOver.getRow(1);
			cellToWriteOn = rowToWriteOn.getCell(2);
			if (k==0) {
				cellToWriteOn.setCellValue(supportedHWCount);
				cellToWriteOn = rowToWriteOn.getCell(7);
				cellToWriteOn.setCellValue(supportedSWCount);
			}
			if (k==1) {
				rowToWriteOn = sheetToWriteOver.getRow(k+1);
				cellToWriteOn = rowToWriteOn.getCell(2);
				cellToWriteOn.setCellValue(retiredHWCount);
				cellToWriteOn = rowToWriteOn.getCell(8);
				cellToWriteOn.setCellValue(retiredSWCount);
			}
			if (k==2) {				
				rowToWriteOn = sheetToWriteOver.getRow(k+1);
				cellToWriteOn = rowToWriteOn.getCell(2);
				cellToWriteOn.setCellValue(TBDHWCount);
				cellToWriteOn = rowToWriteOn.getCell(8);
				cellToWriteOn.setCellValue(TBDSWCount);
			}
			if (k==3) {				
				rowToWriteOn = sheetToWriteOver.getRow(k+1);
				cellToWriteOn = rowToWriteOn.getCell(2);
				cellToWriteOn.setCellValue(sunsetHWCount);
				cellToWriteOn = rowToWriteOn.getCell(8);
				cellToWriteOn.setCellValue(sunsetSWCount);
			}
			if (k==4) {
				rowToWriteOn = sheetToWriteOver.getRow(k+1);
				cellToWriteOn = rowToWriteOn.getCell(2);
				cellToWriteOn.setCellValue(GAHWCount);
				cellToWriteOn = rowToWriteOn.getCell(8);
				cellToWriteOn.setCellValue(GASWCount);
			}
		}		
	}

	/**
	 * Writes the Data Provenance Sheet in the workbook
	 * @param wb 					XSSFWorkbook containing the Data Provenance Sheet to populate
	 * @param result 				ArrayList containing the data provenance results
	 * @param uniqueDataSystems		ArrayList containing the systems that create/read/modify unique data objects
	 * @param uniqueData			ArrayList containing the unique created/read/modified data objects
	 */
	public void writeDataProvenanceSheet (XSSFWorkbook wb, ArrayList result, ArrayList uniqueDataSystems, ArrayList uniqueData)	{
		XSSFSheet sheetToWriteOver = wb.getSheet("Data Provenance");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);		
		XSSFCellStyle boldStyle = wb.createCellStyle();		
		XSSFCellStyle defaultStyle = wb.createCellStyle();
		XSSFFont font = wb.createFont();
		XSSFFont defaultFont = wb.createFont();
		int countC = 0, countR = 0, countM = 0, indexC = 0, indexR = 0, indexM = 0;

		writeHeader(wb, sheetToWriteOver);

		font.setFontHeightInPoints((short)10);
		font.setFontName("Calibri");
		font.setBold(true);
		font.setItalic(false);

		defaultFont.setFontHeightInPoints((short)10);
		defaultFont.setFontName("Calibri");
		defaultFont.setBold(false);
		defaultFont.setItalic(false);

		boldStyle.setFont(font);
		boldStyle.setBorderRight(CellStyle.BORDER_THIN);
		defaultStyle.setFont(defaultFont);
		defaultStyle.setBorderRight(CellStyle.BORDER_THIN);

		font.setFontHeightInPoints((short)10);
		font.setFontName("Calibri");
		font.setBold(true);
		font.setItalic(false);

		cellToWriteOn.setCellValue(systemName);
		rowToWriteOn = sheetToWriteOver.getRow(5);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(systemName);	

		//Writes out List of Data Objects sorted by Create/Read/Modify
		for (int i=0; i<result.size(); i++) {
			ArrayList row = (ArrayList) result.get(i);
			if(sheetToWriteOver.getLastRowNum()>=i+6)
				rowToWriteOn = sheetToWriteOver.getRow(i+6);
			else
			{
				rowToWriteOn = sheetToWriteOver.createRow(i+6);
				rowToWriteOn.setRowStyle(sheetToWriteOver.getRow(i+5).getRowStyle());
			}
			String crm = (String) row.get(0);

			if (crm.contains("C")) { countC++; indexC++; }					
			if (crm.contains("M")) { countM++; indexM++; }
			if (crm.contains("R")) { countR++; indexR++; }
			if(indexC==1 || indexM == 1 ||indexR == 1)
			{
				if(rowToWriteOn.getLastCellNum()>=1)
					cellToWriteOn = rowToWriteOn.getCell(1);
				else
				{
					cellToWriteOn = rowToWriteOn.createCell(1);
					cellToWriteOn.setCellStyle(sheetToWriteOver.getRow(i+5).getCell(1).getCellStyle());
					sheetToWriteOver.getRow(i+5).getCell(1).getCellStyle().setBorderBottom(CellStyle.BORDER_NONE);
				}
			}
			if (indexC==1) {cellToWriteOn = rowToWriteOn.getCell(1);
			cellToWriteOn.setCellValue("Create"); indexC++; }
			if (indexM==1) {cellToWriteOn = rowToWriteOn.getCell(1);
			cellToWriteOn.setCellValue("Modify"); indexM++; }
			if (indexR==1) {cellToWriteOn = rowToWriteOn.getCell(1);
			cellToWriteOn.setCellValue("Read"); indexR++; }
			if(rowToWriteOn.getLastCellNum()>=2&&((indexC!=2 && indexM != 2 && indexR != 2)))
				cellToWriteOn = rowToWriteOn.getCell(2);
			else
			{

				cellToWriteOn = rowToWriteOn.createCell(2);
				cellToWriteOn.setCellStyle(sheetToWriteOver.getRow(i+5).getCell(2).getCellStyle());
				sheetToWriteOver.getRow(i+5).getCell(2).getCellStyle().setBorderBottom(CellStyle.BORDER_NONE);
				if(indexC!=2 && indexM != 2 && indexR != 2)
				{
					XSSFCell cellOne = rowToWriteOn.createCell(1);
					cellOne.setCellStyle(sheetToWriteOver.getRow(i+5).getCell(1).getCellStyle());
					sheetToWriteOver.getRow(i+5).getCell(1).getCellStyle().setBorderBottom(CellStyle.BORDER_NONE);
				}

			}
			int val = ((Double) row.get(2)).intValue();
			String value = "-  " + (String) row.get(1) + " (" + val + ")";
			cellToWriteOn.setCellValue(value.replaceAll("_"," ")); 

			//Bold Uniquely Created Data Objects
			if (uniqueDataSystems.contains(systemName)) {
				int index = uniqueDataSystems.indexOf(systemName);
				String uniquedata = ((String)uniqueData.get(index)).replaceAll("_", " ");
				if(cellToWriteOn.getStringCellValue().contains(uniquedata))						
					cellToWriteOn.setCellStyle(boldStyle);
				else cellToWriteOn.setCellStyle(defaultStyle);
			}

		}		

		//Puts count of C/R/M into Feeder tab
		sheetToWriteOver = wb.getSheet("Feeder");
		rowToWriteOn = sheetToWriteOver.getRow(7);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(systemName);
		rowToWriteOn = sheetToWriteOver.getRow(8);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(countC);
		rowToWriteOn = sheetToWriteOver.getRow(9);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(countR);
		rowToWriteOn = sheetToWriteOver.getRow(10);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(countM);		
	}

	/**
	 * Writes the Business Logic Sheet in the workbook
	 * @param wb					XSSFWorkbook containing the Business Logic Sheet to populate
	 * @param result				ArrayList containing the business logic results
	 * @param uniqueBLUSystems		ArrayList containing the list of systems that create unique business logic units
	 * @param uniqueBLU				ArrayList containing the list of unique created business logic units
	 */
	public void writeBusinessLogicSheet (XSSFWorkbook wb, ArrayList result, ArrayList uniqueBLUSystems, ArrayList uniqueBLU) {
		XSSFSheet sheetToWriteOver = wb.getSheet("Business Logic");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
		XSSFCellStyle boldStyle = wb.createCellStyle();		
		XSSFCellStyle defaultStyle = wb.createCellStyle();
		XSSFFont font = wb.createFont();
		XSSFFont defaultFont = wb.createFont();

		writeHeader(wb, sheetToWriteOver);

		font.setFontHeightInPoints((short)10);
		font.setFontName("Calibri");
		font.setBold(true);
		font.setItalic(false);

		defaultFont.setFontHeightInPoints((short)10);
		defaultFont.setFontName("Calibri");
		defaultFont.setBold(false);
		defaultFont.setItalic(false);

		boldStyle.setFont(font);
		boldStyle.setBorderRight(CellStyle.BORDER_THIN);
		defaultStyle.setFont(defaultFont);
		defaultStyle.setBorderRight(CellStyle.BORDER_THIN);

		cellToWriteOn.setCellValue(systemName);
		rowToWriteOn = sheetToWriteOver.getRow(5);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(systemName);

		for (int i = 0; i<result.size(); i++) {
			ArrayList row = (ArrayList) result.get(i);
			if(sheetToWriteOver.getLastRowNum()>=i+6)
				rowToWriteOn = sheetToWriteOver.getRow(i+6);
			else
			{
				rowToWriteOn = sheetToWriteOver.createRow(i+6);
				rowToWriteOn.setRowStyle(sheetToWriteOver.getRow(i+5).getRowStyle());
			}
			for (int j=0; j<row.size(); j++) {
				XSSFCell cellToWrite;
				if(rowToWriteOn.getLastCellNum()>=2)
					cellToWrite = rowToWriteOn.getCell(2);
				else
				{
					cellToWrite = rowToWriteOn.createCell(2);
					cellToWrite.setCellStyle(sheetToWriteOver.getRow(i+5).getCell(2).getCellStyle());
					sheetToWriteOver.getRow(i+5).getCell(2).getCellStyle().setBorderBottom(CellStyle.BORDER_NONE);
					XSSFCell cellOne = rowToWriteOn.createCell(1);
					cellOne.setCellStyle(sheetToWriteOver.getRow(i+5).getCell(1).getCellStyle());
					sheetToWriteOver.getRow(i+5).getCell(1).getCellStyle().setBorderBottom(CellStyle.BORDER_NONE);

				}
				String blu = (String) row.get(j);
				String value = "-  " + blu;
				cellToWrite.setCellValue(value.replaceAll("_", " "));

				//Bold Uniquely Created BLU Objects
				if (uniqueBLUSystems.contains(systemName)) {
					int index = uniqueBLUSystems.indexOf(systemName);
					String uniqueblu = ((String)uniqueBLU.get(index)).replaceAll("_", " ");					
					if(cellToWrite.getStringCellValue().contains(uniqueblu))						
						cellToWrite.setCellStyle(boldStyle);
					else cellToWrite.setCellStyle(defaultStyle);
				}
			}
		}
	}

	/**
	 * Writes the data from the system similarity and system similarity queries
	 * @param wb 				XSSFWorkbook containing the System Similarity Sheet to populate
	 * @param sysSimResults	ArrayList containing the system similarity results
	 * @param sysResults		ArrayList containing the similar system results
	 */
	public void writeSystemSimilaritySheet (XSSFWorkbook wb, ArrayList sysSimResults, ArrayList sysResults){
		XSSFSheet sheetToWriteOver = wb.getSheet("System Similarity");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(systemName);
		rowToWriteOn = sheetToWriteOver.getRow(6);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(systemName);

		writeHeader(wb, sheetToWriteOver);

		for (int i=0; i<sysResults.size(); i++) {
			cellToWriteOn = rowToWriteOn.getCell(i+3);
			cellToWriteOn.setCellValue((String)sysResults.get(i));
		}

		for (int i=0; i<sysSimResults.size(); i++) {
			ArrayList row = (ArrayList) sysSimResults.get(i);

			for (int j=0; j<row.size(); j++) {	
				rowToWriteOn = sheetToWriteOver.getRow(j+7);
				cellToWriteOn = rowToWriteOn.getCell(i+3);
				if ( (row.get(j)) instanceof String ) 
					cellToWriteOn.setCellValue((String) row.get(j)); 
				else 
					cellToWriteOn.setCellValue((Double) row.get(j));
			}			
		}

	}	

	/**
	 * Locates the map export picture files for a given system and adds them to the Deployment Strategy Sheet
	 * @param wb	XSSFWorkbook containing the Deployment Strategy Sheet to populate
	 */
	public void writeDeploymentStrategySheet (XSSFWorkbook wb){
		XSSFSheet sheetToWriteOver = wb.getSheet("Deployment Strategy");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);	
		XSSFCellStyle redStyle = wb.createCellStyle();
		XSSFFont font = wb.createFont();
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = "\\export\\Images\\";
		String picFileName = systemName.replaceAll(":", "")+"_CONUS_Map_Export.png";
		String picFileLoc = workingDir + folder + picFileName;
		String picFileName2 = systemName.replaceAll(":", "")+"_OCONUS_Map_Export.png";
		String picFileLoc2 = workingDir + folder + picFileName2;
		String templateFolder = "\\pictures\\";
		String templateCONUSFileLoc = workingDir + templateFolder + "templateCONUS.png";
		String templateOCONUSFileLoc = workingDir + templateFolder + "templateOCONUS.png";

		writeHeader(wb, sheetToWriteOver);
		cellToWriteOn.setCellValue(systemName);

		XSSFColor red = new XSSFColor();
		int r = 255, g = 0, b = 0;
		byte[] redByte = {(byte)r, (byte)g, (byte)b};
		red.setRgb(redByte);

		XSSFColor white = new XSSFColor();
		int rr = 255, gg = 255, bb = 255;
		byte[] whiteByte = {(byte)rr, (byte)gg, (byte)bb};
		white.setRgb(whiteByte);

		font.setFontHeightInPoints((short)10);
		font.setFontName("Calibri");
		font.setBold(true);
		font.setItalic(false);
		font.setColor(white);

		redStyle.setFont(font);
		redStyle.setAlignment(CellStyle.ALIGN_CENTER);
		redStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		redStyle.setFillForegroundColor(red);
		redStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);

		int pictureIdx = 0;
		int pictureIdx2 = 0;

		FileInputStream inputStream1 = null;
		FileInputStream inputStream2 = null;
		try {
			inputStream1 = new FileInputStream(picFileLoc); //FileInputStream obtains input bytes from the image file
			byte[] bytes = IOUtils.toByteArray(inputStream1); //Get the contents of an InputStream as a byte[].
			pictureIdx = wb.addPicture(bytes, XSSFWorkbook.PICTURE_TYPE_PNG); //Adds a picture to the workbook
			inputStream1.close();

			inputStream2 = new FileInputStream(picFileLoc2); //FileInputStream obtains input bytes from the image file
			bytes = IOUtils.toByteArray(inputStream2); //Get the contents of an InputStream as a byte[].
			pictureIdx2 = wb.addPicture(bytes, XSSFWorkbook.PICTURE_TYPE_PNG); //Adds a picture to the workbook
		} catch (FileNotFoundException e) {
			try {
				inputStream1 = new FileInputStream(templateCONUSFileLoc);
				byte[] bytes = IOUtils.toByteArray(inputStream1); //Get the contents of an InputStream as a byte[].
				pictureIdx = wb.addPicture(bytes, XSSFWorkbook.PICTURE_TYPE_PNG); //Adds a picture to the workbook

				inputStream2 = new FileInputStream(templateOCONUSFileLoc); //FileInputStream obtains input bytes from the image file
				bytes = IOUtils.toByteArray(inputStream2); //Get the contents of an InputStream as a byte[].
				pictureIdx2 = wb.addPicture(bytes, XSSFWorkbook.PICTURE_TYPE_PNG); //Adds a picture to the workbook

				rowToWriteOn = sheetToWriteOver.getRow(6);
				cellToWriteOn = rowToWriteOn.getCell(1);
				cellToWriteOn.setCellStyle(redStyle);

			} catch (FileNotFoundException e1) {
				logger.error("CONUS and OCONUS map images not found for this system");
			} 
			catch (IOException e1) {
				logger.error("CONUS and OCONUS map images not able to be loaded");
			}
		} catch (IOException e) {
			logger.error("Error closing input stream for image");
		} finally {
			try {
				if(inputStream1!=null)
					inputStream1.close();
			} catch (IOException e2) {
				logger.error("Error closing input stream for image");
			}
			try {
				if(inputStream2!=null)
					inputStream2.close();
			} catch (IOException e2) {
				logger.error("Error closing input stream for image");
			}
		}

		CreationHelper helper = wb.getCreationHelper(); //Returns an object that handles instantiating concrete classes
		ClientAnchor anchor = helper.createClientAnchor(); //Create an anchor that is attached to the worksheet
		anchor.setCol1(1); //select where to put the picture
		anchor.setRow1(9);

		Drawing drawing = sheetToWriteOver.createDrawingPatriarch(); //Creates the top-level drawing patriarch, specify sheet to draw on
		Picture pict = drawing.createPicture(anchor, pictureIdx); //Creates a picture
		pict.resize(); //Reset the image to the original size

		CreationHelper helper2 = wb.getCreationHelper(); //Returns an object that handles instantiating concrete classes
		ClientAnchor anchor2 = helper2.createClientAnchor(); //Create an anchor that is attached to the worksheet
		anchor2.setCol1(1); //select where to put the picture
		anchor2.setRow1(35);
		Picture pict2 = drawing.createPicture(anchor2, pictureIdx2); //Creates a picture
		pict2.resize(); //Reset the image to the original size

	}

	/**
	 * Writes the data from the site list query to the Site List Sheet
	 * @param wb		XSSFWorkbook containing the Site List Sheet to populate
	 * @param result	ArrayList containing the output of the site list query
	 */
	public void writeSiteListSheet (XSSFWorkbook wb, ArrayList result){
		XSSFSheet sheetToWriteOver = wb.getSheet("Site List");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);

		writeHeader(wb, sheetToWriteOver);
		cellToWriteOn.setCellValue(systemName);

		for (int i=0; i<result.size(); i++) {
			ArrayList row = (ArrayList) result.get(i);
			rowToWriteOn = sheetToWriteOver.getRow(i+7);

			for (int j=0; j<row.size(); j++) {
				cellToWriteOn = rowToWriteOn.getCell(j+1);
				if (!(((String) row.get(j) ).replaceAll("\"", "")).equals("NULL") && row.get(j) != null) {	
					cellToWriteOn.setCellValue( (((String) row.get(j) ).replaceAll("\"", "")).replaceAll("_", " ") );
				}
			}
		}
	}

	/**
	 * Writes the data from the budget query to the Budget Sheet
	 * @param wb		XSSFWorkbook containing the Budget Sheet to populate
	 * @param result	ArrayList containing the output of the budget query
	 */
	public void writeBudgetSheet (XSSFWorkbook wb, ArrayList result){
		XSSFSheet sheetToWriteOver = wb.getSheet("Budget");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);

		writeHeader(wb, sheetToWriteOver);
		cellToWriteOn.setCellValue(systemName);

		for (int i=0; i<result.size(); i++) {
			ArrayList row = (ArrayList) result.get(i);

			if ( ((String) row.get(0)).equals("OP_Total") ) {					
				rowToWriteOn = sheetToWriteOver.getRow(6);
				for (int j=1; j<row.size(); j++) {
					cellToWriteOn = rowToWriteOn.getCell(j+1);
					cellToWriteOn.setCellValue( (Double) row.get(j));
				}
			}
			if ( ((String) row.get(0)).equals("O&M_Total") ) {
				rowToWriteOn = sheetToWriteOver.getRow(7);
				for (int j=1; j<row.size(); j++) {
					cellToWriteOn = rowToWriteOn.getCell(j+1);
					cellToWriteOn.setCellValue( (Double) row.get(j));
				}
			}
			if ( ((String) row.get(0)).equals("RDT&E_Total") ) {
				rowToWriteOn = sheetToWriteOver.getRow(8);
				for (int j=1; j<row.size(); j++) {
					cellToWriteOn = rowToWriteOn.getCell(j+1);
					cellToWriteOn.setCellValue( (Double) row.get(j));
				}
			}
		}
	}

	/**
	 * Creates the headers for the workbook for each system fact sheet 
	 * @param wb 		XSSFWorkbook to add the headers
	 * @param sheet 	XSSFSheet within the workbook to add the header
	 */
	public void writeHeader(XSSFWorkbook wb, XSSFSheet sheet){
		// &B is to bold the text
		// &18 is the size of the text
		XSSFHeaderFooter header = (XSSFHeaderFooter) sheet.getHeader();
		header.setCenter("&B &11 System Fact Sheets");
		header.setLeft("&B &11" + systemName);
		header.setRight("&B &11" + "Page " + (1+wb.getSheetIndex(sheet)));
	}
}
