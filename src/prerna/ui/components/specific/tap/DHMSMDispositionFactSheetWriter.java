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
package prerna.ui.components.specific.tap;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.extensions.XSSFHeaderFooter;

import prerna.error.EngineException;
import prerna.util.ConstantsTAP;
import prerna.util.Utility;

public class DHMSMDispositionFactSheetWriter {
	
	private static final Logger logger = LogManager.getLogger(DHMSMDispositionFactSheetWriter.class.getName());

	public String systemName;
	
	private XSSFWorkbook wb;
	public Hashtable<String,XSSFCellStyle> myStyles;
	private final String[] dispositionDescriptions = new String[]{
			"has a low probability of being replaced by the new EHR and will need a direct interface to the new system.",
			"has a low probability of being replaced by the new EHR and will NOT need a direct interface to the new system. "
			+ "These systems, along with the 'High' systems, may be candidates for de-duplication and portfolio rationalization.",
			"has a high probability of being replaced by the new EHR."};
	
	/**
	 * Creates the new workbook
	 * @param systemName		String containing the name of the system to create the fact sheet
	 * @param templateFileLoc	String containing the location of the fact sheet template
	 */
	public void createWorkbook(String systemName, String templateFileLoc) {
		//XSSFWorkbook wb;
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
	}

	/**
	 * Retrieves the query results for a given system from the fact sheet processor and writes the OTM disposition fact sheet report
	 * @param fileLoc			String containing the file location of the fact sheet
	 * @param systemDataHash	Hashtable containing all the query results from the fact sheet processor for a given system
	 */
	public void exportFactSheets(String fileLoc, Hashtable systemDataHash, int devICDCount, int decomICDCount, int sustainICDCount) {
		//create an Arraylist of results for each query - retrieve results from Hashtable
		ArrayList siteResults = (ArrayList) systemDataHash.get(ConstantsTAP.SITE_LIST_QUERY);
		ArrayList budgetResults = (ArrayList) systemDataHash.get(ConstantsTAP.BUDGET_QUERY);
		ArrayList pocResults = (ArrayList) systemDataHash.get(ConstantsTAP.POC_QUERY);
		ArrayList valueResults = (ArrayList) systemDataHash.get(ConstantsTAP.VALUE_QUERY);		
		ArrayList systemDescriptionResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_DESCRIPTION_QUERY);
		ArrayList systemHighlightsResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_HIGHLIGHTS_QUERY);
		ArrayList ppiResults = (ArrayList) systemDataHash.get(ConstantsTAP.PPI_QUERY);
		ArrayList icdResults = (ArrayList) systemDataHash.get(ConstantsTAP.LIST_OF_INTERFACES_QUERY);
		//ArrayList capabilitiesSupportedResults = (ArrayList) systemDataHash.get(ConstantsTAP.CAPABILITIES_SUPPORTED_QUERY);	
		ArrayList lpiSystemResults = (ArrayList) systemDataHash.get(ConstantsTAP.LPI_SYSTEMS_QUERY);	
		ArrayList lpniSystemResults = (ArrayList) systemDataHash.get(ConstantsTAP.LPNI_SYSTEMS_QUERY);	
		ArrayList highSystemResults = (ArrayList) systemDataHash.get(ConstantsTAP.HIGH_SYSTEMS_QUERY);	
		ArrayList referenceRepositoryResults = (ArrayList) systemDataHash.get(ConstantsTAP.REFERENCE_REPOSITORY_QUERY);
		ArrayList rtmResults = (ArrayList) systemDataHash.get(ConstantsTAP.RTM_QUERY);
		double percentDataCreatedDHMSMProvide = (Double) systemDataHash.get(ConstantsTAP.DHMSM_DATA_PROVIDED_PERCENT);
		double percentBLUCreatedDHMSMProvide = (Double) systemDataHash.get(ConstantsTAP.DHMSM_BLU_PROVIDED_PERCENT);
		
		writeSystemOverviewSheet(wb, icdResults, systemDescriptionResults, 
				systemHighlightsResults, pocResults, ppiResults, devICDCount, decomICDCount, sustainICDCount, 
				lpiSystemResults, lpniSystemResults, highSystemResults, referenceRepositoryResults, rtmResults, percentDataCreatedDHMSMProvide, percentBLUCreatedDHMSMProvide);
		writeSiteListSheet(wb, siteResults);
		writeBudgetSheet(wb, budgetResults);
		//writeFeederSheet(wb, capabilitiesSupportedResults);
		wb.setForceFormulaRecalculation(true);
		hideWorkSheet("Feeder");
		Utility.writeWorkbook(wb, fileLoc);
	}
	
	/**
	 * Writes the System Overview Sheet in the workbook
	 * @param wb							XSSFWorkbook containing the System Overview Sheet to populate
	 * @param systemDescriptionResults		ArrayList containing the system description
	 * @param systemHighlightsResults		ArrayList containing the system highlights
	 * @param pocResults					ArrayList containing the system point of contact
	 * @param capabilitiesSupportedResults	ArrayList containing the capabilities supported results
	 * @param ppiResults					ArrayList containing the system owner
	 */
	public void writeSystemOverviewSheet(XSSFWorkbook wb, ArrayList icdResults, ArrayList systemDescriptionResults, ArrayList systemHighlightsResults, ArrayList pocResults, 
			ArrayList ppiResults, int devICDCount, int decomICDCount, int sustainICDCount, 
			ArrayList lpiSystemsList, ArrayList lpniSystemsList, ArrayList highSystemsList, ArrayList referenceRepositoryList, ArrayList rtmResults, 
			double percentDataCreatedDHMSMProvide, double percentBLUCreatedDHMSMProvide) {
		XSSFSheet sheetToWriteOver = wb.getSheet("System Overview");
		writeHeader(wb, sheetToWriteOver);
		
		//System Name
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(2);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(3);		
		cellToWriteOn.setCellValue(systemName);
		//System Disposition		
		fillStringInText(sheetToWriteOver, 3, 3,"@SYSTEM@",systemName);		
		String disposition = "", dispositionDescription = "";
		ArrayList<String> systemNameArrayList = new ArrayList<String>();
		systemNameArrayList.add(systemName);
		
		if (lpiSystemsList.contains(systemNameArrayList)) {
			disposition = "LPI";
			dispositionDescription = dispositionDescriptions[0];
		} else if (lpniSystemsList.contains(systemNameArrayList)) {
			disposition = "LPNI";
			dispositionDescription = dispositionDescriptions[1];
		} else if (highSystemsList.contains(systemNameArrayList)) {
			disposition = "High";
			dispositionDescription = dispositionDescriptions[2];
		}		
		fillStringInText(sheetToWriteOver, 3, 3,"@DISPOSITION@", disposition);
		fillStringInText(sheetToWriteOver, 3, 3,"@DESCRIPTION@", dispositionDescription);	
		
		//System Description
		for (int i = 0; i < systemDescriptionResults.size(); i++) {	
			ArrayList description = (ArrayList) systemDescriptionResults.get(i);
			for(int j = 0; j < description.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(4);
				cellToWriteOn = rowToWriteOn.getCell(1);
				String value = (String) description.get(j);
				cellToWriteOn.setCellValue((value.replaceAll("_", " ")).replaceAll("\"", ""));
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
		rowToWriteOn = sheetToWriteOver.getRow(5);
		cellToWriteOn = rowToWriteOn.getCell(3);
		cellToWriteOn.setCellValue((ppiConcat.replaceAll("_", " ")).replaceAll("\"",""));
		//POC
		for (int i = 0; i < pocResults.size(); i++) {	
			ArrayList poc = (ArrayList) pocResults.get(i);
			for(int j = 0; j < poc.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(5);
				cellToWriteOn = rowToWriteOn.getCell(7);
				String value = (String) poc.get(j);
				cellToWriteOn.setCellValue((value.replaceAll("_", " ")).replaceAll("\"",""));
			}
		}
		//Date
		rowToWriteOn = sheetToWriteOver.getRow(5);
		cellToWriteOn = rowToWriteOn.getCell(13);
		cellToWriteOn.setCellValue(DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()));
		
		//Transition Perspective
		fillStringInText(sheetToWriteOver, 7, 1, "@DISPOSITION@", disposition);
		rowToWriteOn = sheetToWriteOver.getRow(10);
		cellToWriteOn = rowToWriteOn.getCell(5);
		cellToWriteOn.setCellValue(percentDataCreatedDHMSMProvide);
		rowToWriteOn = sheetToWriteOver.getRow(10);
		cellToWriteOn = rowToWriteOn.getCell(15);
		cellToWriteOn.setCellValue(percentBLUCreatedDHMSMProvide);
		
		//RTM Information		
		for (int i = 0; i < rtmResults.size(); i++) {	
			ArrayList rtmText = (ArrayList) rtmResults.get(i);
			for(int j = 0; j < rtmText.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(9);
				cellToWriteOn = rowToWriteOn.getCell(4);
				String value = (String) rtmText.get(j);
				if (!value.equals("")) 
					cellToWriteOn.setCellValue((value.replaceAll("_", " ")).replaceAll("\"", ""));
				else {
					cellToWriteOn.setCellValue("Unknown");
				}
			}
		}

		//Interfaces Summary
		//Current Interfaces Overview
		HashSet uniqueInterfacesSupported = new HashSet();
		HashSet uniqueDownstreamSystemsInterfaced = new HashSet();
		HashSet uniqueDataObjects = new HashSet();		
		HashSet uniqueProtocols = new HashSet();
		
		for (int i = 0; i < icdResults.size(); i++) {
			ArrayList row = (ArrayList) icdResults.get(i);

			for (int j=0; j<row.size(); j++) {
				if (j==1)
					uniqueInterfacesSupported.add(row.get(j));
				if (j==2) {
					if (row.get(j).toString().equals(systemName))
						uniqueDownstreamSystemsInterfaced.add(row.get(j+1));
				}
				if (j==4)
					uniqueDataObjects.add(row.get(j));
				if (j==7)
					uniqueProtocols.add(row.get(j));
			}			
		}
			
		rowToWriteOn = sheetToWriteOver.getRow(15);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(uniqueInterfacesSupported.size());
		cellToWriteOn = rowToWriteOn.getCell(3);
		cellToWriteOn.setCellValue(uniqueDownstreamSystemsInterfaced.size());
		cellToWriteOn = rowToWriteOn.getCell(5);
		cellToWriteOn.setCellValue(uniqueDataObjects.size());
		cellToWriteOn = rowToWriteOn.getCell(7);
		cellToWriteOn.setCellValue(referenceRepositoryList.size());
		cellToWriteOn = rowToWriteOn.getCell(9);
		cellToWriteOn.setCellValue(uniqueProtocols.size());
			
		//Future State Interfaces Perspective	
		rowToWriteOn = sheetToWriteOver.getRow(15);
		cellToWriteOn = rowToWriteOn.getCell(12);
		cellToWriteOn.setCellValue(decomICDCount);
		cellToWriteOn = rowToWriteOn.getCell(14);
		cellToWriteOn.setCellValue(sustainICDCount);
		cellToWriteOn = rowToWriteOn.getCell(15);
		cellToWriteOn.setCellValue(devICDCount);
		
			
		//System Highlights
		for (int i=0; i<systemHighlightsResults.size(); i++) {	
			ArrayList highlights = (ArrayList) systemHighlightsResults.get(i);			
			for (int j=0; j< highlights.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(22);
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
					else if(highlight.equals("NA")||highlight.equals("TBD")||highlight.equals(""))
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
	}
	
	/**
	 * Writes a generic list sheet from hashtable
	 * @param sheetName	String containing the name of the sheet to populate
	 * @param results	ArrayList containing the output of the query
	 */
	public void writeListSheet(String sheetName, HashMap<String,Object> result, boolean systemProbabilityHigh){

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");
		writeHeader(wb, sheetToWriteOver);
		fillStringInText(sheetToWriteOver,3,0,null,systemName);
		
		int rowToStart = 6;
		
		if(!sheetName.contains("Interface"))
			fillStringInText(sheetToWriteOver, 3,1,"@SYSTEM@",systemName);
		else 
			fillStringInText(sheetToWriteOver, 3,2,"@SYSTEM@",systemName);
//		else
//		{
//			fillStringInText(sheetToWriteOver, 3,3,"@SYSTEM@",systemName);
//			rowToStart = 9;
//		}


		for (int row=0; row<dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			XSSFRow rowToWriteOn;
			XSSFRow rowToCopyFormat = sheetToWriteOver.getRow(rowToStart);
			if(row==0)
			{
				rowToWriteOn = sheetToWriteOver.getRow(rowToStart+row);
			}
			else
			{
				rowToWriteOn = sheetToWriteOver.createRow(rowToStart+row);
				rowToWriteOn.setRowStyle(rowToCopyFormat.getRowStyle());
			}
			for (int col=0; col< resultRowValues.length; col++) {
				XSSFCell cellToWriteOn;
				if(row==0)
					cellToWriteOn = rowToWriteOn.getCell(col);
				else
				{
					cellToWriteOn = rowToWriteOn.createCell(col);
					XSSFCell cellToCopyFormat = rowToCopyFormat.getCell(col);
					cellToWriteOn.setCellStyle(cellToCopyFormat.getCellStyle());
				}
				if(resultRowValues[col] instanceof Double) {
					cellToWriteOn.setCellValue((Double)resultRowValues[col]);
				}
				else if(resultRowValues[col] instanceof Integer) {
					cellToWriteOn.setCellValue((Integer)resultRowValues[col]);
				}
				else {
					cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
				}				
			}
			if (systemProbabilityHigh) {
				XSSFCell cellToWriteOn;
				if(row==0)
					cellToWriteOn = rowToWriteOn.getCell(7);
				else
				{
					cellToWriteOn = rowToWriteOn.createCell(7);
				}
				XSSFCell cellToCopyFormat = rowToCopyFormat.getCell(7);
				cellToWriteOn.setCellStyle(cellToCopyFormat.getCellStyle());
				cellToWriteOn.setCellValue((String) "Decommission Interface.");
			}
		}

		// for System Interfaces Sheet
		if(result.get("directCost") != null && result.get("indirectCost") != null)
		{
			double totalDirectCost = (Double) result.get("directCost");
			double totalIndirectCost = (Double) result.get("indirectCost");
			int lastRowNum = sheetToWriteOver.getLastRowNum();
			XSSFRow lastRow = sheetToWriteOver.getRow(lastRowNum);
			int lastCellNum = lastRow.getLastCellNum();

			if(dataList.size()>0)
			{				
				lastRow = sheetToWriteOver.createRow(lastRowNum+1);
				XSSFCell cellToWriteOn = lastRow.createCell(lastCellNum-2);
				cellToWriteOn.setCellStyle(myStyles.get("accountStyle"));
				cellToWriteOn.setCellValue(totalDirectCost);
				cellToWriteOn = lastRow.createCell(lastCellNum-1);
				cellToWriteOn.setCellStyle(myStyles.get("accountStyle"));
				cellToWriteOn.setCellValue(totalIndirectCost);
			}
		}
	}
	
	/**
	 * Writes the data from the site list query to the Site List Sheet
	 * @param wb		XSSFWorkbook containing the Site List Sheet to populate
	 * @param result	ArrayList containing the output of the site list query
	 */
	public void writeSiteListSheet (XSSFWorkbook wb, ArrayList result){
		XSSFSheet sheetToWriteOver = wb.getSheet("Site List");
		writeHeader(wb, sheetToWriteOver);
		
		//Write System Name
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue(systemName);

		//Write the list of deployment sites
		for (int i = 0; i < result.size(); i++) {
			ArrayList row = (ArrayList) result.get(i);
			rowToWriteOn = sheetToWriteOver.getRow(i+7);

			for (int j = 0; j < row.size(); j++) {
				cellToWriteOn = rowToWriteOn.getCell(j+1);
				if (j==row.size() - 1)
					cellToWriteOn = rowToWriteOn.getCell(4);
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
		XSSFSheet sheetToWriteOver = wb.getSheet("Financials");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);

		writeHeader(wb, sheetToWriteOver);
		cellToWriteOn.setCellValue(systemName);

		for (int i=0; i<result.size(); i++) {
			ArrayList row = (ArrayList) result.get(i);

			if ( ((String) row.get(0)).equals("OP_Total") ) {					
				rowToWriteOn = sheetToWriteOver.getRow(8);
				for (int j=1; j<row.size(); j++) {
					cellToWriteOn = rowToWriteOn.getCell(j+2);
					cellToWriteOn.setCellValue( (Double) row.get(j));
				}
			}
			if ( ((String) row.get(0)).equals("O&M_Total") ) {
				rowToWriteOn = sheetToWriteOver.getRow(9);
				for (int j=1; j<row.size(); j++) {
					cellToWriteOn = rowToWriteOn.getCell(j+2);
					cellToWriteOn.setCellValue( (Double) row.get(j));
				}
			}
			if ( ((String) row.get(0)).equals("RDT&E_Total") ) {
				rowToWriteOn = sheetToWriteOver.getRow(10);
				for (int j=1; j<row.size(); j++) {
					cellToWriteOn = rowToWriteOn.getCell(j+2);
					cellToWriteOn.setCellValue( (Double) row.get(j));
				}
			}
		}
	}
	
	/**
	 * Writes the hidden Feeder tab to populate the pie charts in the System Overview Sheet
	 * @param wb		XSSFWorkbook containing the Feeder Sheet to populate
	 * @param swList	ArrayList containing the software life cycle results
	 * @param hwList	ArrayList containing the hardware life cycle results
	 */
	public void writeFeederSheet(XSSFWorkbook wb, ArrayList capabilitiesSupportedResults) {
		XSSFSheet sheetToWriteOver = wb.getSheet("Feeder");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(1);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
		
		
		//Write information for capability graph
		//HSD
		rowToWriteOn = sheetToWriteOver.getRow(15);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue((Integer) capabilitiesSupportedResults.get(0));
		//HSS
		rowToWriteOn = sheetToWriteOver.getRow(16);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue((Integer) capabilitiesSupportedResults.get(1));
		//FHP
		rowToWriteOn = sheetToWriteOver.getRow(17);
		cellToWriteOn = rowToWriteOn.getCell(1);
		cellToWriteOn.setCellValue((Integer) capabilitiesSupportedResults.get(2));
	}
	
	/**
	 * Writes the DHMSM Transition Cost Report Information into the fact sheet
	 * @param wb 		XSSFWorkbook to add the headers
	 * @param sheet 	XSSFSheet within the workbook to add the header
	 * @throws EngineException 
	 */
	public void writeTransitionCosts(DHMSMIntegrationTransitionCostWriter costWriter, boolean systemProbabilityHigh) throws EngineException {
		XSSFSheet reportSheet = wb.getSheet("Financials");
		if (!systemProbabilityHigh) {
			costWriter.calculateValuesForReport();
			int i;
			int j;
			int numTags = costWriter.tags.length;
			int numPhases = costWriter.phases.length;
			int rowToOutput = 15;
			
			double[] totalCost = new double[2];
			for(i = 0; i < numTags; i++) {
				if(costWriter.tags[i].contains("Provide")){
					rowToOutput = 23;
				}
				for(j = 0; j < numPhases; j++) {
					String key = costWriter.tags[i].concat("+").concat(costWriter.phases[j]);
					String key1 = costWriter.tags1[i].concat("+").concat(costWriter.phases[j]);
					XSSFRow rowToWriteOn = reportSheet.getRow(rowToOutput);
					if(costWriter.consolidatedSysCostInfo.containsKey(key)) {
						Double cost = costWriter.consolidatedSysCostInfo.get(key);
						if(cost == null) {
							cost = (double) 0;
						} else {
							cost *= costWriter.costPerHr;
						}
						totalCost[i] += cost;
						XSSFCell cellToWriteOn = rowToWriteOn.getCell(3);
						cellToWriteOn.setCellValue(Math.round(cost));
					} else if(costWriter.consolidatedSysCostInfo.containsKey(key1)) {
						Double cost = costWriter.consolidatedSysCostInfo.get(key1);
						if(cost == null) {
							cost = (double) 0;
						} else {
							cost *= costWriter.costPerHr;
						}
						totalCost[i] += cost;
						XSSFCell cellToWriteOn = rowToWriteOn.getCell(3);
						cellToWriteOn.setCellValue(Math.round(cost));
					}
					rowToOutput++;
				}
			}
			
			double consumerTraining = totalCost[0]*costWriter.trainingFactor;
			reportSheet.getRow(21).getCell(3).setCellValue(Math.round(consumerTraining));
			double providerTraining = totalCost[1]*costWriter.trainingFactor;
			reportSheet.getRow(29).getCell(3).setCellValue(Math.round(providerTraining));
			
			for(i = 0; i < 2; i++) {
				int startRow;
				if(i == 0) {
					startRow = 20;
				} else {
					startRow = 28;
				}
				double sustainmentCost = totalCost[i]*costWriter.sustainmentFactor;
				reportSheet.getRow(startRow).getCell(4).setCellValue(Math.round(sustainmentCost));
				for(j = 0; j < 3; j++) {
					sustainmentCost *= (1+costWriter.inflation);
					XSSFCell cellToWriteOn = reportSheet.getRow(startRow).getCell(5+j);
					cellToWriteOn.setCellValue(Math.round(sustainmentCost));
				}
			}
			
			//sum accross columns
			int k;
			for(k = 3; k < 8; k++) {
				for(i = 0; i < 2; i++) {
					int startRow;
					if(i == 0) {
						startRow = 15;
					} else {
						startRow = 23;
					}
					double sumColumn = 0;
					for(j = 0; j < 7; j++) {
						double val = reportSheet.getRow(startRow+j).getCell(k).getNumericCellValue();
						sumColumn += val;
					}
					reportSheet.getRow(startRow+7).getCell(k).setCellValue(sumColumn);
				}
			}
			//sum accross rows
			for(k = 0; k < 8; k++) {
				for(i = 0; i < 2; i++) {
					int startRow;
					if(i == 0) {
						startRow = 15;
					} else {
						startRow = 23;
					}
					double sumRow = 0;
					for(j = 3; j < 8; j++) {
						double val = reportSheet.getRow(startRow+k).getCell(j).getNumericCellValue();
						sumRow += val;
					}
					reportSheet.getRow(startRow+k).getCell(8).setCellValue(sumRow);
				}
			}
			
			reportSheet.getRow(32).getCell(3).setCellValue(Math.round(costWriter.sumHWSWCost));
			//since hwsw cost assumed at FY15, total is equal to value at FY15
			reportSheet.getRow(32).getCell(8).setCellValue(Math.round(costWriter.sumHWSWCost));
			
			int numATO = 0;
			if(costWriter.atoDateList[0] < 2015) {
				costWriter.atoDateList[0] = costWriter.atoDateList[1];
				costWriter.atoDateList[1] += 3; 
			}
			for(Integer date : costWriter.atoDateList) {
				if(date == 2015){
					reportSheet.getRow(34).getCell(3).setCellValue(costWriter.atoCost);
					numATO++;
				} else if(date == 2016) {
					reportSheet.getRow(34).getCell(4).setCellValue(costWriter.atoCost);
					numATO++;
				} else if(date == 2017) {
					reportSheet.getRow(34).getCell(5).setCellValue(costWriter.atoCost);
					numATO++;
				} else if(date == 2018) {
					reportSheet.getRow(34).getCell(6).setCellValue(costWriter.atoCost);
					numATO++;
				} else if(date == 2019) {
					reportSheet.getRow(34).getCell(7).setCellValue(costWriter.atoCost);
					numATO++;
				}
			}
			reportSheet.getRow(34).getCell(8).setCellValue(costWriter.atoCost * numATO);
			
			for(i = 3; i < 9; i++){
				double consumerCost = reportSheet.getRow(22).getCell(i).getNumericCellValue();
				double providerCost = reportSheet.getRow(30).getCell(i).getNumericCellValue();
				double hwswCost = reportSheet.getRow(32).getCell(i).getNumericCellValue();
				double diacapCost = reportSheet.getRow(34).getCell(i).getNumericCellValue();
				
				reportSheet.getRow(35).getCell(i).setCellValue(consumerCost + providerCost + hwswCost + diacapCost);
			}
		}
				
		String description = reportSheet.getRow(4).getCell(1).getStringCellValue();
		description = description.replaceAll(costWriter.sysKey, systemName);
		reportSheet.getRow(4).getCell(1).setCellValue(description);
		
		String tabelEnd = reportSheet.getRow(35).getCell(1).getStringCellValue();
		tabelEnd = tabelEnd.replaceAll(costWriter.sysKey, systemName);
		reportSheet.getRow(35).getCell(1).setCellValue(tabelEnd);
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
		//header.setCenter("&B &11 OTM System Disposition Fact Sheets");
		//header.setLeft("&B &11" + systemName);
		header.setLeft((String) header.getLeft().replaceAll("@SYSTEM@", systemName));
		//header.setRight("&B &11" + "Page " + (1+wb.getSheetIndex(sheet)));
	}
	
	public void fillStringInText(XSSFSheet sheetToWriteOver, int row, int col,String textToFind,String textToReplace) {
		XSSFRow rowToSetSystemName = sheetToWriteOver.getRow(row);
		XSSFCell cellToSetSystemName = rowToSetSystemName.getCell(col);
		if(textToFind == null)
			cellToSetSystemName.setCellValue(textToReplace);
		else{
			String currString = cellToSetSystemName.getStringCellValue();
			currString = currString.replaceAll(textToFind,textToReplace);
			cellToSetSystemName.setCellValue(currString);
		}
	}
	
	public void hideWorkSheet(String excelWorkSheetName) {
		wb.setSheetHidden(wb.getSheetIndex(excelWorkSheetName), true);
	}

	/**
	 * Creates a cell border style in an Excel workbook
	 * @param wb 		Workbook to create the style
	 * @return style	XSSFCellStyle containing the format
	 */
	private static XSSFCellStyle createBorderedStyle(Workbook wb) {
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
	private void makeStyles(XSSFWorkbook workbook) {
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
		
		XSSFCellStyle accountStyle = createBorderedStyle(workbook);
		accountStyle.setFont(boldBodyFont);
		accountStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		XSSFDataFormat df = wb.createDataFormat();
		accountStyle.setDataFormat(df.getFormat("$* #,##0.00"));
		myStyles.put("accountStyle", accountStyle);
	}

}
