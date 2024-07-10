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
package prerna.ui.components.specific.tap;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IDatabaseEngine;
import prerna.poi.specific.TAPLegacySystemDispositionReportWriter;
import prerna.ui.components.specific.tap.AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK;
import prerna.util.Constants;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DHMSMIntegrationTransitionCostWriter {
	
	private static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationTransitionCostWriter.class.getName());
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private Map<String, Map<String, Double>> sysCost = new HashMap<String, Map<String, Double>>();

	private IDatabaseEngine TAP_Core_Data;
	private IDatabaseEngine TAP_Cost_Data;
	private IDatabaseEngine FutureDB;
	private IDatabaseEngine FutureCostDB;
	
	private String sysURI;
	private String systemName;
	double costPerHr = 150.0;
	double sumHWSWCost;
	int[] atoDateList = new int[2];

	private TAPLegacySystemDispositionReportWriter diacapReport;
	private LPInterfaceCostProcessor processor;
	
	final String[] phases = new String[]{"Requirements","Design","Develop","Test","Deploy"};
	final String[] tags1 = new String[]{"Consume", "Provide"};
	final String[] tags = new String[]{"Consumer", "Provider"};
	final double sustainmentFactor = 0.18;
	final double trainingFactor = 0.15;
	final double inflation = 0.018;
	final String sysKey = "@SYSTEM@";
	double atoCost;
	
	private Set<String> selfReportedSystems;
	private Set<String> sorV;
	private Map<String, String> sysTypeHash;
	private Map<String, Map<String, Map<String, Double>>> selfReportedSystemCostByPhase;
	
	public DHMSMIntegrationTransitionCostWriter() throws IOException{
		TAP_Cost_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(TAP_Cost_Data==null) {
			throw new IOException("TAP_Cost_Data database not found");
		}
		FutureDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureDB");
		if(FutureDB==null) {
			throw new IOException("FutureDB database not found");
		}
		FutureCostDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureCostDB");
		if(FutureCostDB==null) {
			throw new IOException("FutureCostDB database not found");
		}
		TAP_Core_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		if(TAP_Core_Data==null) {
			throw new IOException("TAP_Core_Data database not found");
		}
		selfReportedSystems = DHMSMTransitionUtility.getAllSelfReportedSystemNames(FutureDB);
		sorV = DHMSMTransitionUtility.processSysDataSOR(TAP_Core_Data);
		sysTypeHash = DHMSMTransitionUtility.processReportTypeQuery(TAP_Core_Data);
	}
	
	public void setCostPerHr(double costPerHr) {
		this.costPerHr = costPerHr;
	}
	
	public void setSysURI(String sysURI){
		this.sysURI = sysURI;
		this.systemName = Utility.getInstanceName(sysURI);
	}
	
	public void calculateValuesForReport() throws IOException 
	{
		if(processor == null) {
			processor = new LPInterfaceCostProcessor();
			processor.setEngine(TAP_Core_Data);
		}
		if(selfReportedSystemCostByPhase == null) {
			selfReportedSystemCostByPhase = DHMSMTransitionUtility.getSystemSelfReportedP2PCostByTagAndPhase(FutureCostDB, TAP_Cost_Data);
		}
		sysCost.clear();
		if(selfReportedSystems.contains(systemName)) {
			sysCost = selfReportedSystemCostByPhase.get(systemName);
		} else {
			sysCost = processor.generateSystemCostByTagPhase(systemName, selfReportedSystems, sorV, sysTypeHash, COST_FRAMEWORK.P2P);
		}
		
		if(diacapReport == null) {
			diacapReport = new TAPLegacySystemDispositionReportWriter(sysURI);
		} else {
			diacapReport.setSysURI(sysURI);
		}
		diacapReport.processBasisSysInfo();
		diacapReport.generateModernizationActivitiesData();
		this.atoCost = diacapReport.getAtoCost();
		double[] hwCostAndUpdates = diacapReport.calculateHwSwCostAndNumUpdates(diacapReport.getSysHWHash());
		double[] swCostAndUpdates = diacapReport.calculateHwSwCostAndNumUpdates(diacapReport.getSysSWHash());
		
		sumHWSWCost = hwCostAndUpdates[1] + swCostAndUpdates[1];
		
		atoDateList =  diacapReport.getAtoDateList();
	} 
	
	public void writeToExcel() throws IOException 
	{
		String templateName = "Transition_Estimates_Template.xlsx";
		String fileName = "MHS_GENESIS_Transition_Estimate_" + Utility.getInstanceName(sysURI) + ".xlsx";
		writeToExcel(templateName, fileName);
	}
	
	
	public void writeToExcel(String templateName, String fileName) throws IOException {
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = DIR_SEPARATOR + "export" + DIR_SEPARATOR + "Reports" + DIR_SEPARATOR;
		
		XSSFWorkbook wb;
		try {
			wb = (XSSFWorkbook) WorkbookFactory.create(new File(workingDir + folder + templateName));
		} catch (IOException e) {
			LOGGER.error(Constants.STACKTRACE, e);
			throw new IOException("Could not find template for report.");
		}
		
		wb = finishWB(wb);
		
		Utility.writeWorkbook(wb, workingDir + folder + fileName);
		
	}
	
	public XSSFWorkbook generateWB(String templateName) throws IOException {
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = DIR_SEPARATOR + "export" + DIR_SEPARATOR + "Reports" + DIR_SEPARATOR;
		
		XSSFWorkbook wb;
		try {
			wb = (XSSFWorkbook) WorkbookFactory.create(new File(workingDir + folder + templateName));
		} catch (IOException e) {
			LOGGER.error(Constants.STACKTRACE, e);
			throw new IOException("Could not find template for report.");
		}
		
		return finishWB(wb);
		
	}
	
	public XSSFWorkbook finishWB(XSSFWorkbook wb) throws IOException {
		
		XSSFSheet reportSheet = wb.getSheet("Transition Cost Estimates");
		int i;
		int j;
		int numTags = tags.length;
		int numPhases = phases.length;
		int rowToOutput = 7;
		
		double[] totalCost = new double[2];
		for(i = 0; i < numTags; i++) {
			if(tags[i].contains("Provide")){
				rowToOutput = 15;
			}
			for(j = 0; j < numPhases; j++) {
				String key = tags[i].concat("+").concat(phases[j]);
				String key1 = tags1[i].concat("+").concat(phases[j]);
				XSSFRow rowToWriteOn = reportSheet.getRow(rowToOutput);
				if(sysCost.containsKey(tags[i])) {
					Map<String, Double> costAtPhases = sysCost.get(tags[i]);
					if(costAtPhases.containsKey(phases[j])) {
						Double cost = costAtPhases.get(phases[j]);
						if(cost == null) {
							cost = (double) 0;
						} else {
							cost *= costPerHr;
						}
						totalCost[i] += cost;
						XSSFCell cellToWriteOn = rowToWriteOn.getCell(2);
						cellToWriteOn.setCellValue(Math.round(cost));
					}
				} else if(sysCost.containsKey(tags1)) {
					Map<String, Double> costAtPhases = sysCost.get(tags[i]);
					if(costAtPhases.containsKey(phases[j])) {
						Double cost = costAtPhases.get(phases[j]);
						if(cost == null) {
							cost = (double) 0;
						} else {
							cost *= costPerHr;
						}
						totalCost[i] += cost;
						XSSFCell cellToWriteOn = rowToWriteOn.getCell(2);
						cellToWriteOn.setCellValue(Math.round(cost));
					}
				}
				rowToOutput++;
			}
		}
		
		double consumerTraining = totalCost[0]*trainingFactor;
		reportSheet.getRow(13).getCell(2).setCellValue(Math.round(consumerTraining));
		double providerTraining = totalCost[1]*trainingFactor;
		reportSheet.getRow(21).getCell(2).setCellValue(Math.round(providerTraining));
		
		for(i = 0; i < 2; i++) {
			int startRow;
			if(i == 0) {
				startRow = 12;
			} else {
				startRow = 20;
			}
			double sustainmentCost = totalCost[i]*sustainmentFactor;
			reportSheet.getRow(startRow).getCell(3).setCellValue(Math.round(sustainmentCost));
			for(j = 0; j < 3; j++) {
				sustainmentCost *= (1+inflation);
				XSSFCell cellToWriteOn = reportSheet.getRow(startRow).getCell(4+j);
				cellToWriteOn.setCellValue(Math.round(sustainmentCost));
			}
		}
		
		//sum across columns
		int k;
		for(k = 2; k < 7; k++) {
			for(i = 0; i < 2; i++) {
				int startRow;
				if(i == 0) {
					startRow = 7;
				} else {
					startRow = 15;
				}
				double sumColumn = 0;
				for(j = 0; j < 7; j++) {
					double val = reportSheet.getRow(startRow+j).getCell(k).getNumericCellValue();
					sumColumn += val;
				}
				reportSheet.getRow(startRow+7).getCell(k).setCellValue(sumColumn);
			}
		}
		//sum across rows
		for(k = 0; k < 8; k++) {
			for(i = 0; i < 2; i++) {
				int startRow;
				if(i == 0) {
					startRow = 7;
				} else {
					startRow = 15;
				}
				double sumRow = 0;
				for(j = 2; j < 7; j++) {
					double val = reportSheet.getRow(startRow+k).getCell(j).getNumericCellValue();
					sumRow += val;
				}
				reportSheet.getRow(startRow+k).getCell(7).setCellValue(sumRow);
			}
		}
		
		reportSheet.getRow(24).getCell(2).setCellValue(Math.round(sumHWSWCost));
		//since hwsw cost assumed at FY15, total is equal to value at FY15
		reportSheet.getRow(24).getCell(7).setCellValue(Math.round(sumHWSWCost));
		
		int numATO = 0;
		if(atoDateList[0] < 2015) {
			atoDateList[0] = atoDateList[1];
			atoDateList[1] += 3; 
		}
		for(Integer date : atoDateList) {
			if(date == 2015){
				reportSheet.getRow(26).getCell(2).setCellValue(atoCost);
				numATO++;
			} else if(date == 2016) {
				reportSheet.getRow(26).getCell(3).setCellValue(atoCost);
				numATO++;
			} else if(date == 2017) {
				reportSheet.getRow(26).getCell(4).setCellValue(atoCost);
				numATO++;
			} else if(date == 2018) {
				reportSheet.getRow(26).getCell(5).setCellValue(atoCost);
				numATO++;
			} else if(date == 2019) {
				reportSheet.getRow(26).getCell(6).setCellValue(atoCost);
				numATO++;
			}
		}
		reportSheet.getRow(26).getCell(7).setCellValue(atoCost * numATO);
		
		for(i = 2; i < 8; i++){
			double consumerCost = reportSheet.getRow(14).getCell(i).getNumericCellValue();
			double providerCost = reportSheet.getRow(22).getCell(i).getNumericCellValue();
			double hwswCost = reportSheet.getRow(24).getCell(i).getNumericCellValue();
			double diacapCost = reportSheet.getRow(26).getCell(i).getNumericCellValue();
			
			reportSheet.getRow(27).getCell(i).setCellValue(consumerCost + providerCost + hwswCost + diacapCost);
		}
		
		String header = reportSheet.getRow(0).getCell(0).getStringCellValue();
		header = header.replaceAll(sysKey, systemName);
		reportSheet.getRow(0).getCell(0).setCellValue(header);
		
		String description = reportSheet.getRow(3).getCell(0).getStringCellValue();
		description = description.replaceAll(sysKey, systemName);
		reportSheet.getRow(3).getCell(0).setCellValue(description);
		
		String tableLabel = reportSheet.getRow(5).getCell(0).getStringCellValue();
		tableLabel = tableLabel.replaceAll(sysKey, systemName);
		reportSheet.getRow(5).getCell(0).setCellValue(tableLabel);

		String tabelEnd = reportSheet.getRow(27).getCell(0).getStringCellValue();
		tabelEnd = tableLabel.replaceAll(sysKey, systemName);
		reportSheet.getRow(27).getCell(0).setCellValue(tabelEnd);
		
		
		
		return wb;
		
	}
	
	
	

	public  Map<String, Map<String, Double>> getData(){
		return sysCost;
	}
	
	public double getCostPerHr(){
		return costPerHr;
	}
	
	public double getSumHWSWCost(){
		return sumHWSWCost;
	}
	
	public double getAtoCost(){
		return atoCost;
	}
	
	public int[] getAtoDateList(){
		return atoDateList;
	}
	
	public String getSystemName(){
		return systemName;
	}
}
