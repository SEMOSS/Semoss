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

import java.awt.Font;
import java.awt.font.FontRenderContext;
import java.awt.font.LineBreakMeasurer;
import java.awt.font.TextAttribute;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.AttributedString;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.ui.components.specific.tap.IndividualSystemTransitionReport;
import prerna.ui.components.specific.tap.OCONUSMapExporter;
import prerna.util.Constants;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class TAPLegacySystemDispositionReportWriter {

	private static final Logger LOGGER = LogManager.getLogger(TAPLegacySystemDispositionReportWriter.class.getName());

	private String sysName;
	private String sysURI;
	private HashMap<String,String> reportTypeHash = new HashMap<String,String>();

	//query for basic sys information
	private String basicSysInfoQuery = "SELECT DISTINCT (COALESCE(?description,'') AS ?Description) (GROUP_CONCAT(?Owner ; SEPARATOR = ', ') AS ?SysOwner) (COALESCE(?Ato,'') AS ?ATO) WHERE { SELECT DISTINCT ?sys (COALESCE(?des,'') AS ?description) (SUBSTR(STR(?owner),50) AS ?Owner) (COALESCE(SUBSTR(STR(?ato),0,10),'') AS ?Ato) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?sys <http://semoss.org/ontologies/Relation/OwnedBy> ?owner} OPTIONAL{ {?sys <http://semoss.org/ontologies/Relation/Contains/Description> ?des} } OPTIONAL{ {?sys <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ato} } } } GROUP BY ?description ?Ato BINDINGS ?sys {(@BINDING_STRING@)}";

	//queries for modernization activities
	private String sysSWCostQuery = "SELECT DISTINCT ?System (COUNT(?SoftwareVersion) AS ?numProducts) (SUM(COALESCE(?swTotalCost, 0)) AS ?total) WHERE { SELECT DISTINCT ?System ?SoftwareVersion (COALESCE(?unitcost*?Quantity,0) AS ?swTotalCost) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?SoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?SoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>} {?System <http://semoss.org/ontologies/Relation/Consists> ?SoftwareModule} {?SoftwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?SoftwareVersion} {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } } GROUP BY ?System";
	private HashMap<String, HashMap<String, Double>> sysSWHash = new HashMap<String, HashMap<String, Double>>();
	private String sysHWCostQuery = "SELECT DISTINCT ?System (COUNT(?HardwareVersion) AS ?numProducts) (SUM(COALESCE(?hwTotalCost, 0)) AS ?total) WHERE { SELECT DISTINCT ?System ?HardwareVersion (COALESCE(?unitcost*?Quantity,0) AS ?hwTotalCost) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?HardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?HardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>} {?System <http://semoss.org/ontologies/Relation/Has> ?HardwareModule} {?HardwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?HardwareVersion } {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } } GROUP BY ?System";
	private HashMap<String, HashMap<String, Double>> sysHWHash = new HashMap<String, HashMap<String, Double>>();
	private String sysInterfaceModCostQuery = "SELECT DISTINCT ?system ?cost WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?system <http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost> ?cost} }";
	private HashMap<String, Double> sysInterfaceModHash = new HashMap<String, Double>();

	//queries for modernization timeline
	private String systemBudgetQuery = "SELECT DISTINCT ?system ?year ?cost WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systembudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?system <http://semoss.org/ontologies/Relation/Has> ?systembudget} {?year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> } {?systembudget <http://semoss.org/ontologies/Relation/OccursIn> ?year} {?systembudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?cost} }";
	private HashMap<String, HashMap<String, Object>> sysBudgetHash = new HashMap<String, HashMap<String, Object>>(); //cannot use Double because when no info received it is stored as "No_cost_info_received."

	private final String bindingsKey = "@BINDING_STRING@";
	private final String numProductKey = "numProducts";
	private final String updateCostKey = "upgradeCostKey";

	private IEngine HR_Core;
	private IEngine TAP_Portfolio;

	private XSSFWorkbook wb;
	private XSSFSheet reportSheet;
	
	private final double atoCost = 150000;
	int[] atoDateList = new int[2];
	private String description;
	private String sysOwner;
	private String ato;
	private String atoRenewalDate;
	
	private IndividualSystemTransitionReport report;
	
	public TAPLegacySystemDispositionReportWriter() throws EngineException {
		HR_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		TAP_Portfolio = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Portfolio");

		if(HR_Core == null) {
			throw new EngineException("Could not find HR_Core database.\nPlease load the appropriate database to produce report");
		}
		if(TAP_Portfolio == null){
			throw new EngineException("Could not find TAP_Portfolio database.\nPlease load the appropriate database to produce report");
		}
	}
	
	public TAPLegacySystemDispositionReportWriter(String sysURI) throws EngineException {
		this.sysURI = sysURI.replace(">", "").replace("<", "");
		this.sysName = Utility.getInstanceName(this.sysURI);

		HR_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		TAP_Portfolio = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Portfolio");

		if(HR_Core == null) {
			throw new EngineException("Could not find HR_Core database.\nPlease load the appropriate database to produce report");
		}
		if(TAP_Portfolio == null){
			throw new EngineException("Could not find TAP_Portfolio database.\nPlease load the appropriate database to produce report");
		}
	}

	public int[] getAtoDateList(){
		return this.atoDateList;
	}
	
	public double getAtoCost(){
		return this.atoCost;
	}
	
	@SuppressWarnings("unchecked")
	public HashMap<String, HashMap<String, Double>> getSysSWHash(){
		return (HashMap<String, HashMap<String, Double>>) this.sysSWHash.clone();
	}
	
	@SuppressWarnings("unchecked")
	public HashMap<String, HashMap<String, Double>> getSysHWHash(){
		return (HashMap<String, HashMap<String, Double>>) this.sysHWHash.clone();
	}
	
	public void writeToExcel() throws FileReaderException, EngineException
	{
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports" + System.getProperty("file.separator");
		String templateName = "TAP_Legacy_System_Dispositions_Template.xlsx";
		try {
			wb = (XSSFWorkbook) WorkbookFactory.create(new File(workingDir + folder + templateName));
		} catch (InvalidFormatException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find template for report.");
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find template for report.");
		}

		reportSheet = wb.getSheetAt(0);
		processBasisSysInfo();
		writeBasicSysDescription();
		writeTransitionAnalysis();
		generateModernizationActivitiesData();
		ArrayList<Double> costInfo = writeModernizationActivities();
		writeModernizationTimeline(costInfo);
		writeInterfaceSummary();
		writeSystemDeployment();		
		String fileName = "TAP_Legacy_System_Dispositions_" + sysName + ".xlsx";
		Utility.writeWorkbook(wb, workingDir + folder + fileName);
	}

	private void writeSystemDeployment() {
		ArrayList<String> listWithSysName = new ArrayList<String>();
		listWithSysName.add(sysName);
		
		OCONUSMapExporter imageExporter = new OCONUSMapExporter();
		String imageLoc = imageExporter.processData(listWithSysName);
		
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(imageLoc); //FileInputStream obtains input bytes from the image file
			byte[] bytes = IOUtils.toByteArray(inputStream); //Get the contents of an InputStream as a byte[].
			int pictureIdx = wb.addPicture(bytes, XSSFWorkbook.PICTURE_TYPE_PNG); //Adds a picture to the workbook
			inputStream.close();

			CreationHelper helper = wb.getCreationHelper(); //Returns an object that handles instantiating concrete classes
			XSSFClientAnchor anchor = (XSSFClientAnchor) helper.createClientAnchor(); //Create an anchor that is attached to the worksheet
			anchor.setCol1(2); //select where to put the picture
			anchor.setRow1(25);
			anchor.setDx1((short)5*36000);
			anchor.setDy1((short)5*36000);

			Drawing drawing = reportSheet.createDrawingPatriarch(); //Creates the top-level drawing patriarch, specify sheet to draw on
			Picture pict = drawing.createPicture(anchor, pictureIdx); //Creates a picture
			pict.resize(0.618);
		} catch (FileNotFoundException e){
			LOGGER.info("CONUS Map image not found for this system");
		} catch (IOException e) {
			LOGGER.info("CONUS Map image not found for this system");
		} finally {
			try {
				if(inputStream!=null)
					inputStream.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void writeInterfaceSummary() throws EngineException {
		if(report == null) {
			report = new IndividualSystemTransitionReport();
		}
		report.setSystemName(sysName);
		//pass in empty hashmap and method automatically creates the required hash
		HashMap<String, Object> barHash = new HashMap<String, Object>();
		barHash = report.createInterfaceBarChart(barHash);
		
		String replaceSysName = reportSheet.getRow(24).getCell(10).getStringCellValue();
		replaceSysName = replaceSysName.replace("@SYSTEM@", sysName);
		reportSheet.getRow(24).getCell(10).setCellValue(replaceSysName);
		
		XSSFSheet chartSheet = wb.getSheetAt(1);
		int[] dataList = (int[])barHash.get("data");
		for(int i = 0; i < dataList.length; i++)
		{
			chartSheet.getRow(1+i).getCell(1).setCellValue(dataList[i]);
		}
	}

	private void writeModernizationTimeline(ArrayList<Double> costInfo) {
		Double hwswCost = costInfo.get(0);
		Double interfaceModCost = costInfo.get(1);
		reportSheet.getRow(16).getCell(10).setCellValue(hwswCost);
		reportSheet.getRow(17).getCell(10).setCellValue(interfaceModCost);

		generateSysBudgetData();
		
		String replaceSysName = reportSheet.getRow(13).getCell(9).getStringCellValue();
		replaceSysName = replaceSysName.replace("@SYSTEM@", sysName);
		reportSheet.getRow(13).getCell(9).setCellValue(replaceSysName);
		HashMap<String, Object> innerHash = sysBudgetHash.get(sysName);
		if(innerHash != null) 
		{
			String[] yearData = new String[]{"FY15", "FY16", "FY17", "FY18", "FY19"};		
			int offSet = 10;
			for(int i = 0; i < yearData.length; i++) {
				Object value = innerHash.get(yearData[i]);
				if(value == null) {
					value = (double) 0;
				} else {
					try {
						value = Double.parseDouble(value.toString());
						reportSheet.getRow(14).getCell(offSet + i).setCellValue((Double) value);
					} catch (NumberFormatException ex) {
						reportSheet.getRow(14).getCell(offSet + i).setCellValue(value.toString().replace("_", " "));
					}
				}
			}
		}
		
		for(Integer date : atoDateList) {
			if(date == 2015){
				reportSheet.getRow(18).getCell(10).setCellValue(atoCost);
			} else if(date == 2016) {
				reportSheet.getRow(18).getCell(11).setCellValue(atoCost);
			} else if(date == 2017) {
				reportSheet.getRow(18).getCell(12).setCellValue(atoCost);
			} else if(date == 2018) {
				reportSheet.getRow(18).getCell(13).setCellValue(atoCost);
			} else if(date == 2019) {
				reportSheet.getRow(18).getCell(14).setCellValue(atoCost);
			}
		}

		for(int i = 0; i < 5; i++)
		{
			double costHWSW = reportSheet.getRow(16).getCell(10+i).getNumericCellValue();
			double costIntMod = reportSheet.getRow(17).getCell(10+i).getNumericCellValue();
			double costDIACAP = reportSheet.getRow(18).getCell(10+i).getNumericCellValue();
			reportSheet.getRow(15).getCell(10+i).setCellValue(costHWSW + costIntMod + costDIACAP);
		}
	}

	private void generateSysBudgetData() {
		if(sysBudgetHash.isEmpty()) {
			ISelectWrapper sjsw = Utility.processQuery(TAP_Portfolio, systemBudgetQuery);
			String[] varNames = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(varNames[0]).toString();
				String year = sjss.getVar(varNames[1]).toString();
				Object cost = sjss.getVar(varNames[2]).toString();
				if(sysBudgetHash.get(sysName) == null) {
					HashMap<String, Object> innerHash = new HashMap<String, Object>();
					innerHash.put(year, cost);
					sysBudgetHash.put(sysName, innerHash);
				} else {
					HashMap<String, Object> innerHash = sysBudgetHash.get(sysName);
					innerHash.put(year, cost);
				}
			}
		}
	}
	
	public double[] calculateHwSwCostAndNumUpdates(HashMap<String, HashMap<String, Double>> sysSwHwHash)
	{
		HashMap<String, Double> innerHash = sysSwHwHash.get(sysName);
		Double hwNumUpdates = (double) 0;
		Double hwCost = (double) 0;
		if(innerHash != null) {
			hwNumUpdates = innerHash.get(numProductKey);
			if(hwNumUpdates == null) {
				hwNumUpdates = (double) 0;
			}
			hwCost = innerHash.get(updateCostKey);
			if(hwCost == null) {
				hwCost = (double) 0;
			}
		}
		
		return new double[]{hwNumUpdates, hwCost};
	}

	private ArrayList<Double> writeModernizationActivities() {
		double[] hwCostAndUpdate = calculateHwSwCostAndNumUpdates(sysHWHash);
		double[] swCostAndUpdate = calculateHwSwCostAndNumUpdates(sysSWHash);

		Double interfaceModCost = sysInterfaceModHash.get(sysName);
		if(interfaceModCost == null) {
			interfaceModCost = (double) 0;
		}

		reportSheet.getRow(14).getCell(4).setCellValue(hwCostAndUpdate[0]);
		reportSheet.getRow(14).getCell(6).setCellValue(hwCostAndUpdate[1]);
		reportSheet.getRow(15).getCell(4).setCellValue(swCostAndUpdate[0]);
		reportSheet.getRow(15).getCell(6).setCellValue(swCostAndUpdate[1]);
		reportSheet.getRow(16).getCell(6).setCellValue(interfaceModCost);

		int counter = 0;
		if(atoDateList[0] >= 2015 && atoDateList[0] <= 2019) {
			counter++;
		}
		if(atoDateList[1] >= 2015 && atoDateList[1] <= 2019) {
			counter++;
		}
		double totalAtoCost = counter * atoCost;
		reportSheet.getRow(17).getCell(6).setCellValue(totalAtoCost);
		
		//sum up totals
		reportSheet.getRow(18).getCell(6).setCellValue(hwCostAndUpdate[1] + swCostAndUpdate[1] + interfaceModCost + totalAtoCost);
		
		// pass cost information over to reduce access of hashmaps
		ArrayList<Double> costInfo = new ArrayList<Double>();
		costInfo.add(hwCostAndUpdate[1] + swCostAndUpdate[1]);
		costInfo.add(interfaceModCost);

		return costInfo;
	}

	public void generateModernizationActivitiesData() {
		if(sysSWHash.isEmpty()){
			ISelectWrapper sjsw = Utility.processQuery(HR_Core, sysSWCostQuery);
			String[] varNames = sjsw.getVariables();
			while(sjsw.hasNext()){
				ISelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(varNames[0]).toString();
				Double numProducts = (Double) sjss.getVar(varNames[1]);
				Double cost = (Double) sjss.getVar(varNames[2]);
				HashMap<String, Double> innerHash = new HashMap<String, Double>();
				innerHash.put(numProductKey, numProducts);
				innerHash.put(updateCostKey, cost);
				sysSWHash.put(sysName, innerHash);
			}
		}
		if(sysHWHash.isEmpty()){
			ISelectWrapper sjsw = Utility.processQuery(HR_Core, sysHWCostQuery);
			String[] varNames = sjsw.getVariables();
			while(sjsw.hasNext()){
				ISelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(varNames[0]).toString();
				Double numProducts = (Double) sjss.getVar(varNames[1]);
				Double cost = (Double) sjss.getVar(varNames[2]);
				HashMap<String, Double> innerHash = new HashMap<String, Double>();
				innerHash.put(numProductKey, numProducts);
				innerHash.put(updateCostKey, cost);
				sysHWHash.put(sysName, innerHash);
			}
		}
		if(sysInterfaceModHash.isEmpty()){
			ISelectWrapper sjsw = Utility.processQuery(HR_Core, sysInterfaceModCostQuery);
			String[] varNames = sjsw.getVariables();
			while(sjsw.hasNext()){
				ISelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(varNames[0]).toString();
				Double cost = (Double) sjss.getVar(varNames[1]);
				sysInterfaceModHash.put(sysName, cost);
			}
		}
	}
	
	private void writeTransitionAnalysis() {
		String lpiDescription = "LPI systems were designated by the Functional Advisory Council (FAC) as having a low probability of being replaced by the DHMSM EHR solution and were also designated as requiring integration with DHMSM in order to support data exchange.";
		String lpniDescription = "LPNI systems were designated by the Functional Advisory Council (FAC) as having a low probability of being replaced by the DHMSM EHR solution and were also designated as not requiring integration with DHMSM.";
		String hpiDescription = "HPI systems were designated by the Functional Advisory Council (FAC) as having a high probability of being replaced by the DHMSM EHR solution and were also designated as requiring integration with DHMSM in order to support data exchange.";
		String hpniDescription = "HPNI systems were designated by the Functional Advisory Council (FAC) as having a high probability of being replaced by the DHMSM EHR solution and were also designated as not requiring integration with DHMSM.";
		
		if(reportTypeHash.isEmpty())
			reportTypeHash = DHMSMTransitionUtility.processReportTypeQuery(HR_Core);
		String reportType = reportTypeHash.get(sysName).replaceAll("\"", "");
		if(reportType.equals("LPI")) {
			reportSheet.getRow(8).getCell(4).setCellValue("Low Probability with Integration (LPI)");
			reportSheet.getRow(8).getCell(7).setCellValue(lpiDescription);
		} else if(reportType.equals("LPNI")) {
			reportSheet.getRow(8).getCell(4).setCellValue("Low Probability without Integration (LPNI)");
			reportSheet.getRow(8).getCell(7).setCellValue(lpniDescription);
		} else if(reportType.equals("HPI")) {
			reportSheet.getRow(8).getCell(4).setCellValue("High Probability with Integration (HPI)");
			reportSheet.getRow(8).getCell(7).setCellValue(hpiDescription);
		} else if(reportType.equals("HPNI")) {
			reportSheet.getRow(8).getCell(4).setCellValue("High Probability without Integration (HPNI)");
			reportSheet.getRow(8).getCell(7).setCellValue(hpniDescription);
		}
	}

	private void writeBasicSysDescription() {
		reportSheet.getRow(3).getCell(1).setCellValue(sysName.replaceAll("_", " "));
		reportSheet.getRow(3).getCell(4).setCellValue(description.replaceAll("_", " "));
		// to autosize description row height
		Font currFont = new Font("Calibri", Font.PLAIN, 10);
		AttributedString attrStr = new AttributedString(description);
		attrStr.addAttribute(TextAttribute.FONT, currFont);
		FontRenderContext frc = new FontRenderContext(null, true, true);
		LineBreakMeasurer measurer = new LineBreakMeasurer(attrStr.getIterator(), frc);
		int nextPos = 0;
		int lineCnt = 0;
		while (measurer.getPosition() < description.length())
		{
		    nextPos = measurer.nextOffset(920f); // mergedCellWidth is the max width of each line
		    lineCnt++;
		    measurer.setPosition(nextPos);
		}
		XSSFRow currRow = reportSheet.getRow(3);
		if(lineCnt < 4){
			lineCnt = 5;
		}
		currRow.setHeight((short)(currRow.getHeight() * lineCnt));
		// end autosize description row height
		reportSheet.getRow(4).getCell(4).setCellValue(sysOwner);
		reportSheet.getRow(5).getCell(4).setCellValue(ato);
		reportSheet.getRow(5).getCell(11).setCellValue(atoRenewalDate);
	}

	public void processBasisSysInfo() {
		String query = basicSysInfoQuery.replace(bindingsKey, "<" + sysURI + ">");
		ISelectWrapper sjsw = Utility.processQuery(HR_Core, query);
		String[] varNames = sjsw.getVariables();
		// output binds to a single system - query should only return one line
		// if query returns nothing, never executed
		description = "Data Not Received";
		sysOwner = "Data Not Received";
		ato = "Data Not Received";
		atoRenewalDate = "REQUIRES IMMEDIATE ACCREDITATION";
		//String atoRenewal = "";
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			description = sjss.getVar(varNames[0]).toString();
			sysOwner = sjss.getVar(varNames[1]).toString();
			ato = sjss.getVar(varNames[2]).toString();
		}
		
		int atoYear = 0;
		int atoRenewalYear = 0;
		
		if(description == null){
			description = "Data Not Received";
		}
		if(sysOwner == null){
			sysOwner = "Data Not Received";
		}
		if(ato == null || ato.equals("") || ato.equals("NA") || ato.equals("Data Not Received")){
			// no ato information -> assume ato date is year 2015
			ato = "Data Not Received";
			atoYear = 2015;
			atoRenewalYear = 2018;
		} else {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Date atoDate;
			try {
				atoDate = df.parse(ato);
				Calendar c = Calendar.getInstance();
				c.setTime(atoDate);
				atoYear = c.get(Calendar.YEAR);
				atoRenewalYear = atoYear + 3;
				// if ato is very old
				if(atoYear+3 < 2015) {
					atoRenewalDate = "REQUIRES IMMEDIATE ACCREDITATION";
					// set ato to be year 2015
					atoYear = 2015;
					atoRenewalYear = atoYear + 3;
				} else { // set ato renewal date to be exactly 3 years from ato date
					String month = "";
					String day = "";
					if((c.get(Calendar.MONTH) + 1) < 10) {
						month = "0" + (c.get(Calendar.MONTH) + 1);
					} else {
						month = (c.get(Calendar.MONTH) + 1) + "";
					}
					if(c.get(Calendar.DAY_OF_MONTH) < 10) {
						day = "0" + c.get(Calendar.DAY_OF_MONTH);
					} else {
						day = c.get(Calendar.DAY_OF_MONTH) + "";
					}
					atoRenewalDate = (c.get(Calendar.YEAR) + 3) + "-" + month + "-" + day;
				}
			} catch (ParseException e) {
				//ignore
			}
		}
		
		atoDateList = new int[]{atoYear, atoRenewalYear};
		if(atoDateList[0] < 2015) {
			atoDateList[0] = atoDateList[1];
			atoDateList[1] += 3; 
		}
	}

	public void setReportTypeHash(HashMap<String,String> reportTypeHash) {
		this.reportTypeHash = reportTypeHash;
	}
	
	public void setSysURI(String sysURI) {
		this.sysURI = sysURI;
		this.sysName = Utility.getInstanceName(sysURI);
	}
}
