package prerna.ui.components.specific.tap;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.poi.specific.TAPLegacySystemDispositionReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DHMSMIntegrationTransitionCostWriter {
	
	private static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationTransitionCostWriter.class.getName());

	LPInterfaceProcessor processor;
	public HashMap<String, Double> consolidatedSysCostInfo = new HashMap<String, Double>();

	private IEngine hrCore;
	private IEngine TAP_Cost_Data;
	
	private String sysURI;
	private String systemName;
	private double costPerHr = 150.0;
	private double sumHWSWCost;
	int[] atoDateList = new int[2];

	private TAPLegacySystemDispositionReportWriter diacapReport;
	
	private final String[] phases = new String[]{"Requirements","Design","Develop","Test","Deploy"};
	private final String[] tags = new String[]{"Consume", "Provide"};
	private final double sustainmentFactor = 0.18;
	private final double trainingFactor = 0.15;
	private final double inflation = 0.018;
	private final String sysKey = "@SYSTEM@";
	private double atoCost;
	
	public DHMSMIntegrationTransitionCostWriter() throws EngineException{
		TAP_Cost_Data = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(TAP_Cost_Data==null) {
			throw new EngineException("TAP_Cost_Data database not found");
		}
		hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		if(hrCore==null) {
				throw new EngineException("HR_Core database not found");
		}
	}
	
	public void setCostPerHr(double costPerHr) {
		this.costPerHr = costPerHr;
	}
	
	public void setSysURI(String sysURI){
		this.sysURI = sysURI;
	}
	
	public void calculateValuesForReport() throws EngineException 
	{
		//clear list since we call this method in a loop when generating all reports
		if(processor == null){
			processor = new LPInterfaceProcessor();
			processor.getCostInfoAtPhaseLevel(TAP_Cost_Data);
			processor.getLPNIInfo(hrCore);

		} else {
			processor.setConsolidatedSysCostInfo(new HashMap<String, Double>());
			processor.setSysCostInfo(new HashMap<Integer, HashMap<String, Double>>());
		}
		this.systemName = Utility.getInstanceName(sysURI);
		systemName = systemName.replaceAll("\\(", "\\\\\\\\\\(").replaceAll("\\)", "\\\\\\\\\\)");
		String lpSystemInterfacesQuery = DHMSMTransitionUtility.lpSystemInterfacesQuery.replace("@SYSTEMNAME@", systemName);
		processor.setQuery(lpSystemInterfacesQuery);
		processor.setEngine(hrCore);
		processor.setUsePhase(true);
		processor.isGenerateCost(true);

		processor.generateReport();
		processor.consolodateCostHash();
		consolidatedSysCostInfo = processor.getConsolidatedSysCostInfo();
		
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
	
	public void writeToExcel() throws FileReaderException {
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports" + System.getProperty("file.separator");
		String templateName = "Transition_Estimates_Template.xlsx";
		XSSFWorkbook wb;
		try {
			wb = (XSSFWorkbook) WorkbookFactory.create(new File(workingDir + folder + templateName));
		} catch (InvalidFormatException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find template for report.");
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find template for report.");
		}

		XSSFSheet reportSheet = wb.getSheetAt(0);
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
				XSSFRow rowToWriteOn = reportSheet.getRow(rowToOutput);
				if(consolidatedSysCostInfo.containsKey(key)) {
					double cost = consolidatedSysCostInfo.get(key)*costPerHr;
					totalCost[i] += cost;
					XSSFCell cellToWriteOn = rowToWriteOn.getCell(2);
					cellToWriteOn.setCellValue(Math.round(cost));
				}
				rowToOutput++;
			}
		}
		//
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
		
		//sum accross columns
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
		//sum accross rows
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
		
		
		String fileName = "DHMSM_Transition_Estimate_" + Utility.getInstanceName(sysURI) + ".xlsx";
		Utility.writeWorkbook(wb, workingDir + folder + fileName);
		
	}

	public  HashMap<String, Double> getData(){
		return consolidatedSysCostInfo;
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
	
	public String getSysKey(){
		return sysKey;
	}
	
	public String getSystemName(){
		return systemName;
	}
}
