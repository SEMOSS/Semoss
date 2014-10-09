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

@SuppressWarnings("serial")
public class DHMSMIntegrationTransitionBySystemOwnerWriter {
	
	//logger
	private static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationTransitionBySystemOwnerWriter.class.getName());
	
	private XSSFWorkbook wb;
	private String workingDir;
	private String folder;
	private String templateName;
	
	//workbook management for iterating over multiple systems
	
	//constants
	private final String[] phases = new String[]{"Requirements","Design","Develop","Test","Deploy"};
	private final String[] tags = new String[]{"Consume", "Provider"};
	private final double sustainmentFactor = 0.18;
	private final double trainingFactor = 0.15;
	private final double inflation = 0.018;
	
	private int[] atoDateList;
	private double costPerHr;
	private double atoCost;
	private double sumHWSWCost;
	private String sysKey;
	private String systemName;
	private String sysURI;
	private HashMap<String, Double> consolidatedSysCostInfo;
	
	public DHMSMIntegrationTransitionBySystemOwnerWriter(){
		workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports" + System.getProperty("file.separator");
		templateName = "Transition_Estimates_Template.xlsx";
	}
	
	public void setDataSource(DHMSMIntegrationTransitionCostWriter data){
		this.consolidatedSysCostInfo = data.getData();
		this.atoCost = data.getAtoCost();
		this.sumHWSWCost = data.getSumHWSWCost();
		this.costPerHr = data.getCostPerHr();
		this.atoDateList = data.getAtoDateList();
		this.sysKey = data.getSysKey();
		this.systemName = data.getSystemName();
	}
	
	public void setSysURI(String sysURI){
		this.sysURI = sysURI;
	}
	
	@SuppressWarnings("deprecation")
	public void write(int count) throws FileReaderException{
		if(wb == null) {
			try {
				wb = (XSSFWorkbook) WorkbookFactory.create(new File(workingDir + folder + "Test.xlsx"));
			} 
			catch (InvalidFormatException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not find template for report.");
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not find template for report.");
			}
		}
		
		XSSFSheet reportSheet = wb.getSheetAt(0);	
		int i;
		int j;
		int numTags = tags.length;
		int numPhases = phases.length;
		int rowToOutput = 7*count;
		
		double[] totalCost = new double[2];
		for(i = 0; i < numTags; i++) {
			if(tags[i].contains("Provide")){
				rowToOutput = 15*count;
			}
			for(j = 0; j < numPhases; j++) {
				String key = tags[i].concat("+").concat(phases[j]);
				XSSFRow rowToWriteOn = reportSheet.createRow(rowToOutput);
				if(consolidatedSysCostInfo.containsKey(key)) {
					double cost = consolidatedSysCostInfo.get(key)*costPerHr;
					totalCost[i] += cost;
					XSSFCell cellToWriteOn = rowToWriteOn.createCell(2);
					cellToWriteOn.setCellValue(Math.round(cost));
				}
				rowToOutput++;
			}
		}
		//
		double consumerTraining = totalCost[0]*trainingFactor;
		reportSheet.createRow(13).createCell(2).setCellValue(Math.round(consumerTraining));
		double providerTraining = totalCost[1]*trainingFactor;
		reportSheet.createRow(21).createCell(2).setCellValue(Math.round(providerTraining));
		
		for(i = 0; i < 2; i++) {
			int startRow;
			if(i == 0) {
				startRow = 12*count;
			} else {
				startRow = 20*count;
			}
			double sustainmentCost = totalCost[i]*sustainmentFactor;
			reportSheet.createRow(startRow).createCell(3).setCellValue(Math.round(sustainmentCost));
			for(j = 0; j < 3; j++) {
				sustainmentCost *= (1+inflation);
				XSSFCell cellToWriteOn = reportSheet.createRow(startRow).createCell(4+j);
				cellToWriteOn.setCellValue(Math.round(sustainmentCost));
			}
		}
		
		//sum across columns
		int k;
		for(k = 2; k < 7; k++) {
			for(i = 0; i < 2; i++) {
				int startRow;
				if(i == 0) {
					startRow = 7*count;
				} else {
					startRow = 15*count;
				}
				double sumColumn = 0;
				for(j = 0; j < 7; j++) {
					double val = reportSheet.createRow(startRow+j).createCell(k).getNumericCellValue();
					sumColumn += val;
				}
				reportSheet.createRow(startRow+7).createCell(k).setCellValue(sumColumn);
			}
		}
		//sum across rows
		for(k = 0; k < 8; k++) {
			for(i = 0; i < 2; i++) {
				int startRow;
				if(i == 0) {
					startRow = 7*count;
				} else {
					startRow = 15*count;
				}
				double sumRow = 0;
				for(j = 2; j < 7; j++) {
					double val = reportSheet.createRow(startRow+k).createCell(j).getNumericCellValue();
					sumRow += val;
				}
				reportSheet.createRow(startRow+k).createCell(7).setCellValue(sumRow);
			}
		}
		
		reportSheet.createRow(24).createCell(2).setCellValue(Math.round(sumHWSWCost));
		//since hwsw cost assumed at FY15, total is equal to value at FY15
		reportSheet.createRow(24).createCell(7).setCellValue(Math.round(sumHWSWCost));
		
		int numATO = 0;
		if(atoDateList[0] < 2015) {
			atoDateList[0] = atoDateList[1];
			atoDateList[1] += 3; 
		}
		for(Integer date : atoDateList) {
			if(date == 2015){
				reportSheet.createRow(26*count).createCell(2).setCellValue(atoCost);
				numATO++;
			} else if(date == 2016) {
				reportSheet.createRow(26*count).createCell(3).setCellValue(atoCost);
				numATO++;
			} else if(date == 2017) {
				reportSheet.createRow(26*count).createCell(4).setCellValue(atoCost);
				numATO++;
			} else if(date == 2018) {
				reportSheet.createRow(26*count).createCell(5).setCellValue(atoCost);
				numATO++;
			} else if(date == 2019) {
				reportSheet.createRow(26*count).createCell(6).setCellValue(atoCost);
				numATO++;
			}
		}
		reportSheet.createRow(26*count).createCell(7).setCellValue(atoCost * numATO);
		
		for(i = 2; i < 8; i++){
			double consumerCost = reportSheet.createRow(14*count).createCell(i).getNumericCellValue();
			double providerCost = reportSheet.createRow(22*count).createCell(i).getNumericCellValue();
			double hwswCost = reportSheet.createRow(24*count).createCell(i).getNumericCellValue();
			double diacapCost = reportSheet.createRow(26*count).createCell(i).getNumericCellValue();
			
			reportSheet.createRow(27*count).createCell(i).setCellValue(consumerCost + providerCost + hwswCost + diacapCost);
		}
		
		String header = reportSheet.createRow(0).createCell(0).getStringCellValue();
		header = header.replaceAll(sysKey, systemName);
		reportSheet.createRow(0).createCell(0).setCellValue(header);
		
		String description = reportSheet.createRow(3).createCell(0).getStringCellValue();
		description = description.replaceAll(sysKey, systemName);
		reportSheet.createRow(3).createCell(0).setCellValue(description);
		
		String tableLabel = reportSheet.createRow(5*count).createCell(0).getStringCellValue();
		tableLabel = tableLabel.replaceAll(sysKey, systemName);
		reportSheet.createRow(5*count).createCell(0).setCellValue(tableLabel);

		String tabelEnd = reportSheet.createRow(27*count).createCell(0).getStringCellValue();
		tabelEnd = tableLabel.replaceAll(sysKey, systemName);
		reportSheet.createRow(27*count).createCell(0).setCellValue(tabelEnd);
	}
	
	public void writeFile(String owner){
		String fileName = "DHMSM_Transition_Estimate_by_" + owner + ".xlsx";
		Utility.writeWorkbook(wb, workingDir + folder + fileName);
	}

}
