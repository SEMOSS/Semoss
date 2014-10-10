package prerna.ui.components.specific.tap;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.FileReaderException;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import thredds.wcs.v1_1_0.Request.Format;

public class DHMSMIntegrationTransitionBySystemOwnerWriter {
	
	//logger
	private static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationTransitionBySystemOwnerWriter.class.getName());
	
	private XSSFWorkbook wb;
	private String workingDir;
	private String folder;
	private String templateName;
	
	//workbook management for iterating over multiple systems
	
	//constants
	private final String[] phases = new String[]{"Requirements","Design","Develop","Test","Deploy", "Sustainment", "Training"};
	private final String[] tags = new String[]{"Consumer", "Provider"};
	private final double sustainmentFactor = 0.18;
	private final double trainingFactor = 0.15;
	private final double inflation = 0.018;
	
	private int[] atoDateList;
	private double costPerHr;
	private double atoCost;
	private double sumHWSWCost;
	private String sysKey;
	private String systemName;
	private HashMap<String, Double> consolidatedSysCostInfo;
	
	private final int OFFSET = 19;
	//TODO need to finalize template to decide start row
	private final int START_ROW = 1;
	
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
		// calculate some useful row start indices
		int startRow = START_ROW + count*OFFSET;
		int providerStart = startRow + phases.length + 1;
		int hwswRow = providerStart + phases.length + 1;
		int diacapRow = hwswRow + 1;
		int totalSysRow = diacapRow + 1;
		
		//output sysName 
		XSSFCell sysName = reportSheet.createRow(startRow).createCell(0);
		sysName.setCellValue(systemName);
		sysName.setCellStyle(setBoldFont(sysName));
		// CONSUMER INFORMATION
		// consumer information starts on same row as startRow
		// output consume tag
		XSSFCell consumerCell = reportSheet.getRow(startRow).createCell(1);
		consumerCell.setCellValue("Consumer");
		consumerCell.setCellStyle(setBoldFont(consumerCell));
		// output phases for consumer tag
		// note: getRow because this row has already been created
		reportSheet.getRow(startRow).createCell(2).setCellValue(phases[0]);
		// note: createRow since all the other rows have not been created yet
		for(int i = 1; i < phases.length; i++) {
			reportSheet.createRow(startRow+i).createCell(2).setCellValue(phases[i]);
		}
		// consumer total row
		XSSFCell consumerTotal = reportSheet.createRow(startRow + phases.length).createCell(1);
		consumerTotal.setCellValue("Consumer Total");
		consumerTotal.setCellStyle(setBoldFont(consumerTotal));
		
		// REPEAT FOR PROVIDER INFORMATION
		// provider information start row
		XSSFCell providerCell = reportSheet.createRow(providerStart).createCell(1);
		providerCell.setCellValue("Provider");
		providerCell.setCellStyle(setBoldFont(consumerCell));
		// output phases for provider tag
		// note: getRow because this row has already been created
		reportSheet.getRow(providerStart).createCell(2).setCellValue(phases[0]);
		// note: createRow since all the other rows have not been created yet
		for(int i = 1; i < phases.length; i++) {
			reportSheet.createRow(providerStart + i).createCell(2).setCellValue(phases[i]);
		}
		// provider total row
		XSSFCell providerTotal = reportSheet.createRow(providerStart + phases.length).createCell(1);
		providerTotal.setCellValue("Provider Total");
		providerTotal.setCellStyle(setBoldFont(providerTotal));
		
		// HWSW Information
		XSSFCell hwswInfo = reportSheet.createRow(hwswRow).createCell(1);
		hwswInfo.setCellValue("Total Hardware / Software Costs");
		hwswInfo.setCellStyle(setBoldFont(hwswInfo));
		
		// DIACAP Information
		XSSFCell diacapCell = reportSheet.createRow(diacapRow).createCell(1);
		diacapCell.setCellValue("Total DIACAP Costs");
		diacapCell.setCellStyle(setBoldFont(diacapCell));
		
		//Total System Row
		XSSFCell totalSystem = reportSheet.createRow(totalSysRow).createCell(0);
		totalSystem.setCellValue(systemName + " Total");
		totalSystem.setCellStyle(setBoldFont(totalSystem));
		
		// loop through all fields that will contain cost and make them accounting and set cost to 0
		for(int i = 3; i < 9; i++) {
			for(int j = startRow; j < startRow + OFFSET; j++) {
				XSSFCell cell = reportSheet.getRow(j).createCell(i);
				cell.setCellStyle(accountingStyle(cell));
				cell.setCellValue(0.0);
			}
		}
		
		
		// merge cells for formatting
		// merge systemName column
//		XSSFCell mergedCell = null;
		mergeCells(reportSheet, startRow, startRow + OFFSET - 2, 0, 0);
//		mergedCell = reportSheet.getRow(startRow).getCell(0);
//		mergedCell.setCellStyle(outlineCell(mergedCell));
		// merge consume tag column
		mergeCells(reportSheet, startRow, startRow + phases.length - 1, 1, 1);
//		mergedCell = reportSheet.getRow(startRow).getCell(1);
//		mergedCell.setCellStyle(outlineCell(mergedCell));
		// merge provider tag column
		mergeCells(reportSheet, providerStart, providerStart + phases.length - 1, 1, 1);
//		mergedCell = reportSheet.getRow(providerStart).getCell(1);
//		mergedCell.setCellStyle(outlineCell(mergedCell));
		// merge consumer total row
		mergeCells(reportSheet, startRow + phases.length, startRow + phases.length, 1, 2);
//		mergedCell = reportSheet.getRow(startRow + phases.length).getCell(1);
//		mergedCell.setCellStyle(outlineCell(mergedCell));
		// merge provider total row
		mergeCells(reportSheet, providerStart + phases.length, providerStart + phases.length, 1, 2);
//		mergedCell = reportSheet.getRow(providerStart + phases.length).getCell(1);
//		mergedCell.setCellStyle(outlineCell(mergedCell));
		// merge hw/sw row
		mergeCells(reportSheet, hwswRow, hwswRow, 1, 2);
//		mergedCell = reportSheet.getRow(hwswRow).getCell(1);
//		mergedCell.setCellStyle(outlineCell(mergedCell));
		// merge diacap row
		mergeCells(reportSheet, diacapRow, diacapRow, 1, 2);
//		mergedCell = reportSheet.getRow(diacapRow).getCell(1);
//		mergedCell.setCellStyle(outlineCell(mergedCell));
		// merge sys total row
		mergeCells(reportSheet, totalSysRow , totalSysRow, 0, 2);
//		mergedCell = reportSheet.getRow(totalSysRow).getCell(0);
//		mergedCell.setCellStyle(outlineCell(mergedCell));
		
		int i;
		int j;
		int numTags = tags.length;
		int numPhases = phases.length-2;//Sustainment and Training need to be calculated differently
		int rowToOutput = START_ROW + count*OFFSET;

		
		double[] totalCost = new double[2];
		for(i = 0; i < numTags; i++) {
			if(tags[i].contains("Provider")){
				rowToOutput = 9 + count * OFFSET;
			}
			for(j = 0; j < numPhases; j++) {
				String key = tags[i].concat("+").concat(phases[j]);
				XSSFRow rowToWriteOn = reportSheet.getRow(rowToOutput);
				if(consolidatedSysCostInfo.containsKey(key)) {
					double cost = consolidatedSysCostInfo.get(key)*costPerHr;
					totalCost[i] += cost;
					XSSFCell cellToWriteOn = rowToWriteOn.getCell(3);
					cellToWriteOn.setCellValue(Math.round(cost));
				}
				rowToOutput++;
			}
		}

		double consumerTraining = totalCost[0]*trainingFactor;
		reportSheet.getRow(7 + count * OFFSET).getCell(3).setCellValue(Math.round(consumerTraining));
		double providerTraining = totalCost[1]*trainingFactor;
		reportSheet.getRow(15 + count * OFFSET).getCell(3).setCellValue(Math.round(providerTraining));
		
		for(i = 0; i < 2; i++) {
			if(i == 0) {
				startRow = 6 + count * OFFSET;
			} else {
				startRow = 14 + count * OFFSET;
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
		for(k = 3; k < 8; k++) {
			for(i = 0; i < 2; i++) {
				if(i == 0) {
					startRow = 1 + count * OFFSET;
				} else {
					startRow = 9 + count * OFFSET;
				}
				double sumColumn = 0;
				for(j = 0; j < 7; j++) {
					double val = reportSheet.getRow(startRow+j).getCell(k).getNumericCellValue();
					sumColumn += val;
				}
				reportSheet.getRow(startRow+7).getCell(k).setCellValue(sumColumn);
			}
		}

		
		reportSheet.getRow(16 + count * OFFSET).getCell(3).setCellValue(Math.round(sumHWSWCost));
		//since hwsw cost assumed at FY15, total is equal to value at FY15
		//reportSheet.getRow(17).getCell(8).setCellValue(Math.round(sumHWSWCost));
		
		int numATO = 0;
		if(atoDateList[0] < 2015) {
			atoDateList[0] = atoDateList[1];
			atoDateList[1] += 3; 
		}
		for(Integer date : atoDateList) {
			if(date == 2015){
				reportSheet.getRow(17 + count * OFFSET).getCell(3).setCellValue(atoCost);
				numATO++;
			} else if(date == 2016) {
				reportSheet.getRow(17 + count * OFFSET).getCell(4).setCellValue(atoCost);
				numATO++;
			} else if(date == 2017) {
				reportSheet.getRow(17 + count * OFFSET).getCell(5).setCellValue(atoCost);
				numATO++;
			} else if(date == 2018) {
				reportSheet.getRow(17 + count * OFFSET).getCell(6).setCellValue(atoCost);
				numATO++;
			} else if(date == 2019) {
				reportSheet.getRow(17+ count * OFFSET).getCell(7).setCellValue(atoCost);
				numATO++;
			}
		}
		reportSheet.getRow(16 + count * OFFSET).getCell(7).setCellValue(atoCost * numATO);
		
		for(i = 3; i < 8; i++){
			double consumerCost = reportSheet.getRow(8 + count * OFFSET).getCell(i).getNumericCellValue();
			double providerCost = reportSheet.getRow(16 + count * OFFSET).getCell(i).getNumericCellValue();
			double hwswCost = reportSheet.getRow(17 + count * OFFSET).getCell(i).getNumericCellValue();
			double diacapCost = reportSheet.getRow(18 + count * OFFSET).getCell(i).getNumericCellValue();
			
			reportSheet.getRow(19 + count * OFFSET).getCell(i).setCellValue(consumerCost + providerCost + hwswCost + diacapCost);
		}
		
//		//sum across rows
//		for(k = 0; k < 8; k++) {
//			for(i = 0; i < 2; i++) {
//				if(i == 0) {
//					startRow = 1 + count * OFFSET;
//				} else {
//					startRow = 9 + count * OFFSET;
//				}
//				double sumRow = 0;
//				for(j = 3; j < 7; j++) {
//					double val = reportSheet.getRow(startRow+k).getCell(j).getNumericCellValue();
//					sumRow += val;
//				}
//				reportSheet.getRow(startRow+k).getCell(8).setCellValue(sumRow);
//			}
//		}
		
		//sum across rows
		startRow = 1 + count * OFFSET;
		for(k = 0; k < 19; k++) {
			double sumRow = 0;
			for(j = 3; j < 7; j++) {
				double val = reportSheet.getRow(startRow+k).getCell(j).getNumericCellValue();
				sumRow += val;
			}
			reportSheet.getRow(startRow+k).getCell(8).setCellValue(sumRow);
			
		}
		
//		
//		String header = reportSheet.createRow(0).createCell(0).getStringCellValue();
//		header = header.replaceAll(sysKey, systemName);
//		reportSheet.createRow(0).createCell(0).setCellValue(header);
//		
//		String description = reportSheet.createRow(3).createCell(0).getStringCellValue();
//		description = description.replaceAll(sysKey, systemName);
//		reportSheet.createRow(3).createCell(0).setCellValue(description);
//		
//		String tableLabel = reportSheet.createRow(5 + count * OFFSET).createCell(0).getStringCellValue();
//		tableLabel = tableLabel.replaceAll(sysKey, systemName);
//		reportSheet.createRow(5*count).createCell(0).setCellValue(tableLabel);
//
//		String tabelEnd = reportSheet.createRow(27 + count * OFFSET).createCell(0).getStringCellValue();
//		tabelEnd = tableLabel.replaceAll(sysKey, systemName);
//		reportSheet.createRow(27*count).createCell(0).setCellValue(tabelEnd);
	}
	
	private CellStyle accountingStyle(XSSFCell cell) {
		CellStyle styleCurrencyFormat = cell.getCellStyle();
		styleCurrencyFormat.setDataFormat((short) 0x2a);
//		XSSFFont font = wb.createFont();
//		font.setBold(false);
//		styleCurrencyFormat.setFont(font);
		return styleCurrencyFormat;
	}

	private void mergeCells(XSSFSheet reportSheet, int startRow, int endRow, int startCol, int endCol) {
		reportSheet.addMergedRegion(new CellRangeAddress(startRow, endRow, startCol, endCol));
		XSSFCell cell = reportSheet.getRow(startRow).getCell(startCol);
		CellStyle style = cell.getCellStyle();
		style.setAlignment(CellStyle.ALIGN_LEFT);
		style.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		cell.setCellStyle(style);
	}

	public CellStyle setBoldFont(XSSFCell cell){
		CellStyle style = cell.getCellStyle();
		XSSFFont font = wb.createFont();
		font.setBold(true);
		style.setFont(font);
		
		return style;
	}
	
	public CellStyle outlineCell(XSSFCell cell){
		CellStyle style = cell.getCellStyle();
		style.setBorderBottom(XSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(XSSFCellStyle.BORDER_THIN);
		style.setBorderRight(XSSFCellStyle.BORDER_THIN);
		style.setBorderTop(XSSFCellStyle.BORDER_THIN);
		return style;
	}
	
	
	public void writeFile(String owner){
		String fileName = "DHMSM_Transition_Estimate_by_" + owner + ".xlsx";
		Utility.writeWorkbook(wb, workingDir + folder + fileName);
	}

}
