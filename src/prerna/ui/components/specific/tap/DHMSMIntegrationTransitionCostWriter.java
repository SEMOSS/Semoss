package prerna.ui.components.specific.tap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

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
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DHMSMIntegrationTransitionCostWriter {
	
	private static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationTransitionCostWriter.class.getName());
	private final String sysProposedICDQuery = "SELECT DISTINCT ?System ?Data ?ICD ?Type WHERE { {SELECT DISTINCT ?System ?ICD ?Data ?Type WHERE { BIND('Provider' AS ?Type)  FILTER(?System != <http://health.mil/ontologies/Concept/System/DHMSM>) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ProposedInterfaceControlDocument>} {?System <http://semoss.org/ontologies/Relation/Provide> ?ICD} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?ICD ?Payload ?Data} } } UNION { SELECT DISTINCT ?System ?ICD ?Data ?Type WHERE{ BIND('Consumer' AS ?Type) FILTER(?System != <http://health.mil/ontologies/Concept/System/DHMSM>) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ProposedInterfaceControlDocument>} {?ICD <http://semoss.org/ontologies/Relation/Consume> ?System} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?ICD ?Payload ?Data} } } }";
	
	private HashMap<String, ArrayList<String[]>> sysICDDataList = new HashMap<String, ArrayList<String[]>>();
	
	public HashMap<String, Double> consolidatedSysCostInfo = new HashMap<String, Double>();

	private IEngine hrCore;
	private IEngine TAP_Cost_Data;
	private IEngine FutureDB;
	private IEngine FutureCostDB;
	
	private String sysURI;
	private String systemName;
	private double costPerHr = 150.0;
	private double sumHWSWCost;
	int[] atoDateList = new int[2];

	private TAPLegacySystemDispositionReportWriter diacapReport;
	private LPInterfaceProcessor processor;
	
	private final String[] phases = new String[]{"Requirements","Design","Develop","Test","Deploy"};
	private final String[] tags1 = new String[]{"Consume", "Provide"};
	private final String[] tags = new String[]{"Consumer", "Provider"};
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
		FutureDB = (IEngine) DIHelper.getInstance().getLocalProp("FutureDB");
		if(FutureDB==null) {
			throw new EngineException("FutureDB database not found");
		}
		FutureCostDB = (IEngine) DIHelper.getInstance().getLocalProp("FutureCostDB");
		if(FutureCostDB==null) {
				throw new EngineException("FutureCostDB database not found");
		}
	}
	
	public void setCostPerHr(double costPerHr) {
		this.costPerHr = costPerHr;
	}
	
	public void setSysURI(String sysURI){
		this.sysURI = sysURI;
		this.systemName = Utility.getInstanceName(sysURI);
	}
	
	public void calculateValuesForReport() throws EngineException 
	{
		if(processor == null) {
			processor = new LPInterfaceProcessor();
			processor.getCostInfoAtPhaseLevel(TAP_Cost_Data);
		}
		processor.setSysCostInfo(new HashMap<Integer, HashMap<String, Double>>());
		processor.setConsolidatedSysCostInfo(new HashMap<String, Double>());
		consolidatedSysCostInfo.clear();
		generateAllCost();
		getCostForSys(systemName);
		
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
	
	private void generateAllCost() {
		if(sysICDDataList.isEmpty()) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(FutureDB, sysProposedICDQuery);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(names[0]).toString();
				String dataName = sjss.getVar(names[1]).toString();
				String icdName = sjss.getVar(names[2]).toString();
				String type = sjss.getVar(names[3]).toString();
				ArrayList<String[]> icdList;
				if(sysICDDataList.containsKey(sysName)) {
					icdList = sysICDDataList.get(sysName);
					icdList.add(new String[]{icdName, dataName, type});
				} else {
					icdList = new ArrayList<String[]>();
					icdList.add(new String[]{icdName, dataName, type});
					sysICDDataList.put(sysName, icdList);
				}
			}
		}
	}
	
	private void getCostForSys(String systemName) {
		ArrayList<String[]> icdDataList = sysICDDataList.get(systemName);
		
		HashSet<String> serProvideList = new HashSet<String>();
		HashSet<String> serConsumeList = new HashSet<String>();
		if(icdDataList != null) {
			int size = icdDataList.size();
			int i = 0;
			for(; i < size; i++) {
				String[] icdArr = icdDataList.get(i);
				String dataObject = icdArr[1];
				String tag = icdArr[2];
				boolean includeGenericCost = false;
				if(tag.contains("Provide")) {
					includeGenericCost = true;
					processor.calculateCost(dataObject, systemName, tag, includeGenericCost, serProvideList, i);
				} else {
					processor.calculateCost(dataObject, systemName, tag, includeGenericCost, serConsumeList, i);
				}
			}
		}
		processor.consolodateCostHash();
		consolidatedSysCostInfo = processor.getConsolidatedSysCostInfo();
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
				String key1 = tags1[i].concat("+").concat(phases[j]);
				XSSFRow rowToWriteOn = reportSheet.getRow(rowToOutput);
				if(consolidatedSysCostInfo.containsKey(key)) {
					Double cost = consolidatedSysCostInfo.get(key);
					if(cost == null) {
						cost = (double) 0;
					} else {
						cost *= costPerHr;
					}
					totalCost[i] += cost;
					XSSFCell cellToWriteOn = rowToWriteOn.getCell(2);
					cellToWriteOn.setCellValue(Math.round(cost));
				} else if(consolidatedSysCostInfo.containsKey(key1)) {
					Double cost = consolidatedSysCostInfo.get(key1);
					if(cost == null) {
						cost = (double) 0;
					} else {
						cost *= costPerHr;
					}
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
	
	public String getSystemName(){
		return systemName;
	}
}
