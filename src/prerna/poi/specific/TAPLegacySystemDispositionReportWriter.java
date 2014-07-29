package prerna.poi.specific;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.specific.tap.IndividualSystemTransitionReport;
import prerna.ui.components.specific.tap.OCONUSMapExporter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class TAPLegacySystemDispositionReportWriter {

	Logger logger = Logger.getLogger(getClass());

	private String sysName;
	private String sysURI;

	//queries to determine type of system being passed in
	private String lpiListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'}} BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private String lpniListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N'}} BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private String hpListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} } BINDINGS ?Probability {('High')('Question')}";

	//query for basic sys information
	private String basicSysInfoQuery = "SELECT DISTINCT (COALESCE(?description,'') AS ?Description) (GROUP_CONCAT(?Owner ; SEPARATOR = ', ') AS ?SysOwner) (COALESCE(?Ato,'') AS ?ATO) WHERE { SELECT DISTINCT ?sys (COALESCE(?des,'') AS ?description) (SUBSTR(STR(?owner),50) AS ?Owner) (COALESCE(SUBSTR(STR(?ato),0,10),'') AS ?Ato) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?sys <http://semoss.org/ontologies/Relation/OwnedBy> ?owner} OPTIONAL{ {?sys <http://semoss.org/ontologies/Relation/Contains/Description> ?des} } OPTIONAL{ {?sys <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ato} } } } GROUP BY ?description ?Ato BINDINGS ?sys {(@BINDING_STRING@)}";

	//queries for modernization activities
	private String sysSWCostQuery = "SELECT ?System (COUNT(?SoftwareVersion) AS ?numProducts) (SUM(COALESCE(?swTotalCost, 0)) AS ?total) WHERE { SELECT DISTINCT ?System ?SoftwareVersion (COALESCE(?unitcost*?Quantity,0) AS ?swTotalCost) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?SoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?SoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>} {?System <http://semoss.org/ontologies/Relation/Consists> ?SoftwareModule} {?SoftwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?SoftwareVersion} {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } } GROUP BY ?System";
	private Hashtable<String, Hashtable<String, Double>> sysSWHash = new Hashtable<String, Hashtable<String, Double>>();
	private String sysHWCostQuery = "SELECT DISTINCT ?System (COUNT(?HardwareVersion) AS ?numProducts) (SUM(COALESCE(?hwTotalCost, 0)) AS ?total) WHERE { SELECT DISTINCT ?System ?HardwareVersion (COALESCE(?unitcost*?Quantity,0) AS ?hwTotalCost) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?HardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?HardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>} {?System ?Has ?HardwareModule} {?HardwareModule ?TypeOf ?HardwareVersion } {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } } GROUP BY ?System";
	private Hashtable<String, Hashtable<String, Double>> sysHWHash = new Hashtable<String, Hashtable<String, Double>>();
	private String sysInterfaceModCostQuery = "SELECT DISTINCT ?system ?cost WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?system <http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost> ?cost} }";
	private Hashtable<String, Double> sysInterfaceModHash = new Hashtable<String, Double>();

	//queries for modernization timeline
	private String systemBudgetQuery = "SELECT DISTINCT ?system ?year ?cost WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systembudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?system <http://semoss.org/ontologies/Relation/Has> ?systembudget} {?year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> } {?systembudget <http://semoss.org/ontologies/Relation/OccursIn> ?year} {?systembudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?cost} }";
	private Hashtable<String, Hashtable<String, Object>> sysBudgetHash = new Hashtable<String, Hashtable<String, Object>>(); //cannot use Double because when no info received it is stored as "No_cost_info_received."

	private String bindingsKey = "@BINDING_STRING@";
	private String numProductKey = "numProducts";
	private String updateCostKey = "upgradeCostKey";

	private IEngine HR_Core;
	private IEngine TAP_Portfolio;

	private XSSFWorkbook wb;
	private XSSFSheet reportSheet;
	
	private double atoCost = 150000;
	private int atoYear;
	private int atoRenewalYear;
	
	private IndividualSystemTransitionReport report;
	
	public TAPLegacySystemDispositionReportWriter(String sysURI) throws EngineException {
		this.sysURI = sysURI;
		this.sysName = Utility.getInstanceName(sysURI.replace(">", "").replace("<", ""));

		HR_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		TAP_Portfolio = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Portfolio");

		if(HR_Core == null) {
			throw new EngineException("Could not find HR_Core database.\nPlease load the appropriate database to produce report");
		}
		if(TAP_Portfolio == null){
			throw new EngineException("Could not find TAP_Portfolio database.\nPlease load the appropriate database to produce report");
		}
	}

	public void writeToExcel() throws FileReaderException
	{

		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = "\\export\\Reports\\";
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
		writeBasicSysDescription();
		writeTransitionAnalysis();
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
		
		try {
			FileInputStream inputStream = new FileInputStream(imageLoc); //FileInputStream obtains input bytes from the image file
			byte[] bytes = IOUtils.toByteArray(inputStream); //Get the contents of an InputStream as a byte[].
			int pictureIdx = wb.addPicture(bytes, XSSFWorkbook.PICTURE_TYPE_PNG); //Adds a picture to the workbook
			inputStream.close();

			CreationHelper helper = wb.getCreationHelper(); //Returns an object that handles instantiating concrete classes
			XSSFClientAnchor anchor = (XSSFClientAnchor) helper.createClientAnchor(); //Create an anchor that is attached to the worksheet
			anchor.setCol1(1); //select where to put the picture
			anchor.setRow1(24);
			anchor.setDx1((short)5*36000);
			anchor.setDy1((short)5*36000);

			Drawing drawing = reportSheet.createDrawingPatriarch(); //Creates the top-level drawing patriarch, specify sheet to draw on
			Picture pict = drawing.createPicture(anchor, pictureIdx); //Creates a picture
			pict.resize(0.6196);
		} catch (FileNotFoundException e){
			logger.info("CONUS Map image not found for this system");
		} catch (IOException e) {
			logger.info("CONUS Map image not found for this system");
		}
	}

	private void writeInterfaceSummary() {
		if(report == null) {
			report = new IndividualSystemTransitionReport();
		}
		report.setSystemName(sysName);
		//pass in empty hashmap and method automatically creates the required hash
		HashMap<String, Object> barHash = new HashMap<String, Object>();
		barHash = report.createInterfaceBarChart(barHash);
		
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
		reportSheet.getRow(16).getCell(9).setCellValue(hwswCost);
		reportSheet.getRow(17).getCell(9).setCellValue(interfaceModCost);

		generateSysBudgetData();
		Hashtable<String, Object> innerHash = sysBudgetHash.get(sysName);
		String[] yearData = new String[]{"FY15", "FY16", "FY17", "FY18", "FY19"};

		int offSet = 9;
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
		
		
		ArrayList<Integer> atoDateList = new ArrayList<Integer>();
		atoDateList.add(atoYear);
		atoDateList.add(atoRenewalYear);
		for(Integer date : atoDateList) {
			if(date == 2015){
				reportSheet.getRow(18).getCell(9).setCellValue(atoCost);
			} else if(date == 2016) {
				reportSheet.getRow(18).getCell(10).setCellValue(atoCost);
			} else if(date == 2017) {
				reportSheet.getRow(18).getCell(11).setCellValue(atoCost);
			} else if(date == 2018) {
				reportSheet.getRow(18).getCell(12).setCellValue(atoCost);
			} else if(date == 2019) {
				reportSheet.getRow(18).getCell(13).setCellValue(atoCost);
			}
		}

		for(int i = 0; i < 5; i++){
			double costHWSW = reportSheet.getRow(16).getCell(9+i).getNumericCellValue();
			double costIntMod = reportSheet.getRow(17).getCell(9+i).getNumericCellValue();
			double costDIACAP = reportSheet.getRow(18).getCell(9+i).getNumericCellValue();
			reportSheet.getRow(15).getCell(9+i).setCellValue(costHWSW + costIntMod + costDIACAP);
		}
	}

	private void generateSysBudgetData() {
		if(sysBudgetHash.isEmpty()) {
			SesameJenaSelectWrapper sjsw = processQuery(TAP_Portfolio, systemBudgetQuery);
			String[] varNames = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(varNames[0]).toString();
				String year = sjss.getVar(varNames[1]).toString();
				Object cost = sjss.getVar(varNames[2]).toString();
				if(sysBudgetHash.get(sysName) == null) {
					Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
					innerHash.put(year, cost);
					sysBudgetHash.put(sysName, innerHash);
				} else {
					Hashtable<String, Object> innerHash = sysBudgetHash.get(sysName);
					innerHash.put(year, cost);
				}
			}
		}
	}

	private ArrayList<Double> writeModernizationActivities() {
		generateModernizationActivitiesData();
		Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
		innerHash = sysHWHash.get(sysName);
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

		innerHash = sysHWHash.get(sysName);
		Double swNumUpdates = (double) 0;
		Double swCost = (double) 0;
		if(innerHash != null){
			swNumUpdates = sysHWHash.get(sysName).get(numProductKey);
			if(swNumUpdates == null) {
				swNumUpdates = (double) 0;
			}
			swCost = sysHWHash.get(sysName).get(updateCostKey);
			if(swCost == null) {
				swCost = (double) 0;
			}
		}

		Double interfaceModCost = sysInterfaceModHash.get(sysName);
		if(interfaceModCost == null) {
			interfaceModCost = (double) 0;
		}

		reportSheet.getRow(14).getCell(5).setCellValue(hwCost);
		reportSheet.getRow(15).getCell(3).setCellValue(swNumUpdates);
		reportSheet.getRow(15).getCell(5).setCellValue(swCost);
		reportSheet.getRow(16).getCell(5).setCellValue(interfaceModCost);

		int counter = 0;
		if(atoYear >= 2015 && atoYear <= 2019) {
			counter++;
		}
		if(atoRenewalYear >= 2015 && atoRenewalYear <= 2019) {
			counter++;
		}
		double totalAtoCost = counter * atoCost;
		reportSheet.getRow(17).getCell(5).setCellValue(totalAtoCost);
		
		//sum up totals
		reportSheet.getRow(18).getCell(5).setCellValue(swCost + hwCost + interfaceModCost + totalAtoCost);
		
		// pass cost information over to reduce access of hashtables
		ArrayList<Double> costInfo = new ArrayList<Double>();
		costInfo.add(swCost + hwCost);
		costInfo.add(interfaceModCost);

		return costInfo;
	}

	private void generateModernizationActivitiesData() {
		if(sysSWHash.isEmpty()){
			SesameJenaSelectWrapper sjsw = processQuery(HR_Core, sysSWCostQuery);
			String[] varNames = sjsw.getVariables();
			while(sjsw.hasNext()){
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(varNames[0]).toString();
				Double numProducts = (Double) sjss.getVar(varNames[1]);
				Double cost = (Double) sjss.getVar(varNames[2]);
				Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
				innerHash.put(numProductKey, numProducts);
				innerHash.put(updateCostKey, cost);
				sysSWHash.put(sysName, innerHash);
			}
		}
		if(sysHWHash.isEmpty()){
			SesameJenaSelectWrapper sjsw = processQuery(HR_Core, sysHWCostQuery);
			String[] varNames = sjsw.getVariables();
			while(sjsw.hasNext()){
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(varNames[0]).toString();
				Double numProducts = (Double) sjss.getVar(varNames[1]);
				Double cost = (Double) sjss.getVar(varNames[2]);
				Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
				innerHash.put(numProductKey, numProducts);
				innerHash.put(updateCostKey, cost);
				sysHWHash.put(sysName, innerHash);
			}
		}
		if(sysInterfaceModHash.isEmpty()){
			SesameJenaSelectWrapper sjsw = processQuery(HR_Core, sysInterfaceModCostQuery);
			String[] varNames = sjsw.getVariables();
			while(sjsw.hasNext()){
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(varNames[0]).toString();
				Double cost = (Double) sjss.getVar(varNames[1]);
				sysInterfaceModHash.put(sysName, cost);
			}
		}
	}
	
	private void writeTransitionAnalysis() {
		String lpiDescription = "LPI systems were designated by the Functional Advisory Council (FAC) as having a low probability of being replaced by the DHMSM EHR solution and were also designated as requiring integration with DHMSM in order to support data exchange.";
		String lpniDescription = "LPNI systems were designated by the Functional Advisory Council (FAC) as having a low probability of being replaced by the DHMSM EHR solution and were also designated as not requiring integration with DHMSM.";
		String hpDescription = "HP systems were designated by the Functional Advisory Council (FAC) as having a high probability of being replaced by the DHMSM solution.";

		HashSet<String> lpiList = processSingleUniqueReturnQuery(HR_Core, lpiListQuery);
		if(lpiList.contains(sysName)){
			reportSheet.getRow(8).getCell(3).setCellValue("Low Probability with Integration (LPI)");
			reportSheet.getRow(8).getCell(6).setCellValue(lpiDescription);
		} else {
			HashSet<String> lpniList = processSingleUniqueReturnQuery(HR_Core, lpniListQuery);
			if(lpniList.contains(sysName)) {
				reportSheet.getRow(8).getCell(3).setCellValue("Low Probability without Integration (LPNI)");
				reportSheet.getRow(8).getCell(6).setCellValue(lpniDescription);
			} else {
				HashSet<String> hpList = processSingleUniqueReturnQuery(HR_Core, hpListQuery);
				if(hpList.contains(sysName)) {
					reportSheet.getRow(8).getCell(3).setCellValue("High Probability (HP)");
					reportSheet.getRow(8).getCell(6).setCellValue(hpDescription);
				}
			}
		}
	}

	private HashSet<String> processSingleUniqueReturnQuery(IEngine engine, String query) {
		HashSet<String> retList = new HashSet<String>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String entity = sjss.getVar(varName[0]).toString();
			retList.add(entity);
		}

		return retList;
	}

	private void writeBasicSysDescription() {
		basicSysInfoQuery = basicSysInfoQuery.replace(bindingsKey, sysURI);
		SesameJenaSelectWrapper sjsw = processQuery(HR_Core, basicSysInfoQuery);
		String[] varNames = sjsw.getVariables();
		// output binds to a single system - query should only return one line
		// if query returns nothing, never executed
		String description = "NA";
		String sysOwner = "NA";
		String ato = "";
		String atoRenewalDate = "REQUIRES IMMEDIATE ACCREDITATION";
		//String atoRenewal = "";
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			description = sjss.getVar(varNames[0]).toString();
			sysOwner = sjss.getVar(varNames[1]).toString();
			ato = sjss.getVar(varNames[2]).toString();
		}

		if(description == null){
			description = "NA";
		}
		if(sysOwner == null){
			sysOwner = "NA";
		}
		if(ato == null || ato.equals("")){
			// no ato information -> assume ato date is year 2015
			ato = "NA";
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
					if(c.get(Calendar.MONTH) < 10) {
						month = "0" + c.get(Calendar.MONTH);
					} else {
						month = c.get(Calendar.MONTH) + "";
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
		reportSheet.getRow(3).getCell(1).setCellValue(sysName);
		reportSheet.getRow(3).getCell(3).setCellValue(description.replace("_", " "));
		reportSheet.getRow(4).getCell(3).setCellValue(sysOwner);
		reportSheet.getRow(5).getCell(3).setCellValue(ato);
		reportSheet.getRow(5).getCell(10).setCellValue(atoRenewalDate);
	}

	private SesameJenaSelectWrapper processQuery(IEngine engine, String query){
		logger.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}
}
