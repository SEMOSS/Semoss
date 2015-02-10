package prerna.ui.components.specific.tap;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.poi.specific.FactSheetProcessor;
import prerna.poi.specific.FactSheetWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DHMSMDispositionFactSheetProcessor extends FactSheetProcessor {
	private static final Logger logger = LogManager.getLogger(DHMSMDispositionFactSheetProcessor.class.getName());
	
	private String proposedFutureICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data ?Format ?Frequency ?Protocol ?Recommendation WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ProposedInterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> ?Recommendation;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } } ";
	private String decommissionedFutureICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data ?Format ?Frequency ?Protocol ?Recommendation WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ProposedDecommissionedInterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> ?Recommendation;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } }";
	private String allPresentICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data ?Format ?Frequency ?Protocol WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} {?carries <http://www.w3.org/2000/01/rdf-schema#label> ?label} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } } ORDER BY ?Interface";
	private final String lpiSystemsQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' }} BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private final String lpniSystemsQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N' }} BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private final String highSystemsQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N' }} BINDINGS ?Probability {('High')('Question')}";
	private String referenceRepositoryQuery = "SELECT DISTINCT ?DataObject WHERE{BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?System) BIND(<http://health.mil/ontologies/Concept/SourceType/MigrationReference> AS ?MigrationReference){?MigrationReference <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SourceType>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?DataObjectSource <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObjectSource>;}{?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?DataObjectSource <http://semoss.org/ontologies/Relation/LabeledAs> ?MigrationReference}{?System <http://semoss.org/ontologies/Relation/Designated> ?DataObjectSource}{?DataObjectSource <http://semoss.org/ontologies/Relation/Delivers> ?DataObject}}";
	private String rtmQuery = "SELECT DISTINCT ?RTM_Justification WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/RTM_Justification> ?RTM_Justification ;} }";
	//System and DHMSM Providing BLU Queries
	private String sysNumBLUQuery = "SELECT DISTINCT (COUNT(?blu) AS ?Num_Of_Business_Logic_Units_System_Provides) WHERE{SELECT DISTINCT ?System ?blu WHERE { {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?System) {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System ?provideBLU ?blu}}} GROUP BY ?System";
	private String sysAndDHMSMNumBLUQuery = "SELECT DISTINCT (COUNT(?blu) AS ?Num_Of_Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities) WHERE{SELECT DISTINCT ?System ?blu WHERE { BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?System) BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?task <http://semoss.org/ontologies/Relation/Needs> ?blu}{?System ?provideBLU ?blu}}} GROUP BY ?System";
	
	private final String sysBaseURI = "http://health.mil/ontologies/Concept/System/";
	
	private double percentDataCreatedDHMSMProvide = 0.0, percentBLUCreatedDHMSMProvide = 0.0;
	
	private IEngine HR_Core;
	private IEngine TAP_Cost_Data;
	private IEngine TAP_Site_Data;
	private IEngine FutureDB;
	private IEngine FutureCostDB;
	

	public DHMSMDispositionFactSheetProcessor() throws EngineException{
		TAP_Cost_Data = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(TAP_Cost_Data==null) {
			throw new EngineException("TAP_Cost_Data database not found");
		}
		HR_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		if(HR_Core==null) {
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
	
	
	public boolean retrieveDatabases() {
		boolean success = true;
		try{
			HR_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
			if(HR_Core==null)
				throw new EngineException("Database not found");
		} catch(EngineException e) {
			Utility.showError("Could not find necessary database: HR_Core. Cannot generate report.");
			return false;
		}
		
		try{
			FutureDB = (IEngine) DIHelper.getInstance().getLocalProp("FutureDB");
			if(FutureDB==null)
				throw new EngineException("Database not found");
		} catch(EngineException e) {
			Utility.showError("Could not find necessary database: FutureDB. Cannot generate report.");
			return false;
		}
		return success;
	}
	
	
	@Override
	public void generateReports() {	
		boolean dbcheck = retrieveDatabases();
		if (!dbcheck) {
			return;
		}
		//make this the query to return all High, LPNI, and LPI systems
		ArrayList<String> systemList = new ArrayList<String>();
		ArrayList<String> lpiSystemList = createSystemList(lpiSystemsQuery);
		ArrayList<String> lpniSystemList = createSystemList(lpiSystemsQuery);
		ArrayList<String> highSystemList = createSystemList(lpiSystemsQuery);
		systemList.addAll(lpiSystemList);
		systemList.addAll(lpniSystemList);
		systemList.addAll(highSystemList);

		boolean shouldStart = true;
		for (int i=0; i<systemList.size(); i++) {
			String systemName = systemList.get(i);
			if(systemName.equals("LDM"))
				shouldStart = true;
			if(shouldStart)
			{
				generateSystemReport(systemName, false);
			}
		}		
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\FactSheets\\");		
	}
	
	/**
	 * Processes and stores the fact sheet queries and calls the report writer for each system in a list of the Services Systems
	 * @param systemName	String containing the system name to produce a fact sheet
	 */
	@Override
	public void generateSystemReport(String systemName, boolean showMessage) {
		boolean dbcheck = retrieveDatabases();
		if (!dbcheck) {
			return;
		}		

		Hashtable queryResults = processQueries(systemName);
		ArrayList serviceResults = (ArrayList) queryResults.get(ConstantsTAP.PPI_QUERY);
		String service = null;
		for (int i = 0; i < serviceResults.size(); i++) {	
			ArrayList ppi = (ArrayList) serviceResults.get(i);
			for(int j = 0; j < ppi.size(); j++) {
				service = (String) ppi.get(j);
			}
		}
		writeToFile(service, systemName, queryResults);
		if (showMessage) {
			Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\FactSheets\\");
		}
	}
	
	/**
	 * Contains all the queries required to return the necessary data for the DHMSM Disposition fact sheet reports
	 * @param systemName	String containing the system name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	@Override
	public Hashtable processQueries(String systemName) {
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		systemDescriptionQuery = systemDescriptionQuery.replaceAll("AHLTA", systemName);
		ppiQuery = ppiQuery.replaceAll("ASIMS", systemName);
		systemPOCQuery = systemPOCQuery.replaceAll("AHLTA", systemName);
		siteListQuery = siteListQuery.replaceAll("AHLTA", systemName);
		budgetQuery = budgetQuery.replaceAll("APEQS", systemName);
		sysHighlightsQuery = sysHighlightsQuery.replaceAll("ASIMS", systemName);
		interfacesQuery = interfacesQuery.replaceAll("ASIMS", systemName);
		referenceRepositoryQuery = referenceRepositoryQuery.replaceAll("ABACUS", systemName);
		rtmQuery = rtmQuery.replaceAll("AHLTA", systemName);
		
		
		ArrayList<ArrayList<Object>> siteResultsList = runQuery(tapSiteEngine, siteListQuery);
		ArrayList<ArrayList<Object>> budgetResultsList = runQuery(tapCostEngine, budgetQuery);		
		ArrayList<String> systemDescriptionResultsList = runQuery(tapCoreEngine, systemDescriptionQuery);
		ArrayList<String> systemHighlightsList = runQuery(tapCoreEngine, sysHighlightsQuery);
		ArrayList<String> pocList = runQuery(tapCoreEngine, systemPOCQuery);		
		ArrayList<String> ppiList = runQuery(tapCoreEngine, ppiQuery);
		ArrayList<ArrayList<Object>> interfacesResultsList = runQuery(tapCoreEngine, interfacesQuery);
		ArrayList<String> lpiSystemsList = runQuery(hrCoreEngine, lpiSystemsQuery);  
		ArrayList<String> lpniSystemsList = runQuery(hrCoreEngine, lpniSystemsQuery);
		ArrayList<String> highSystemsList = runQuery(hrCoreEngine, highSystemsQuery);
		ArrayList<String> referenceRepositoryList = runQuery(hrCoreEngine, referenceRepositoryQuery);
		ArrayList<String> rtmList = runQuery(hrCoreEngine, rtmQuery);

		//ArrayList<Integer> capabilitiesSupportedResultsList = dhelp.getNumOfCapabilitiesSupported(systemName);
		
		calculateTransitionPercentageValues(systemName);
				
		returnHash.put(ConstantsTAP.SITE_LIST_QUERY, siteResultsList);
		returnHash.put(ConstantsTAP.BUDGET_QUERY, budgetResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_DESCRIPTION_QUERY, systemDescriptionResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_HIGHLIGHTS_QUERY, systemHighlightsList);
		returnHash.put(ConstantsTAP.POC_QUERY, pocList);		
		returnHash.put(ConstantsTAP.PPI_QUERY, ppiList);
		returnHash.put(ConstantsTAP.LIST_OF_INTERFACES_QUERY, interfacesResultsList);
		//returnHash.put(ConstantsTAP.CAPABILITIES_SUPPORTED_QUERY, capabilitiesSupportedResultsList);
		returnHash.put(ConstantsTAP.LPI_SYSTEMS_QUERY, lpiSystemsList);
		returnHash.put(ConstantsTAP.LPNI_SYSTEMS_QUERY, lpniSystemsList);
		returnHash.put(ConstantsTAP.HIGH_SYSTEMS_QUERY, highSystemsList);
		returnHash.put(ConstantsTAP.REFERENCE_REPOSITORY_QUERY, referenceRepositoryList);
		returnHash.put(ConstantsTAP.RTM_QUERY, rtmList);
		returnHash.put(ConstantsTAP.DHMSM_DATA_PROVIDED_PERCENT, percentDataCreatedDHMSMProvide);
		returnHash.put(ConstantsTAP.DHMSM_BLU_PROVIDED_PERCENT, percentBLUCreatedDHMSMProvide);
		
		return returnHash;	
	}
	
	/**
	 * Create the report file name and location, and call the writer to write the report for the specified system
	 * Create the location for the fact sheet report template
	 * @param service			String containing the service category of the system
	 * @param systemName		String containing the system name to produce the fact sheet
	 * @param systemDataHash 	Hashtable containing the results for the query for the specified system
	 */
	public void writeToFile(String service, String systemName, Hashtable systemDataHash) {
		DHMSMDispositionFactSheetWriter writer = new DHMSMDispositionFactSheetWriter();
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + 
					"Reports" + System.getProperty("file.separator") + "FactSheets" + System.getProperty("file.separator");
		String writeFileName;

		writeFileName = systemName.replaceAll(":", "") + "_Disposition_Fact_Sheet_" + 
				DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";
		
		String fileLoc = workingDir + folder + writeFileName;
		String templateFileName = "System_Disposition_Fact_Sheet_Template.xlsx";
		String templateLoc = workingDir + folder + templateFileName;
		logger.info(fileLoc);
		
		writer.createWorkbook(systemName, templateLoc);
		//For the DHSMSM Transition Report Interface tabs
		String systemQueryURI = "<" + sysBaseURI + systemName + ">"; 
		proposedFutureICDQuery = proposedFutureICDQuery.replace("@SYSTEM@", systemQueryURI);
		decommissionedFutureICDQuery = decommissionedFutureICDQuery.replace("@SYSTEM@", systemQueryURI);
		allPresentICDQuery = allPresentICDQuery.replace("@SYSTEM@", systemQueryURI);
		
		HashMap<String, Object> sysProposedFutureICD = getQueryDataWithHeaders(FutureDB, proposedFutureICDQuery);
		HashMap<String, Object> sysDecommissionedFutureICD = getQueryDataWithHeaders(FutureDB, decommissionedFutureICDQuery);
		HashMap<String, Object> sysPersistentICD = determinePersistentICDs(sysDecommissionedFutureICD);

		ArrayList<Object[]> devICDList = (ArrayList<Object[]>)sysProposedFutureICD.get("data");
		if (devICDList.size() == 0)
			writer.hideWorkSheet("Future Interface Development");
		else {
			writer.writeListSheet("Future Interface Development", sysProposedFutureICD);
		}
		
		ArrayList<Object[]> decomICDList = (ArrayList<Object[]>)sysDecommissionedFutureICD.get("data");
		if (decomICDList.size() == 0)
			writer.hideWorkSheet("Future Interface Decommission");
		else {
			writer.writeListSheet("Future Interface Decommission", sysDecommissionedFutureICD);
		}		
		ArrayList<Object[]> sustainICDList = (ArrayList<Object[]>)sysPersistentICD.get("data");
		if (sustainICDList.size() == 0)
			writer.hideWorkSheet("Future Interface Sustainment");
		else {
			writer.writeListSheet("Future Interface Sustainment", sysPersistentICD);
		}
		
		
		//DHMSM Integration Transition Cost writing
		DHMSMIntegrationTransitionCostWriter costWriter = null;
		String systemURI = sysBaseURI + systemName; 
		try {
			costWriter = new DHMSMIntegrationTransitionCostWriter();
			costWriter.setSysURI(systemURI);
			writer.writeTransitionCosts(costWriter);
		} catch (EngineException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		}
		
		writer.exportFactSheets(fileLoc, systemDataHash, devICDList.size(), decomICDList.size(), sustainICDList.size());
	}
	
	private HashMap<String, Object> getQueryDataWithHeaders(IEngine engine, String query){
		HashMap<String, Object> dataHash = new HashMap<String, Object>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		dataHash.put(DHMSMTransitionUtility.HEADER_KEY, names);

		ArrayList<Object[]> dataToAddArr = new ArrayList<Object[]>();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			Object[] dataRow = new Object[names.length];
			for(int i = 0; i < names.length; i++)
			{
				Object dataElem = sjss.getVar(names[i]);
				if(dataElem.toString().startsWith("\"") && dataElem.toString().endsWith("\""))
				{
					dataElem = dataElem.toString().substring(1, dataElem.toString().length()-1); // remove annoying quotes
				}
				dataRow[i] = dataElem;
			}
			dataToAddArr.add(dataRow);
		}

		dataHash.put(DHMSMTransitionUtility.DATA_KEY, dataToAddArr);

		return dataHash;
	}
	
	private HashMap<String, Object> determinePersistentICDs(HashMap<String, Object> decommissionedFutureICDs) {
		HashMap<String, Object> retHash = new HashMap<String, Object>();
		
		HashMap<String, Object> allICDs = getQueryDataWithHeaders(HR_Core, allPresentICDQuery);
		ArrayList<Object[]> allData = (ArrayList<Object[]>) allICDs.get(DHMSMTransitionUtility.DATA_KEY);
		ArrayList<Object[]> removedData = (ArrayList<Object[]>) decommissionedFutureICDs.get(DHMSMTransitionUtility.DATA_KEY);
		
		Iterator<Object[]> itOverArray = allData.iterator();
		while(itOverArray.hasNext()) {
			Object[] icdRow = itOverArray.next();
			INNER: for(Object[] removeICD : removedData) {
				if(icdRow[2].toString().equals(removeICD[2].toString())) {
					itOverArray.remove();
					break INNER;
				}
			}
		}
		
		retHash.put(DHMSMTransitionUtility.HEADER_KEY, allICDs.get(DHMSMTransitionUtility.HEADER_KEY));
		retHash.put(DHMSMTransitionUtility.DATA_KEY, allData);
		
		return retHash;
	}
	
	public void calculateTransitionPercentageValues(String systemName) {
		sysNumBLUQuery = sysNumBLUQuery.replaceAll("ABACUS", systemName);
		sysAndDHMSMNumBLUQuery = sysAndDHMSMNumBLUQuery.replaceAll("ABACUS", systemName);
		
		ArrayList<String> sysNumBLU = runQuery(hrCoreEngine, sysNumBLUQuery);
		ArrayList<String> sysAndDHMSMBLU = runQuery(hrCoreEngine, sysAndDHMSMNumBLUQuery);
		
		DHMSMHelper dhelp = new DHMSMHelper();
		dhelp.runData(HR_Core);
		
		ArrayList<String> dataSystemSORList = dhelp.getAllDataFromSys(systemName, "C");
		int numOfDataSystemSOR = dataSystemSORList.size();
		ArrayList<String> dataSystemSORAndDHMSMCreateList = dhelp.getDataObjectListSupportedFromSystem(systemName, "C", "C");
		int numOfDataSystemSORAndDHMSMCreate = dataSystemSORAndDHMSMCreateList.size();
				
		double numOfBLUSystemSOR = 0;
		double numOfBLUSystemSORAndDHMSMCreate = 0;

		numOfBLUSystemSOR = sysNumBLU.size() / 1.0;
		numOfBLUSystemSORAndDHMSMCreate = sysAndDHMSMBLU.size() / 1.0;
		
		//Calculate % of BLU that DHMSM will provide for the given system
		if (numOfBLUSystemSOR == 0)
			percentBLUCreatedDHMSMProvide = 0;
		else if (numOfBLUSystemSORAndDHMSMCreate == 0)
			percentBLUCreatedDHMSMProvide = 0;
		else if (numOfBLUSystemSOR == numOfBLUSystemSORAndDHMSMCreate)
			percentBLUCreatedDHMSMProvide = 1.0;
		else {
			double bluPercent = (double) numOfBLUSystemSORAndDHMSMCreate / (double) numOfBLUSystemSOR;
			percentBLUCreatedDHMSMProvide = (bluPercent);
		}
		
		//Calculate % of Data Objects that DHMSM will provide for the given system
		if(numOfDataSystemSOR == 0)
			percentDataCreatedDHMSMProvide = 0;
		else if((numOfDataSystemSOR == numOfDataSystemSORAndDHMSMCreate))
			percentDataCreatedDHMSMProvide = 1.0;
		else if(numOfDataSystemSORAndDHMSMCreate == 0)
			percentDataCreatedDHMSMProvide = 0;
		else {
			double dataPercent = (double)numOfDataSystemSORAndDHMSMCreate/ (double)numOfDataSystemSOR;
			percentDataCreatedDHMSMProvide = (dataPercent);
		}
	}

}
