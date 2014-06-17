package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class IndividualLPISystemTransitionReport extends AbstractRDFPlaySheet{

	// list of queries to run
	private String sysInfoQuery = "SELECT DISTINCT ?System (COALESCE(?GT, 'Garrison') AS ?GarrisonTheater) (IF(BOUND(?MU),'Yes','No') AS ?MHS_Specific) (COALESCE(IF(DATATYPE(?TC) = xsd:double,?TC,-1),-1) AS ?Transaction_Count) (COALESCE(SUBSTR(STR(?ATO),0,10),'NA') AS ?ATO_Date) (COALESCE(SUBSTR(STR(?ES),0,10),'NA') AS ?End_Of_Support) (COALESCE(IF(DATATYPE(?NU) = xsd:double,?NU,-1),-1) AS ?Num_Users) WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> ?MU}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?TC}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ATO}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?ES}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?NU}} }";
	private String sysDataConsumeProvideQuery = "SELECT DISTINCT ?System ?Data ?ProvideOrConsume (GROUP_CONCAT(?Ser; SEPARATOR = ', ') AS ?Services) WHERE { {SELECT DISTINCT ?System ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) WHERE {BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} {?System ?provideData ?Data} FILTER( !regex(str(?crm),'R')) BIND('Provide' AS ?ProvideOrConsume)}} UNION {SELECT DISTINCT ?System ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) WHERE {BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} OPTIONAL {{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data}  FILTER(!BOUND(?icd2)) BIND('Provide' AS ?ProvideOrConsume)}} UNION {SELECT DISTINCT ?System ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) WHERE {BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>}   {?otherSystem <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} BIND('Consume' AS ?ProvideOrConsume)}} } GROUP BY ?System ?Data ?ProvideOrConsume ORDER BY ?System";
	private String sysSORDataWithDHMSMQuery = "SELECT DISTINCT ?System ?Data ?DHMSM_SOR WHERE { {SELECT DISTINCT ?System ?Data (IF(BOUND(?Needs),'Yes','No') AS ?DHMSM_SOR) WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>}  {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} FILTER( !regex(str(?crm),'R')) OPTIONAL { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} } } } UNION {SELECT DISTINCT ?System ?Data (IF(BOUND(?Needs),'Yes','No') AS ?DHMSM_SOR) WHERE {BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} OPTIONAL { {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } FILTER(!BOUND(?icd2)) OPTIONAL { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} } }} } ORDER BY ?System";
	private String sysSORDataWithDHMSMCapQuery = "SELECT DISTINCT ?Capability ?Data ?System (GROUP_CONCAT(DISTINCT ?Comment ; Separator = ' and ') AS ?Comments) WHERE { { SELECT DISTINCT ?Capability ?Data ?System ?Comment ?DHMSMcrm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> }{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?SystemProvideData_CRM} FILTER( !regex(str(?SystemProvideData_CRM),'R')) {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} {?Task ?Needs ?Data} BIND('ICD validated through downstream interfaces' AS ?Comment) { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability.} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } GROUP BY ?Data } BIND('R' AS ?DHMSMcrm) } } UNION { SELECT DISTINCT ?Capability ?Data ?System ?Comment ?DHMSMcrm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} {?Task ?Needs ?Data} OPTIONAL { {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } {?System <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data } FILTER(!BOUND(?icd2)) BIND('ICD implied through downstream and no upstream interfaces' AS ?Comment) { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability.} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } GROUP BY ?Data } BIND('R' AS ?DHMSMcrm) } } } GROUP BY ?Capability ?Data ?System ORDER BY ?Capability ?Data ?System";

	private String headerKey = "headers";
	private String dataKey = "data";

	private IEngine hr_Core;
	private IEngine TAP_Cost_Data;
	
	Logger logger = Logger.getLogger(getClass());

	@Override
	public void createData() {
		try{
			hr_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
			TAP_Cost_Data = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		} catch(Exception e) {
			e.printStackTrace();
			Utility.showError("Could not find necessary databases:\nHR_Core, TAP_Cost_Data");
		}

		String systemURI = this.query;

		sysInfoQuery = sysInfoQuery.replace("@SYSTEM@", systemURI);
		logger.info("SysInfoQuery: " + sysInfoQuery);
		sysDataConsumeProvideQuery = sysDataConsumeProvideQuery.replace("@SYSTEM@", systemURI);
		logger.info("SysDataConsumeProvideQuery: " + sysDataConsumeProvideQuery);
		sysSORDataWithDHMSMQuery = sysSORDataWithDHMSMQuery.replace("@SYSTEM@", systemURI);
		logger.info("SysSORDataWithDHMSMQuery: " + sysSORDataWithDHMSMQuery);
		sysSORDataWithDHMSMCapQuery = sysSORDataWithDHMSMCapQuery.replace("@SYSTEM@", systemURI);
		logger.info("SysSORDataWithDHMSMCapQuery: " + sysSORDataWithDHMSMCapQuery);

		HashMap<String, Object> sysInfoHash = getQueryData(hr_Core, sysInfoQuery);
		HashMap<String, Object> sysDataConsumeProvideHash = getQueryData(hr_Core, sysDataConsumeProvideQuery);
		HashMap<String, Object> sysSORDataWithDHMSMHash = getQueryData(hr_Core, sysSORDataWithDHMSMQuery);
		HashMap<String, Object> sysSORDataWithDHMSMCapHash = getQueryData(hr_Core, sysSORDataWithDHMSMCapQuery);

		System.out.println("Done");
	}

	public HashMap<String, Object> getQueryData(IEngine engine, String query){
		HashMap<String, Object> dataHash = new HashMap<String, Object>();
		
		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] names = sjsw.getVariables();
		dataHash.put(headerKey, names);

		ArrayList<Object[]> dataToAddArr = new ArrayList<Object[]>();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			Object[] dataRow = new Object[names.length];
			for(int i = 0; i < names.length; i++)
			{
				String dataElem = sjss.getVar(names[i]).toString();
				if(dataElem.toString().startsWith("\"") && dataElem.toString().endsWith("\""))
				{
					dataElem = dataElem.toString().substring(1, dataElem.toString().length()-1); // remove annoying quotes
				}
				dataRow[i] = dataElem;
			}
			dataToAddArr.add(dataRow);
		}

		dataHash.put(dataKey, dataToAddArr);

		return dataHash;
	}

	public SesameJenaSelectWrapper processQuery(IEngine engine, String query){
		logger.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}

	@Override
	public void refineView() {
		// TODO Auto-generated method stub
	}

	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
	}

}
