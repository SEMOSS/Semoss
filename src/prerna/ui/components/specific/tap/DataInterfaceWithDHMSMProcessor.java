/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.specific.BasicReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains the queries and query processing required to gather the information needed to generate the System Info Report
 * Used in conjunction with SystemInfoGenListener
 */
public class DataInterfaceWithDHMSMProcessor {
	static final Logger logger = LogManager.getLogger(DataInterfaceWithDHMSMProcessor.class.getName());
	String hrCoreEngineName = "HR_Core";
	String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	ArrayList<String> headersList = new ArrayList<String>();
	ArrayList list = new ArrayList();
	ArrayList<String> uniqueIds = new ArrayList<String>();
	Hashtable<String,ArrayList<String>> dataProtHash = new Hashtable<String,ArrayList<String>>();
	Hashtable<String,ArrayList<String>> wireProtHash = new Hashtable<String,ArrayList<String>>();
	Hashtable<String,Vector<String>> sysListHash = new Hashtable<String,Vector<String>>();
	Hashtable<String,Vector<String>> ccdListHash = new Hashtable<String,Vector<String>>();
	
	/**
	 * Processes and stores the system info queries and calls the system info writer to output the system info report
	 */
	public void generateDataInterfaceReport() {	
		//checking to see if databases are loaded
		try
		{
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngineName);
			if(engine==null)
				throw new NullPointerException();
		} catch (NullPointerException e) {
			Utility.showError("Cannot find HR Core engine.");
			return;
		}

		BasicReportWriter writer = new BasicReportWriter();
		String folder = "\\export\\Reports\\";
		String writeFileName = "Data_Interface_With_DHMSM_"+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";

		String fileLoc = workingDir + folder + writeFileName;
		writer.makeWorkbook(fileLoc);
		logger.info(fileLoc);	
		
		processDHMSMInterfaceQueries();
		processResults();
		writer.writeListSheet("Data Interface With DHMSM", headersList,list);
		
		clearData();
		processIOCFOCQueries();
		processResults();
		writer.writeListSheet("IOC to FOC Interface", headersList,list);
		
		clearData();
		processDeviceQueries();
		
		writer.writeWorkbook();
		
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\"+writeFileName);

	}
	public void clearData()
	{
		headersList = new ArrayList<String>();
		list = new ArrayList();
		
		uniqueIds = new ArrayList<String>();
		dataProtHash = new Hashtable<String,ArrayList<String>>();
		wireProtHash = new Hashtable<String,ArrayList<String>>();
		sysListHash = new Hashtable<String,Vector<String>>();
		ccdListHash = new Hashtable<String,Vector<String>>();
	}

	/**
	 * Identifies and runs all the queries required for the system info report.
	 * Stores values in masterHash 
	 */
	public void processDHMSMInterfaceQueries() {

		//systems that consume data from DHMSM
		String dataConsumerQuery1 = "SELECT DISTINCT ?Data (COALESCE(?Format,'') AS ?Data_Protocol) (COALESCE(?Protocol,'') AS ?Wire_Protocol) ?LowMedProbSystem ?ConsumingProviding_Data_FromTo_DHMSM (COALESCE(?CCD2,'') AS ?CCD) (COALESCE(?Interface,'Y') AS ?Interface_Needed) WHERE {BIND('Consuming' AS ?ConsumingProviding_Data_FromTo_DHMSM) {?HighProbSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?LowMedProbSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?ICD1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}  {?LowMedProbSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?LowMed}{?LowMedProbSystem <http://semoss.org/ontologies/Relation/Contains/Device_Interface> 'N'}{?HighProbSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?HighProbSystem <http://semoss.org/ontologies/Relation/Provide> ?ICD1} {?ICD1 <http://semoss.org/ontologies/Relation/Consume> ?LowMedProbSystem}{?ICD1 ?payload ?Data}OPTIONAL{{?payload1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}{?payload1 <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;}{?payload1 <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;}{?ICD1 ?payload1 ?Data}}OPTIONAL{{?CCD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CCD>;}{?Data <http://semoss.org/ontologies/Relation/MapsTo> ?CCD2}}OPTIONAL{?LowMedProbSystem <http://semoss.org/ontologies/Relation/Contains/Interface_Needed> ?Interface} } ORDER BY ?Data BINDINGS ?LowMed {('Low')('Medium')}";
//		String dataConsumerQuery1 = "SELECT DISTINCT ?Data (COALESCE(?Format,'') AS ?Data_Protocol) (COALESCE(?Protocol,'') AS ?Wire_Protocol) ?System ?ConsumingProviding_Data_FromTo_DHMSM ?CCD_Element WHERE { filter( !regex(str(?crm),'R')){?HighSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> }{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> }{?CCD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CCD> }  {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}  {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}{?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}{?HighSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?LowMed}{?HighSystem <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Consume> ?System ;}{?icd ?payload ?Data}{?HighSystem ?provideData ?Data}{?Data <http://semoss.org/ontologies/Relation/MapsTo> ?CCD}BIND('Consuming' AS ?ConsumingProviding_Data_FromTo_DHMSM)OPTIONAL{{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}{?icd ?carries ?Data}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} }  } ORDER BY ?System ?Data BINDINGS ?LowMed {('Low')('Medium')}";
//		String dataConsumerQuery2 = "SELECT DISTINCT ?Data (COALESCE(?Format,'') AS ?Data_Protocol) (COALESCE(?Protocol,'') AS ?Wire_Protocol) ?System ?ConsumingProviding_Data_FromTo_DHMSM ?CCD_Element WHERE {BIND('Consuming' AS ?ConsumingProviding_Data_FromTo_DHMSM)  {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> } {?HighSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?CCD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CCD> } {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?HighSystem}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?HighSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?LowMed}{?HighSystem <http://semoss.org/ontologies/Relation/Provide> ?icd ;}{?icd <http://semoss.org/ontologies/Relation/Consume> ?System ;}{?icd ?payload ?Data ;}{?Data <http://semoss.org/ontologies/Relation/MapsTo> ?CCD}FILTER(!BOUND(?icd2))OPTIONAL{{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}{?icd ?carries ?Data}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} } } ORDER BY ?System ?Data BINDINGS ?LowMed {('Low')('Medium')}";
		
		//systems that provide data to DHMSM
		String dataProviderQuery1 = "SELECT DISTINCT ?Data (COALESCE(?Format,'') AS ?Data_Protocol) (COALESCE(?Protocol,'') AS ?Wire_Protocol) ?LowMedProbSystem ?ConsumingProviding_Data_FromTo_DHMSM (COALESCE(?CCD2,'') AS ?CCD) (COALESCE(?Interface,'Y') AS ?Interface_Needed) WHERE {BIND('Providing' AS ?ConsumingProviding_Data_FromTo_DHMSM) {?HighProbSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?LowMedProbSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?ICD1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?LowMedProbSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?LowMed}{?LowMedProbSystem <http://semoss.org/ontologies/Relation/Contains/Device_Interface> 'N'}{?HighProbSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?LowMedProbSystem <http://semoss.org/ontologies/Relation/Provide> ?ICD1} {?ICD1 <http://semoss.org/ontologies/Relation/Consume> ?HighProbSystem}{?ICD1 <http://semoss.org/ontologies/Relation/Payload> ?Data}OPTIONAL{{?payload1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}{?ICD1 ?payload1 ?Data}{?payload1 <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;}{?payload1 <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;}}OPTIONAL{{?CCD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CCD>;}{?Data <http://semoss.org/ontologies/Relation/MapsTo> ?CCD2}}OPTIONAL{?LowMedProbSystem <http://semoss.org/ontologies/Relation/Contains/Interface_Needed> ?Interface}} ORDER BY ?Data BINDINGS ?LowMed {('Low')('Medium')}";
//		String dataProviderQuery1 = "SELECT DISTINCT ?Data (COALESCE(?Format,'') AS ?Data_Protocol) (COALESCE(?Protocol,'') AS ?Wire_Protocol) ?System ?ConsumingProviding_Data_FromTo_DHMSM ?CCD_Element WHERE { filter( !regex(str(?crm),'R')){?HighSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> }{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> } {?CCD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CCD> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}  {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}{?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}{?HighSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?LowMed}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Consume> ?HighSystem ;}{?icd ?payload ?Data}{?Data <http://semoss.org/ontologies/Relation/MapsTo> ?CCD}{?HighSystem ?provideData ?Data}BIND('Providing' AS ?ConsumingProviding_Data_FromTo_DHMSM)OPTIONAL{{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}{?icd ?carries ?Data}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} }  } ORDER BY ?System ?Data BINDINGS ?LowMed {('Low')('Medium')}";
//		String dataProviderQuery2 = "SELECT DISTINCT ?Data (COALESCE(?Format,'') AS ?Data_Protocol) (COALESCE(?Protocol,'') AS ?Wire_Protocol) ?System ?ConsumingProviding_Data_FromTo_DHMSM ?CCD_Element WHERE {BIND('Providing' AS ?ConsumingProviding_Data_FromTo_DHMSM)  {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> } {?HighSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?CCD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CCD> } {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?HighSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?LowMed}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;}{?icd <http://semoss.org/ontologies/Relation/Consume> ?HighSystem ;}{?icd ?payload ?Data ;}{?Data <http://semoss.org/ontologies/Relation/MapsTo> ?CCD}FILTER(!BOUND(?icd2))OPTIONAL{{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}{?icd ?carries ?Data}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} } } ORDER BY ?System ?Data BINDINGS ?LowMed {('Low')('Medium')}";
		
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngineName);
		runListQuery(engine, dataConsumerQuery1);
		runListQuery(engine, dataProviderQuery1);
	}
	/**
	 * Identifies and runs all the queries required for the system info report.
	 * Stores values in masterHash 
	 */
	public void processIOCFOCQueries() {
		
		String iocFOCInterfaceQuery1 = "SELECT DISTINCT ?Data ?Data_Protocol ?Wire_Protocol ?HighSystem ?ConsumingProviding_Data_FromTo_DHMSM (COALESCE(?CCD2,'') AS ?CCD) WHERE { BIND('TBD' AS ?Data_Protocol) BIND('TBD' AS ?Wire_Protocol)BIND('Providing and Consuming' AS ?ConsumingProviding_Data_FromTo_DHMSM) filter( !regex(str(?crm),'R')){?HighSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> }{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?HighSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}  {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}{?HighSystem <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Consume> ?System ;}{?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}{?HighSystem ?provideData ?Data}OPTIONAL{{?CCD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CCD> } {?Data <http://semoss.org/ontologies/Relation/MapsTo> ?CCD2}}} ORDER BY ?System ?Data";
		String iocFOCInterfaceQuery2 = "SELECT DISTINCT ?Data ?Data_Protocol ?Wire_Protocol ?HighSystem ?ConsumingProviding_Data_FromTo_DHMSM (COALESCE(?CCD2,'') AS ?CCD) WHERE { BIND('TBD' AS ?Data_Protocol) BIND('TBD' AS ?Wire_Protocol) BIND('Providing and Consuming' AS ?ConsumingProviding_Data_FromTo_DHMSM){?HighSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> } {?HighSystem <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?HighSystem <http://semoss.org/ontologies/Relation/Provide> ?icd ;}{?icd <http://semoss.org/ontologies/Relation/Consume> ?System ;}{?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?HighSystem}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}OPTIONAL{{?CCD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CCD> }{?Data <http://semoss.org/ontologies/Relation/MapsTo> ?CCD2}} FILTER(!BOUND(?icd2))} ORDER BY ?System ?Data";

		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngineName);
		
		runListQuery(engine, iocFOCInterfaceQuery1);
		runListQuery(engine, iocFOCInterfaceQuery2);

	}
	
	public void processDeviceQueries() {

		//systems that consume data from DHMSM
		String deviceQuery = "SELECT DISTINCT ?Data ?ConsumingProviding_Data_FromTo_DHMSM ?Device_Category ?System WHERE { { SELECT DISTINCT ?System ?ConsumingProviding_Data_FromTo_DHMSM ?Data WHERE {BIND('Providing' AS ?ConsumingProviding_Data_FromTo_DHMSM) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Device_Interface> '?'} {?Interface <http://semoss.org/ontologies/Relation/Payload> ?Data;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;}{?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} BIND('Downstream' AS ?type)} } UNION { SELECT DISTINCT ?System ?ConsumingProviding_Data_FromTo_DHMSM ?Data WHERE {BIND('Consuming' AS ?ConsumingProviding_Data_FromTo_DHMSM){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?System <http://semoss.org/ontologies/Relation/Contains/Device_Interface> '?'} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface <http://semoss.org/ontologies/Relation/Payload> ?Data;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;}{?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} BIND('Upstream' AS ?type) } } }";
		
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngineName);
		runListQuery(engine, deviceQuery);

	}
	
	public void processResults()
	{
		int numData = dataProtHash.keySet().size();
		int numWire = wireProtHash.keySet().size();
		headersList.add("Data");
		Vector<String> dataProtKeySet = new Vector<String>(dataProtHash.keySet());
		Vector<String> wireProtKeySet = new Vector<String>(wireProtHash.keySet());
		Collections.sort(dataProtKeySet);
		Collections.sort(wireProtKeySet);
		headersList.addAll(dataProtKeySet);
		headersList.addAll(wireProtKeySet);
		headersList.add("Systems");
		headersList.add("Consuming/Providing_Data_From/To_DHMSM");
		headersList.add("CCD");
		for(int i=0;i<list.size();i++)
		{
			Object[] row = (Object[]) list.get(i);
			Object[] newRow = new Object[headersList.size()];
			newRow[0] = row[0];
			newRow[2+numData+numWire] = row[1];
			String identifier = (String)row[0]+row[1];
			for(int dataInd = 1;dataInd<1+numData;dataInd++)
			{
				if(dataProtHash.get(headersList.get(dataInd)).contains(identifier))
					newRow[dataInd] = "X";
			}
			for(int wireInd = 1+numData;wireInd<1+numData+numWire;wireInd++)
			{
				if(wireProtHash.get(headersList.get(wireInd)).contains(identifier))
					newRow[wireInd] = "X";
			}
			Vector<String> sysList = sysListHash.get(identifier);
			Vector<String> ccdList = ccdListHash.get(identifier);
			Collections.sort(sysList);
			String sys = makeString(sysList);
			String ccd = "";
			if(ccdList!=null)
			{
				Collections.sort(ccdList);
				ccd = makeString(ccdList);
			}
			newRow[1+numData+numWire] = sys;
			newRow[3+numData+numWire] = ccd;
			list.set(i, newRow);
		}
	}
	public String makeString(Vector<String> list)
	{
		String retVal=list.get(0);
		for(int i=1;i<list.size();i++)
			retVal+=", "+list.get(i);
		return retVal;
	}
	
	public void runListQuery(IEngine engine, String query) {
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		try{
			wrapper.executeQuery();	
		}
		catch (RuntimeException e)
		{
			e.printStackTrace();
		}
		// get the bindings from it
		String[] names = wrapper.getVariables();
		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				String interfaceNeeded = "";
				if(names.length>=7)
					interfaceNeeded = ((String)getVariable(names[6], sjss)).replaceAll("\"","");
				if(interfaceNeeded.equals("") || interfaceNeeded.contains("Y") || interfaceNeeded.contains("?"))
				{
					String [] values = new String[2];
					values[0] = ((String)getVariable(names[0], sjss)).replaceAll("\"","");
					values[1] = ((String)getVariable(names[4], sjss)).replaceAll("\"","");
					
					String identifier = (String)values[0]+values[1];
					
					if(!uniqueIds.contains(identifier))
					{
						uniqueIds.add(identifier);
						list.add(values);
					}
					
					String dataProt = ((String)getVariable(names[1], sjss)).replaceAll("\"","");
					String wireProt = ((String)getVariable(names[2], sjss)).replaceAll("\"","");
					String system = ((String)getVariable(names[3], sjss)).replaceAll("\"","");
					String CCD = ((String)getVariable(names[5],sjss)).replaceAll("\"","");
					
					if(dataProtHash.containsKey(dataProt))
						dataProtHash.get(dataProt).add(identifier);
					else
					{
						ArrayList<String> dataList = new ArrayList<String>();
						dataList.add(identifier);
						dataProtHash.put(dataProt, dataList);
					}
					if(wireProtHash.containsKey(wireProt))
						wireProtHash.get(wireProt).add(identifier);
					else
					{
						ArrayList<String> wireList = new ArrayList<String>();
						wireList.add(identifier);
						wireProtHash.put(wireProt, wireList);
					}
					if(sysListHash.containsKey(identifier))
					{
						Vector<String> sysList = sysListHash.get(identifier);
						if(!sysList.contains(system))
							sysList.add(system);
					}
					else
					{
						Vector<String> sysList = new Vector<String>();
						sysList.add(system);
						sysListHash.put(identifier,sysList);
					}
					if(!CCD.equals("")&&ccdListHash.containsKey(identifier))
					{
						Vector<String> ccdList = ccdListHash.get(identifier);
						if(!ccdList.contains(CCD))
							ccdList.add(CCD);
					}
					else if(!CCD.equals(""))
					{
						Vector<String> ccdList = new Vector<String>();
						ccdList.add(CCD);
						ccdListHash.put(identifier,ccdList);
					}
					logger.debug("Creating new Value " + values);
					list.add(count, values);
					count++;
				}
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
	}
	/**
	 * Method getVariable. Gets the variable names from the query results.
	 * @param varName String - the variable name.
	 * @param sjss SesameJenaSelectStatement - the associated sesame jena select statement.

	 * @return Object - results.*/
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}
}