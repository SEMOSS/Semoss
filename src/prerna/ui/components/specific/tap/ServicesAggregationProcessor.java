package prerna.ui.components.specific.tap;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sparql.ast.DeleteData;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.UpdateProcessor;
import prerna.util.Utility;

public class ServicesAggregationProcessor {

	Logger logger = Logger.getLogger(getClass());
	private IEngine servicesDB;
	private IEngine coreDB;
	private String semossBaseURI = "http://semoss.org/ontologies/Concept/";
	private String semossRelBaseURI = "http://semoss.org/ontologies/Relation/";
	private String propURI = "http://semoss.org/ontologies/Relation/Contains/";

	private Hashtable<String, Hashtable<String, String>> dataHash = new Hashtable<String, Hashtable<String, String>>();
	private Hashtable<String, Hashtable<String, String>> removeDataHash = new Hashtable<String, Hashtable<String, String>>();

	private Hashtable<String, Set<String>> allRelations = new Hashtable<String, Set<String>>();
	private Hashtable<String, Set<String>> allConcepts = new Hashtable<String, Set<String>>();

	private HashSet<String> allSoftwareModules = new HashSet<String>();
	private HashSet<String> allHardwareModules = new HashSet<String>();

	private String TAP_SYSTEM_SERVICES_PROPERTY_AGGREGATION_QUERY = "SELECT DISTINCT ?system ?prop ?value ?user WHERE { {?system a <http://semoss.org/ontologies/Concept/System>} "
			+ "{?systemService a <http://semoss.org/ontologies/Concept/SystemService>} {?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} "
			+ "{?system ?consists ?systemService} {?prop a <http://semoss.org/ontologies/Relation/Contains>} {?systemService ?prop ?value} "
			+ "{?usedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf>  <http://semoss.org/ontologies/Relation/UsedBy>} {?systemService ?usedBy ?user} {?user a <http://semoss.org/ontologies/Concept/SystemUser> } }";

	private String TAP_CORE_PROPERTY_AGGREGATION_QUERY = "SELECT DISTINCT ?system ?prop ?value WHERE { {?system a <http://semoss.org/ontologies/Concept/System>} "
			+ "{?prop a <http://semoss.org/ontologies/Relation/Contains>} {?system ?prop ?value} }";

	private String TAP_SERVICES_AGGREGATE_PERSONNEL_QUERY = "SELECT DISTINCT ?system ?usedBy ?personnel WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}"
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>}"
			+ "{?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>}"
			+ "{?system ?consistsOf ?SystemService} {?usedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>} "
			+ "{?personnel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Personnel>} {?systemService ?usedBy ?personnel}}";

	private String TAP_SERVICES_AGGREGATE_USER_INTERFACE_QUERY = "SELECT DISTINCT ?system ?utilizes ?userInterface "
			+ "WHERE{ {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} "
			+ "{?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} "
			+ "{?system ?consistsOf ?systemService} {?utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>} "
			+ "{?userInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface>} {?systemService ?utilizes ?userInterface}}";

	private String TAP_SERVICES_AGGREGATE_BP_QUERY = "SELECT DISTINCT ?system ?supports ?bp WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}"
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} "
			+ "{?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} "
			+ "{?system ?consistsOf ?systemService} {?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} "
			+ "{?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {?systemService ?supports ?bp}}";

	private String TAP_SERVICES_AGGREGATE_ACTIVITY_QUERY = "SELECT DISTINCT ?system ?supports ?activity WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}"
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} "
			+ "{?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} "
			+ "{?system ?consistsOf ?systemService} {?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} "
			+ "{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?systemService ?supports ?activity}}";

	private String TAP_SERVICES_AGGREGATE_BLU_QUERY = "SELECT DISTINCT ?system ?provide ?BLU WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} "
			+ "{?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} "
			+ "{?system ?consistsOf ?systemService} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} "
			+ "{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?systemService ?provide ?BLU}}";

	private String TAP_SERVICES_AGGREGATE_TERROR_QUERY = "SELECT DISTINCT ?system ?has ?TError ?weight WHERE{ "
			+ "{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} "
			+ "{?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} "
			+ "{?system ?consistsOf ?systemService} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} "
			+ "{?TError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError>} {?systemService ?has ?TError} "
			+ "{?has <http://semoss.org/ontologies/Relation/Contains/weight> ?weight} }";

	private String TAP_CORE_AGGREGATE_TERROR_QUERY = "SELECT DISTINCT ?system ?has ?TError ?weight WHERE{ "
			+ "{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
			+ "{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} "
			+ "{?TError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError>} "
			+ "{?system ?has ?TError} {?has <http://semoss.org/ontologies/Relation/Contains/weight> ?weight} }";

	private String TAP_CORE_AGGREGATE_ICD_QUERY = "SELECT DISTINCT ?ICD ?prop ?value WHERE {{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} "
			+ "{?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} "
			+ "{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} "
			+ "{?ICD ?Payload ?Data} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?Payload ?prop ?value}}";

	private String TAP_SERVICES_AGGREGATE_ICD_QUERY = "SELECT DISTINCT ?ICD ?prop ?value ?user WHERE {{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} "
			+ "{?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} "
			+ "{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} "
			+ "{?ICD ?Payload ?Data} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?Payload ?prop ?value} "
			+ "{?usedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf>  <http://semoss.org/ontologies/Relation/UsedBy>} {?systemService ?usedBy ?user} {?user a <http://semoss.org/ontologies/Concept/SystemUser> } }";

	private String TAP_SERVICES_AGGREGATE_DATAOBJECT_QUERY = "SELECT DISTINCT ?system ?provide ?data ?crm WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} "
			+ "{?system ?consistsOf ?SystemService} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} "
			+ "{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?SystemService ?provide ?data}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} }";

	private String TAP_CORE_AGGREGATE_DATAOBJECT_QUERY = "SELECT DISTINCT ?system ?provide ?data ?crm WHERE{ {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
			+ "{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} "
			+ "{?system ?provide ?data} {?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}}";

	private String TAP_CORE_SOFTWARE_MODULE_LIST_QUERY = "SELECT DISTINCT ?softwareModule WHERE { {?softwareModule a <http://semoss.org/ontologies/Concept/SoftwareModule>} }";
	private String TAP_CORE_HARDWARE_MODULE_LIST_QUERY = "SELECT DISTINCT ?hardwareModule WHERE { {?hardwareModule a <http://semoss.org/ontologies/Concept/HardwareModule>} }";

	private String TAP_SERVICES_AGGREGATION_SOFTWARE_QUERY = "SELECT DISTINCT ?serviceSoftwareModule ?prop ?value ?system ?softwareVersion ?software ?user WHERE { "
			+ "{?systemService a <http://semoss.org/ontologies/Concept/SystemService>} {?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} "
			+ "{?serviceSoftwareModule a <http://semoss.org/ontologies/Concept/ServiceSoftwareModule>} {?systemService ?consists ?serviceSoftwareModule} "
			+ "{?prop a <http://semoss.org/ontologies/Relation/Contains>} {?serviceSoftwareModule ?prop ?value} {?system a <http://semoss.org/ontologies/Concept/System>} "
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} {?system ?consistsOf ?systemService} "
			+ "{?typeOf1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?softwareModule a <http://semoss.org/ontologies/Concept/SoftwareModule>} "
			+ "{?serviceSoftwareModule ?typeOf1 ?softwareModule} {?typeOf2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} "
			+ "{?softwareVersion a <http://semoss.org/ontologies/Concept/SoftwareVersion>} {?softwareModule ?typeOf2 ?softwareVersion} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} "
			+ "{?software a <http://semoss.org/ontologies/Concept/Software>} {?software ?has ?softwareVersion} {?usedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>} "
			+ "{?systemService ?usedBy ?user} {?user a <http://semoss.org/ontologies/Concept/SystemUser>} }";

	private String TAP_CORE_AGGREGATION_SOFTWARE_QUERY = "SELECT DISTINCT ?softwareModule ?prop ?value WHERE { {?softwareModule a <http://semoss.org/ontologies/Concept/SoftwareModule>} "
			+ "{?prop a <http://semoss.org/ontologies/Relation/Contains>} {?softwareModule ?prop ?value} }";

	private String TAP_SERVICES_AGGREGATE_HARDWARE_QUERY = "SELECT DISTINCT ?serviceHardwareModule ?prop ?value ?system ?hardwareVersion ?hardware ?user WHERE { "
			+ "{?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} "
			+ "{?has1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} "
			+ "{?serviceHardwareModule a <http://semoss.org/ontologies/Concept/ServiceHardwareModule>} {?systemService ?has1 ?serviceHardwareModule} "
			+ "{?prop a <http://semoss.org/ontologies/Relation/Contains>} {?serviceHardwareModule ?prop ?value} "
			+ "{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
			+ "{?consistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>} {?system ?consistsOf ?systemService} "
			+ "{?typeOf1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} "
			+ "{?hardwareModule a <http://semoss.org/ontologies/Concept/HardwareModule>} {?serviceHardwareModule ?typeOf1 ?hardwareModule} "
			+ "{?typeOf2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} "
			+ "{?hardwareVersion a <http://semoss.org/ontologies/Concept/HardwareVersion>} {?hardwareModule ?typeOf2 ?hardwareVersion} "
			+ "{?has2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} "
			+ "{?hardware a <http://semoss.org/ontologies/Concept/Hardware>} {?hardware ?has2 ?hardwareVersion} {?systemService ?usedBy ?user} {?user a <http://semoss.org/ontologies/Concept/SystemUser>} }";

	private String TAP_CORE_AGGREGATION_HARDWARE_QUERY = "SELECT DISTINCT ?hardwareModule ?prop ?value WHERE { {?hardwareModule a <http://semoss.org/ontologies/Concept/HardwareModule>} "
			+ "{?prop a <http://semoss.org/ontologies/Relation/Contains>} {?hardwareModule ?prop ?value} }";

	private String TAP_CORE_RELATIONS_LIST_QUERY = "SELECT ?relations WHERE { {?relations <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} filter( regex(str(?relations),\"^http://semoss\") ) }";
	private String TAP_CORE_CONCEPTS_LIST_QUERY = "SELECT ?concepts WHERE { {?concepts <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> ;} }";

	//	private String TAP_SERVICES_AGGREGATE_HARDWARE_QUERY = "SELECT DISTINCT ?hardwareVersion ?has ?hardware WHERE { {?hardwareVersion a <http://semoss.org/ontologies/Concept/HardwareVersion>} "
	//			+ "{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?hardware a <http://semoss.org/ontologies/Concept/Hardware>} {?hardware ?has ?hardwareVersion} }";

	public ServicesAggregationProcessor(IEngine servicesDB, IEngine coreDB){
		this.servicesDB = servicesDB;
		this.coreDB = coreDB;
	}

	public void runFullAggregation()
	{
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_PERSONNEL_QUERY);
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_USER_INTERFACE_QUERY);
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_BP_QUERY);
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_ACTIVITY_QUERY);
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_BLU_QUERY);
		runSystemServicePropertyAggregation(TAP_SYSTEM_SERVICES_PROPERTY_AGGREGATION_QUERY, TAP_CORE_PROPERTY_AGGREGATION_QUERY);
		runICDAggregation(TAP_SERVICES_AGGREGATE_ICD_QUERY, TAP_CORE_AGGREGATE_ICD_QUERY);
		runTErrorAggregation(TAP_SERVICES_AGGREGATE_TERROR_QUERY, TAP_CORE_AGGREGATE_TERROR_QUERY);
		runDataObjectAggregation(TAP_SERVICES_AGGREGATE_DATAOBJECT_QUERY, TAP_CORE_AGGREGATE_DATAOBJECT_QUERY);
		runGetListOfModules(TAP_CORE_SOFTWARE_MODULE_LIST_QUERY, true);
		runGetListOfModules(TAP_CORE_HARDWARE_MODULE_LIST_QUERY, false);
		runHardwareSoftwareAggregation(TAP_SERVICES_AGGREGATION_SOFTWARE_QUERY, TAP_CORE_AGGREGATION_SOFTWARE_QUERY, true);
		runHardwareSoftwareAggregation(TAP_SERVICES_AGGREGATE_HARDWARE_QUERY, TAP_CORE_AGGREGATION_HARDWARE_QUERY, false);
		processNewConcepts();
		processNewRelationships();
		((BigDataEngine) coreDB).infer();
	}

	private void runRelationshipAggregation(String query)
	{
		logger.info(query);
		dataHash.clear();
		SesameJenaSelectWrapper sjsw = processQuery(query, servicesDB);
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			// get the next row and see how it must be added to the insert query
			String subject = sjss.getRawVar(vars[0]).toString();
			String pred = sjss.getRawVar(vars[1]).toString();
			String object = sjss.getRawVar(vars[2]).toString();
			pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(subject, "/") +":" + getTextAfterFinalDelimeter(object, "/");
			logger.info("ADDING:     " + subject + " -----> {" + pred + " --- " + object + "}");
			addToHash(new String[]{subject, pred, object}, true);
			// add instances to master list
			addToAllConcepts(subject);
			addToAllConcepts(object);
			addToAllRelationships(pred);
		}
		processData(dataHash);
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process system service properties

	private void runSystemServicePropertyAggregation(String propSystemServiceQuery, String propTAPCoreQuery)
	{
		dataHash.clear();

		logger.info("PROCESSING QUERY: " + propSystemServiceQuery);
		SesameJenaSelectWrapper sjswServices = processQuery(propSystemServiceQuery, servicesDB);
		processServiceSystemProperties(sjswServices,  false);

		logger.info("PROCESSING QUERY: " + propTAPCoreQuery);
		SesameJenaSelectWrapper sjswCore = processQuery(propTAPCoreQuery, coreDB);
		processServiceSystemProperties(sjswCore, true);

		// processing modifies class variable dataHash directly
		processData(dataHash);
		deleteData(removeDataHash);
	}

	private void processServiceSystemProperties(SesameJenaSelectWrapper sjsw, boolean TAP_Core)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(vars[0]).toString();
			String prop = sjss.getRawVar(vars[1]).toString();
			String value = sjss.getRawVar(vars[2]).toString();
			String user = "";
			if(!TAP_Core)
			{
				user = sjss.getRawVar(vars[3]).toString();
			}

			if(dataHash.containsKey(sub) || !TAP_Core)
			{
				String[] returnTriple = new String[3];
				if(!value.equals("\"NA\"") && !value.equals("\"TBD\""))
				{
					if(prop.equals(propURI + "ATO_Date"))
					{
						boolean earliest = false;
						returnTriple = processMinMaxDate(sub, prop, value, earliest);
					}
					else if(prop.equals(propURI + "End_of_Support_Date"))
					{
						boolean latest = true;
						returnTriple = processMinMaxDate(sub, prop, value, latest);
					}
					else if(prop.equals(propURI + "Availability-Actual"))
					{
						boolean min = false;
						returnTriple = processMaxMinDouble(sub, prop, value, min);
					}
					else if(prop.equals(propURI + "Availability-Required"))
					{
						boolean max = true;
						returnTriple = processMaxMinDouble(sub, prop, value, max);
					}
					else if(prop.equals(propURI + "Description"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
					}
					else if(prop.equals(propURI + "POC"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
					}
					else if(prop.equals(propURI + "Full_System_Name"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
					}
					else if(prop.equals(propURI + "Number_of_Users"))
					{
						returnTriple = processSumValues(sub, prop, value);
					}
					else if(prop.equals(propURI + "Transaction_Count"))
					{
						returnTriple = processSumValues(sub, prop, value);
					}
					else if(prop.equals(propURI + "User_Consoles"))
					{
						returnTriple = processSumValues(sub, prop, value);
					}
					else if(prop.equals(propURI + "GarrisonTheater"))
					{
						returnTriple = processGarrisonTheater(sub, prop, value);
					}
					else if(prop.equals(propURI + "Transactional"))
					{
						returnTriple = processTransactional(sub, prop, value);
					}

					// returnTriple never gets a value when the property being passed in isn't in the defined list above
					if(returnTriple[0] != null)
					{
						addToHash(returnTriple, true);
					}

					// sub already exists when going through TAP Core db
					if(!TAP_Core)
					{
						addToAllConcepts(sub);
					}

					// must remove existing triple in TAP Core prior to adding
					if(TAP_Core)
					{
						addToHash(new String[]{sub, prop, value}, false);
					}
				}
			}
		}
	}

	private void runICDAggregation(String servicesQuery, String coreQuery)
	{
		dataHash.clear();

		logger.info(servicesQuery);
		SesameJenaSelectWrapper sjswService = processQuery(servicesQuery, servicesDB);
		processICDAggregation(sjswService, false);

		logger.info(coreQuery);
		SesameJenaSelectWrapper sjswCore = processQuery(coreQuery, coreDB);
		processICDAggregation(sjswCore , true);

		// processing modifies class variable dataHash directly
		processData(dataHash);
		deleteData(removeDataHash);
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process icds

	private void processICDAggregation(SesameJenaSelectWrapper sjsw, boolean TAP_Core) 
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(vars[0]).toString();
			String prop = sjss.getRawVar(vars[1]).toString();
			String value = sjss.getRawVar(vars[2]).toString();
			String user = "";
			if(!TAP_Core)
			{
				user = sjss.getRawVar(vars[3]).toString();
			}
			else
			{
				user = "";
			}

			// variables to define relationship
			String edgeType = "";

			if(dataHash.containsKey(sub) || !TAP_Core)
			{
				String[] returnTriple = new String[3];
				if(!value.equals("\"NA\"") && !value.equals("\"TBD\""))
				{
					if(prop.equals(propURI + "Data"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
						edgeType = "Has";
					}
					else if(prop.equals(propURI + "Format"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
						edgeType = "Has";
					}
					else if(prop.equals(propURI + "Frequency"))
					{
						returnTriple = processDFreq(sub, prop, value);
						edgeType = "Has";
					}
					else if(prop.equals(propURI + "Interface_Name"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
					}
					else if(prop.equals(propURI + "Protocol"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
						edgeType = "Has";
					}
					else if(prop.equals(propURI + "Source"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
					}
					else if(prop.equals(propURI + "Type"))
					{
						returnTriple = processConcatString(sub, prop, value, user);
					}
					// returnTriple never gets a value when the property being passed in isn't in the defined list above
					if(returnTriple[0] != null)
					{
						addToHash(returnTriple, true);
						if(!edgeType.equals(""))
						{
							String newRel = getBaseURI(sub) + "/Relation/" + edgeType + "/"  + getTextAfterFinalDelimeter(sub, "/") +":" + getTextAfterFinalDelimeter(value.replaceAll("\"", ""), "/");
							String newObj = sub.substring(0, sub.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(value.replaceAll("\"", ""), "/");
							logger.info("ADDING:     " + sub + " -----> {" + newRel + " --- " + newObj + "}");
							addToHash(new String[]{sub, newRel, newObj}, true);

							addToAllRelationships(newRel);
							addToAllConcepts(newObj);
						}
					}
					// sub already exists when going through TAP Core db
					if(!TAP_Core)
					{
						addToAllConcepts(sub);
					}
					// must remove existing triple in TAP Core prior to adding
					if(TAP_Core)
					{
						addToHash(new String[]{sub, prop, value}, false);
					}
				}
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process terror

	private void runTErrorAggregation(String servicesQuery, String coreQuery) 
	{
		Hashtable<String, Hashtable<String, LinkedList<String>>> aggregatedTError = new Hashtable<String, Hashtable<String, LinkedList<String>>>();
		dataHash.clear();

		logger.info(servicesQuery);
		SesameJenaSelectWrapper sjswService = processQuery(servicesQuery, servicesDB);
		aggregatedTError = runAggregateAllData(sjswService, aggregatedTError, "weight", false);

		logger.info(coreQuery);
		SesameJenaSelectWrapper sjswCore = processQuery(coreQuery, coreDB);
		aggregatedTError = runAggregateAllData(sjswCore, aggregatedTError, "weight", true);

		// processing modifies class variable dataHash directly
		processTError(aggregatedTError, "weight");

		processData(dataHash);
		deleteData(removeDataHash);
	}

	private void processTError(Hashtable<String, Hashtable<String, LinkedList<String>>> aggregatedTError, String propType) 
	{
		String propertyURI = propURI + propType;
		for( String sub : aggregatedTError.keySet() )
		{
			Hashtable<String, LinkedList<String>> innerHash = aggregatedTError.get(sub);
			for ( String obj : innerHash.keySet() )
			{
				LinkedList<String> tErrList = innerHash.get(obj);
				Iterator<String> tErrIt = tErrList.listIterator();
				int counter = 0;
				double totalTErr = 0;
				String pred = "";
				while(tErrIt.hasNext())
				{
					if(counter == 0)
					{
						pred  = tErrIt.next();
						counter++;
					}
					else 
					{
						String[] valueAsString = tErrIt.next().split("\"");
						totalTErr += Double.parseDouble(valueAsString[1]);
						counter++;
					}
				}

				Double TError = totalTErr/(counter-1);
				logger.info("ADDING:     " + sub + " -----> {" + pred + " --- " + obj + "}");
				addToHash(new String[]{sub, pred, obj}, true);
				logger.info("ADDING:     " + pred + " -----> {" + propertyURI + " --- " +  String.valueOf(TError) + "}");
				addToHash(new String[]{pred, propertyURI, String.valueOf(TError)}, true);
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process data objects

	private void runDataObjectAggregation(String servicesQuery, String coreQuery)
	{
		Hashtable<String, Hashtable<String, LinkedList<String>>> aggregatedDataObjects = new Hashtable<String, Hashtable<String, LinkedList<String>>>();
		dataHash.clear();

		logger.info(servicesQuery);
		SesameJenaSelectWrapper sjswService = processQuery(servicesQuery, servicesDB);
		aggregatedDataObjects = runAggregateAllData(sjswService , aggregatedDataObjects, "CRM", false);

		logger.info(coreQuery);
		SesameJenaSelectWrapper sjswCore = processQuery(coreQuery, coreDB);
		aggregatedDataObjects = runAggregateAllData(sjswCore, aggregatedDataObjects, "CRM", true);

		// processing modifies class variable dataHash directly
		processDataObjects(aggregatedDataObjects, "CRM");

		processData(dataHash);
		deleteData(removeDataHash);
	}

	private void processDataObjects(Hashtable<String, Hashtable<String, LinkedList<String>>> aggregatedDataObjects, String propType) 
	{
		String propertyURI = propURI + propType;
		for( String sub : aggregatedDataObjects.keySet() )
		{
			Hashtable<String, LinkedList<String>> innerHash = aggregatedDataObjects.get(sub);
			for ( String obj : innerHash.keySet() )
			{
				LinkedList<String> crmList = innerHash.get(obj);
				String pred  = crmList.get(0);
				String CRM = "";
				if(crmList.contains("\"C\""))
				{
					CRM = "\"C\"";
				}
				else if(crmList.contains("\"M\""))
				{
					CRM = "\"M\"";
				}
				else 
				{
					CRM = "\"R\"";
				}

				logger.info("ADDING:     " + sub + " -----> {" + pred + " --- " + obj + "}");
				addToHash(new String[]{sub, pred, obj}, true);
				logger.info("ADDING:     " + pred + " -----> {" + propertyURI + " --- " +  CRM + "}");
				addToHash(new String[]{pred, propertyURI, CRM}, true);
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process software and hardware modules

	private void runGetListOfModules(String query, boolean softwareModule) 
	{
		SesameJenaSelectWrapper sjsw = processQuery(query, coreDB);
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(vars[0]).toString();
			if(softwareModule)
			{
				allSoftwareModules.add(sub);
			}
			else
			{
				allHardwareModules.add(sub);
			}
		}
	}

	private void runHardwareSoftwareAggregation(String servicesQuery, String coreQuery, boolean softwareModule) 
	{
		dataHash.clear();

		logger.info("PROCESSING QUERY: " + servicesQuery);
		SesameJenaSelectWrapper sjswServices = processQuery(servicesQuery, servicesDB);
		processHardwareSoftwareProperties(sjswServices,  false, softwareModule);

		logger.info("PROCESSING QUERY: " + coreQuery);
		SesameJenaSelectWrapper sjswCore = processQuery(coreQuery, coreDB);
		processHardwareSoftwareProperties(sjswCore, true, softwareModule);

		// processing modifies class variable dataHash directly
		processData(dataHash);
		deleteData(removeDataHash);
	}


	private void processHardwareSoftwareProperties(SesameJenaSelectWrapper sjsw, boolean TAP_Core, boolean softwareModule)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String module = sjss.getRawVar(vars[0]).toString();
			String prop = sjss.getRawVar(vars[1]).toString();
			String value = sjss.getRawVar(vars[2]).toString();
			String user = "";
			if(!TAP_Core)
			{
				user = sjss.getRawVar(vars[6]).toString();
			}

			if(dataHash.containsKey(module) || !TAP_Core)
			{
				String[] returnTriple = new String[3];
				if(!value.equals("\"NA\"") && !value.equals("\"TBD\""))
				{
					if(prop.equals(propURI + "Quantity"))
					{
						returnTriple = processSumValues(module, prop, value);
					}
					else if(prop.equals(propURI + "Comments"))
					{
						returnTriple = processConcatString(module, prop, value, user);
					}
					else if(prop.equals(propURI + "EOL"))
					{
						boolean max = true;
						returnTriple = processMinMaxDate(module, prop, value, max);
					}
					else if(prop.equals(propURI + "Manufacturer"))
					{
						returnTriple = processConcatString(module, prop, value, user);
					}
					else if(prop.equals(propURI + "Model"))
					{
						returnTriple = processConcatString(module, prop, value, user);
					}
					else if(prop.equals(propURI + "Product_Type"))
					{
						returnTriple = processConcatString(module, prop, value, user);
					}
					else if(prop.equals(propURI + "Master_Version"))
					{
						returnTriple = processConcatString(module, prop, value, user);
					}
					else if(prop.equals(propURI + "Major_Version"))
					{
						returnTriple = processConcatString(module, prop, value, user);
					}
					else if(prop.equals(propURI + "Vendor"))
					{
						returnTriple = processConcatString(module, prop, value, user);
					}

					// returnTriple never gets a value when the property being passed in isn't in the defined list above
					if(returnTriple[0] != null)
					{
						addToHash(returnTriple, true);
					}

					// must remove existing triple in TAP Core prior to adding
					if(TAP_Core)
					{
						addToHash(new String[]{module, prop, value}, false);
					}

					// perform check to see if must add software/hardware version is software/hardware Module does not exist in TAP Core
					if(softwareModule)
					{
						if(!allSoftwareModules.contains(module))
						{
							String system = sjss.getRawVar(vars[3]).toString();
							String softwareV = sjss.getRawVar(vars[4]).toString();
							String software = sjss.getRawVar(vars[5]).toString();
							addToAllConcepts(system);
							addToAllConcepts(softwareV);
							addToAllConcepts(software);
							addToAllConcepts(module);

							String baseUri = getBaseURI(system);
							//relationship from system to softwareModule
							String predSysToMod = baseUri + "/Relatoin/Has/" + getTextAfterFinalDelimeter(system, "/") + ":" + getTextAfterFinalDelimeter(module, "/");
							addToAllRelationships(predSysToMod);
							addToHash(new String[]{system, predSysToMod, module}, true);
							//relationship from softwareModule to softwareVersion
							String predModToVer = baseUri + "/Relatoin/TypeOf/" + getTextAfterFinalDelimeter(module, "/") + ":" + getTextAfterFinalDelimeter(softwareV, "/");
							addToAllRelationships(predModToVer);
							addToHash(new String[]{module, predModToVer, softwareV}, true);
							//relationship from software to softwareVersion
							String predSoffToVer = baseUri + "/Relatoin/Has/" + getTextAfterFinalDelimeter(software, "/") + ":" + getTextAfterFinalDelimeter(softwareV, "/");
							addToHash(new String[]{software, predSoffToVer, softwareV}, true);
						}
					}
					else
					{
						if(!allHardwareModules.contains(module))
						{
							String system = sjss.getRawVar(vars[3]).toString();
							String hardwareV = sjss.getRawVar(vars[4]).toString();
							String hardware = sjss.getRawVar(vars[5]).toString();
							addToAllConcepts(system);
							addToAllConcepts(hardwareV);
							addToAllConcepts(hardware);
							addToAllConcepts(module);

							String baseUri = getBaseURI(system);
							//relationship from system to hardwareModule
							String predSysToMod = baseUri + "/Relatoin/Has/" + getTextAfterFinalDelimeter(system, "/") + ":" + getTextAfterFinalDelimeter(module, "/");
							addToAllRelationships(predSysToMod);
							addToHash(new String[]{system, predSysToMod, module}, true);
							//relationship from hardwareModule to hardwareVersion
							String predModToVer = baseUri + "/Relatoin/TypeOf/" + getTextAfterFinalDelimeter(module, "/") + ":" + getTextAfterFinalDelimeter(hardwareV, "/");
							addToAllRelationships(predModToVer);
							addToHash(new String[]{module, predModToVer, hardwareV}, true);
							//relationship from software to softwareVersion
							String predSoffToVer = baseUri + "/Relatoin/Has/" + getTextAfterFinalDelimeter(hardware, "/") + ":" + getTextAfterFinalDelimeter(hardwareV, "/");
							addToHash(new String[]{hardware, predSoffToVer, hardwareV}, true);
						}
					}
				}
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// methods used by all aggregation methods

	private void addToHash(String[] returnTriple, boolean add) 
	{
		Hashtable<String, String> innerHash = new Hashtable<String, String>();
		innerHash.put(returnTriple[1], returnTriple[2]);
		if(add)
		{
			if(dataHash.containsKey(returnTriple[0]))
			{
				dataHash.get(returnTriple[0]).putAll(innerHash);
			}
			else{
				dataHash.put(returnTriple[0], innerHash);
			}
		}
		else
		{
			removeDataHash.put(returnTriple[0], innerHash);
			if(removeDataHash.containsKey(returnTriple[0]))
			{
				removeDataHash.get(returnTriple[0]).putAll(innerHash);
			}
			else{
				removeDataHash.put(returnTriple[0], innerHash);
			}
		}
	}

	private void addToAllConcepts(String uri)
	{
		String conceptBaseURI = semossBaseURI + Utility.getClassName(uri);
		if(allConcepts.containsKey(conceptBaseURI))
		{
			allConcepts.get(conceptBaseURI).add(uri);
		}
		else
		{
			allConcepts.put(conceptBaseURI, new HashSet<String>());
			allConcepts.get(conceptBaseURI).add(uri);
		}		
	}

	private void addToAllRelationships(String uri)
	{
		String relationBaseURI = semossRelBaseURI + Utility.getClassName(uri);
		if(allRelations.containsKey(relationBaseURI))
		{
			allRelations.get(relationBaseURI).add(uri);
		}
		else
		{
			allRelations.put(relationBaseURI, new HashSet<String>());
			allRelations.get(relationBaseURI).add(uri);
		}
	}

	private void processData(Hashtable<String, Hashtable<String, String>> data)
	{
		for( String sub : data.keySet())
		{
			for ( String pred : data.get(sub).keySet())
			{
				String obj = data.get(sub).get(pred);
				boolean concept_triple = true;
				if( pred.contains("Relation/Contains"))
				{
					concept_triple = false;
				}

				( (BigDataEngine) coreDB).addStatement(sub, pred, obj, concept_triple);
				logger.info(sub + ">>>>>" + pred + ">>>>>" + obj + ">>>>>");
			}
		}
	}

	private void deleteData(Hashtable<String, Hashtable<String, String>> data)
	{
		StringBuilder deleteQuery = new StringBuilder("DELETE DATA { ");
		for ( String sub : data.keySet())
		{
			for (String pred : data.get(sub).keySet())
			{
				String obj = data.get(sub).get(pred);
				deleteQuery.append("<" + sub + ">" + "<" + pred + ">" + "<" + obj + ">. ");
			}
		}
		deleteQuery.append(" }");
		logger.info("DELETE QUERY: " + deleteQuery.toString());
		UpdateProcessor proc = new UpdateProcessor();
		proc.setEngine(coreDB);
		proc.setQuery(deleteQuery.toString());
		proc.processQuery();
	}

	private void processNewConcepts()
	{
		// get list of all concepts from tap core
		HashSet<String> conceptList = new HashSet<String>();
		logger.info("PROCESSING QUERY: " + TAP_CORE_CONCEPTS_LIST_QUERY);
		SesameJenaSelectWrapper sjsw = processQuery(TAP_CORE_CONCEPTS_LIST_QUERY, coreDB);
		String[] var = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			conceptList.add(sjss.getRawVar(var[0]) + "");
		}

		String pred = "http://www.w3.org/2000/01/rdf-schema#type";
		String concept = "http://semoss.org/ontologies/Concept";
		String subclassOf = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
		for ( String obj : allConcepts.keySet())
		{
			for (String sub : allConcepts.get(obj) )
			{
				( (BigDataEngine) coreDB).addStatement(sub, pred, obj, true);
				logger.info(sub + ">>>>>" + pred + ">>>>>" + obj + ">>>>>");
			}
			// add concepts that are not already in db
			if(!conceptList.contains(obj))
			{
				( (BigDataEngine) coreDB).addStatement(obj, subclassOf, concept, true);
				logger.info(obj + ">>>>>" + subclassOf + ">>>>>" + concept + ">>>>>");
			}
		}
	}

	private void processNewRelationships()
	{
		// get list of all relationships from tap core
		HashSet<String> relationshipList = new HashSet<String>();
		logger.info("PROCESSING QUERY: " + TAP_CORE_RELATIONS_LIST_QUERY);
		SesameJenaSelectWrapper sjsw = processQuery(TAP_CORE_RELATIONS_LIST_QUERY, coreDB);
		String[] var = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			relationshipList.add(sjss.getRawVar(var[0]) + "");
		}

		String pred = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
		String relation = "http://semoss.org/ontologies/Relation";
		String subpropertyOf = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
		for ( String obj : allRelations.keySet())
		{
			for (String sub : allRelations.get(obj) )
			{
				( (BigDataEngine) coreDB).addStatement(sub, pred, obj, false);
				logger.info(sub + ">>>>>" + pred + ">>>>>" + obj + ">>>>>");
			}
			// add relationships that are not already in db
			if(!relationshipList.contains(obj))
			{
				( (BigDataEngine) coreDB).addStatement(obj, pred, relation, false);
				logger.info(obj + ">>>>>" + subpropertyOf + ">>>>>" + relation + ">>>>>");
			}
		}	
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// general methods for properties

	private String[] processSumValues(String sub, String prop, String value)
	{
		Hashtable<String, String> innerHash = new Hashtable<String, String>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			logger.info("ADDING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			String[] newSumAsString = value.split("\"");
			String[] currentSum = innerHash.get(prop).split("\"");
			Double newSum = Double.parseDouble(newSumAsString[1]) + Double.parseDouble(currentSum[1]); 
			value = "\"" + newSum + "\"" + newSumAsString[2];
			logger.info("ADJUSTING:     " + sub + " -----> {" + prop + " --- " + "\"" + newSum + "\"" + newSumAsString[2] + "}");
		}
		return new String[]{sub, prop, value};
	}

	private String[] processConcatString(String sub, String prop, String value, String user) 
	{
		Hashtable<String, String> innerHash = new Hashtable<String, String>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			if(!user.equals(""))
			{
				value = "\"" + getTextAfterFinalDelimeter(user, "/") + ":" + value.substring(1);
			}
			logger.info("ADDING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			String currentString = innerHash.get(prop);
			if(!user.equals(""))
			{
				value = currentString.substring(0, currentString.length()-1) + ";" + getTextAfterFinalDelimeter(user, "/") + ":" + value.substring(1);
			}
			else
			{
				value = currentString.substring(0, currentString.length()-1) + ";" + value.substring(1);
			}
			logger.info("ADJUSTING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new String[]{sub, prop, value};
	}


	private String[] processMaxMinDouble(String sub, String prop, String value, boolean max)
	{
		Hashtable<String, String> innerHash = new Hashtable<String, String>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			logger.info("ADDING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			String[] oldDoubleAsString = innerHash.get(prop).split("\"");
			String[] newDoubleAsString = value.split("\"");
			Double oldDouble = null;
			Double newDouble = null;
			oldDouble = Double.parseDouble(oldDoubleAsString[1]);
			newDouble = Double.parseDouble(newDoubleAsString[1]);
			if(!max)
			{
				if(newDouble < oldDouble)
				{
					value =  "\"" + newDouble + "\"" + newDoubleAsString[2];
					logger.info("ADJUSTING:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
			else
			{
				if(newDouble > oldDouble)
				{
					value = "\"" + newDouble + "\"" + newDoubleAsString[2];
					logger.info("ADJUSTING:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
		}
		return new String[]{sub, prop, value};

	}

	private String[] processMinMaxDate(String sub, String prop, String value, Boolean latest) 
	{
		Hashtable<String, String> innerHash = new Hashtable<String, String>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			innerHash.put(prop, value);
			dataHash.put(sub, innerHash);
			logger.info("ADDING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			DateFormat formatter = new SimpleDateFormat("yyyyy-mm-dd'T'hh:mm:ss.sss'Z'");
			innerHash = dataHash.get(sub);
			String[] oldDateAsString = innerHash.get(prop).split("\"");
			String[] newDateAsString = value.split("\"");
			Date oldDate = null;
			Date newDate = null;
			try {
				oldDate = formatter.parse(oldDateAsString[1]);
				newDate = formatter.parse(newDateAsString[1]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(!latest)
			{
				if(newDate.before(oldDate))
				{
					value = "\"" + formatter.format(newDate) + "\"" + newDateAsString[2];
					logger.info("ADJUSTING:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
			else
			{
				if(newDate.after(oldDate))
				{
					value = "\"" + formatter.format(newDate) + "\"" + newDateAsString[2];
					logger.info("ADJUSTING:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
		}
		return new String[]{sub, prop, value};
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Unique methods for properties

	private String[] processGarrisonTheater(String sub, String prop, String value)
	{
		Hashtable<String, String> innerHash = new Hashtable<String, String>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			logger.info("ADDING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			String oldGT = innerHash.get(prop);
			if(!oldGT.equalsIgnoreCase(value))
			{
				value = "\"Both\"";
				logger.info("ADJUSTING:     " + sub + " -----> {" + prop + " --- " + "\"Both\"" + "}");
			}
		}
		return new String[]{sub, prop, value};
	}

	private String[] processTransactional(String sub, String prop, String value)
	{
		Hashtable<String, String> innerHash = new Hashtable<String, String>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			logger.info("ADDING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		//Different SystemServices should not be sending different transactional value
		//perform check to make sure data is correct
		else
		{
			innerHash = dataHash.get(sub);
			String currentTransactional = innerHash.get(prop);
			if(!currentTransactional.equalsIgnoreCase(value))
			{
				//TODO: add appropriate user information and need to break code
				System.out.println(">>>>>>>>>>>>>>>>>> DIFFERENT TRANSACTIONAL VALUE");
			}
		}
		return new String[]{sub, prop, value};
	}

	private String[] processDFreq(String sub, String prop, String value) 
	{
		Hashtable<String, String> innerHash = new Hashtable<String, String>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			logger.info("ADDING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			String[] frequencies = new String[]{value.replaceAll("\"", ""), innerHash.get(prop).replaceAll("\"", "")};
			Integer[] currentFreqValue = new Integer[2];

			for(int i = 0; i < frequencies.length; i++)
			{
				switch(frequencies[i])
				{
				case "Real-time (user-initiated)" : currentFreqValue[i] = 0; break;
				case "Batch (monthly)" : currentFreqValue[i] = 720; break;
				case "Weekly" : currentFreqValue[i] = 168; break;
				case "Monthly" : currentFreqValue[i] = 720; break;
				case "Batch (daily)" : currentFreqValue[i] = 24; break;
				case "Batch(Daily)" : currentFreqValue[i] = 24; break;
				case "Real-time" : currentFreqValue[i] = 0; break;
				case "Transactional" : currentFreqValue[i] = 0; break;
				case "On Demand" : currentFreqValue[i] = 0; break;
				case "Event Driven (seconds-minutes)" : currentFreqValue[i] = 0; break;
				case "TheaterFramework" : currentFreqValue[i] = 0; break;
				case "Event Driven (Seconds)" : currentFreqValue[i] = 0; break;
				case "Web services" : currentFreqValue[i] = 0; break;
				case "TF" : currentFreqValue[i] = 0; break;
				case "Batch (12/day)" : currentFreqValue[i] = 2; break;
				case "SFTP" : currentFreqValue[i] = 0; break;
				case "Batch (twice monthly)" : currentFreqValue[i] = 360; break;
				case "Daily" : currentFreqValue[i] = 24; break;
				case "Daily " : currentFreqValue[i] = 24; break;
				case "Hourly" : currentFreqValue[i] = 1; break;
				case "Near Real-time (transaction initiated)" : currentFreqValue[i] = 0; break;
				case "Batch (three times a week)" : currentFreqValue[i] = 56; break;
				case "Batch (weekly)" : currentFreqValue[i] = 168; break;
				case "Near Real-time" : currentFreqValue[i] = 0; break;
				case "Real Time" : currentFreqValue[i] = 0; break;
				case "Batch" : currentFreqValue[i] = 0; break;
				case "Batch (bi-monthly)" : currentFreqValue[i] = 1440; break;
				case "Batch (semiannually)" : currentFreqValue[i] = 4392; break;
				case "Event Driven (Minutes-hours)" : currentFreqValue[i] = 1; break;
				case "Annually" : currentFreqValue[i] = 8760; break;
				case "Batch(Monthly)" : currentFreqValue[i] = 720; break;
				case "Bi-Weekly" : currentFreqValue[i] = 336; break;
				case "Daily at end of day" : currentFreqValue[i] = 24; break;
				case "TCP" : currentFreqValue[i] = 0; break;
				case "event-driven (Minutes-hours)" : currentFreqValue[i] = 1; break;
				case "Interactive" : currentFreqValue[i] = 0; break;
				case "Weekly Quarterly" : currentFreqValue[i] = 0; break;
				case "Weekly Daily Weekly Weekly Weekly Weekly Daily Daily Daily" : currentFreqValue[i] = 168; break;
				case "Weekly Daily" : currentFreqValue[i] = 168; break;
				case "Periodic" : currentFreqValue[i] = 0; break;
				case "Batch (4/day)" : currentFreqValue[i] = 6; break;
				case "Batch(Daily/Monthly)" : currentFreqValue[i] = 720; break;
				case "Weekly; Interactive; Interactive" : currentFreqValue[i] = 168; break;
				case "interactive" : currentFreqValue[i] = 0; break;
				case "Batch (quarterly)" : currentFreqValue[i] = 2184; break;
				case "Every 8 hours (KML)/On demand (HTML)" : currentFreqValue[i] = 8; break;
				case "Monthly at beginning of month, or as user initiated" : currentFreqValue[i] = 720; break;
				case "On demad" : currentFreqValue[i] = 0; break;
				case "Monthly Bi-Monthly Weekly Weekly" : currentFreqValue[i] = 720; break;
				case "Quarterly" : currentFreqValue[i] = 2184; break;
				case "On-demand" : currentFreqValue[i] = 0; break;
				case "user upload" : currentFreqValue[i] = 0; break;
				case "1/hour (KML)/On demand (HTML)" : currentFreqValue[i] = 1; break;
				case "DVD" : currentFreqValue[i] = 0; break;
				case "Weekly " : currentFreqValue[i] = 168; break;
				case "Annual" : currentFreqValue[i] = 8760; break;
				case "Daily Interactive" : currentFreqValue[i] = 24; break;
				case "NFS, Oracle connection" : currentFreqValue[i] = 0; break;
				case "Batch(Weekly)" : currentFreqValue[i] = 168; break;
				case "Batch(Quarterly)" : currentFreqValue[i] = 2184; break;
				case "Batch (yearly)" : currentFreqValue[i] = 8760; break;
				case "Each user login instance" : currentFreqValue[i] = 0; break;
				}
			}
			if(currentFreqValue[0] > currentFreqValue[1])
			{
				value = innerHash.get(prop);
			}
			// else, do not change the value to keep the one being inputed
			logger.info("ADJUSTING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new String[]{sub, prop, value};
	}


	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Utility methods 

	private String getTextAfterFinalDelimeter(String uri, String delimeter)
	{
		if(!uri.equals(""))
		{
			uri = uri.substring(uri.lastIndexOf(delimeter)+1);
		}
		return uri;
	}

	private String getBaseURI(String uri)
	{
		return uri.substring(0, uri.substring(0, uri.substring(0, uri.lastIndexOf("/")).lastIndexOf("/")).lastIndexOf("/"));
	}

	//process the query
	private SesameJenaSelectWrapper processQuery(String query, IEngine engine){
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();		
		sjsw.getVariables();
		return sjsw;
	}

	private Hashtable<String, Hashtable<String, LinkedList<String>>> runAggregateAllData(SesameJenaSelectWrapper sjsw, Hashtable<String, Hashtable<String, LinkedList<String>>> aggregatedData, String propType, boolean TAP_Core)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			// get the next row and see how it must be added to the insert query
			String sys = sjss.getRawVar(vars[0]).toString();
			String pred = sjss.getRawVar(vars[1]).toString();
			String obj = sjss.getRawVar(vars[2]).toString();
			String prop = sjss.getRawVar(vars[3]).toString();

			if(!TAP_Core)
			{
				pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(sys, "/") +":" + getTextAfterFinalDelimeter(obj, "/");
			}

			if(aggregatedData.containsKey(sys) || !TAP_Core)
			{
				LinkedList<String> dataList = new LinkedList<String>();
				Hashtable<String, LinkedList<String>> innerHash = new Hashtable<String, LinkedList<String>>();
				if(!aggregatedData.containsKey(sys))
				{
					dataList.add(pred);
					dataList.add(prop);
					innerHash.put(obj, dataList);
					aggregatedData.put(sys, innerHash);
					logger.info("ADDING      :     " + sys + " -----> {" + obj + " --- " + dataList.toString() + "}");
				}
				else
				{
					if(!aggregatedData.get(sys).containsKey(obj))
					{
						innerHash = aggregatedData.get(sys);
						dataList.add(pred);
						dataList.add(prop);
						innerHash.put(obj, dataList);
						logger.info("ADDING      :     " + sys + " -----> {" + obj + " --- " + dataList.toString() + "}");
					}
					else
					{
						innerHash = aggregatedData.get(sys);
						dataList = innerHash.get(obj);
						dataList.add(prop);
						logger.info("ADDING      :     " + sys + " -----> {" + obj + " --- " + dataList.toString() + "}");
					}
				}

				// add instances to master list
				addToAllConcepts(sys);
				addToAllConcepts(obj);

				if(TAP_Core)
				{
					addToHash(new String[]{pred, propURI + propType, prop}, false);
				}
				addToAllRelationships(pred);
			}
		}
		return aggregatedData;
	}
}
