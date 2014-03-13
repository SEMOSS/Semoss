package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

public class DHMSMHelper {

	private String GET_ALL_SYSTEM_WITH_CREATE_AND_DOWNSTREAM_QUERY = "SELECT DISTINCT ?system ?data ?crm WHERE { filter( !regex(str(?crm),\"R\")) {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system ?provideData ?data} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} }";

	private String GET_ALL_SYSTEM_WITH_DOWNSTREAM_AND_NO_UPSTREAM = "SELECT DISTINCT ?System ?Data ?CRM WHERE { BIND(\"C\" as ?CRM) {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}FILTER(!BOUND(?icd2)) } ORDER BY ?System";	

	private String GET_ALL_SYSTEM_WITH_UPSTREAM = "SELECT DISTINCT ?system ?data ?crm WHERE { BIND(\"C\" as ?crm) FILTER NOT EXISTS{?icd <http://semoss.org/ontologies/Relation/Consume> ?system} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system <http://semoss.org/ontologies/Relation/Provide> ?data} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} }";

	private String GET_ALL_CAPABILITIES_AND_CRM = "SELECT DISTINCT ?cap ?data ?crm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?task ?needs ?data} {?needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} }";

	private Hashtable<String, Hashtable<String, String>> dataListSystems = new Hashtable<String, Hashtable<String, String>>();
	private Hashtable<String, Hashtable<String, String>> dataListCapabilities = new Hashtable<String, Hashtable<String, String>>();


	public void runData(IEngine engine)
	{
		SesameJenaSelectWrapper sjswQuery1 = processQuery(GET_ALL_SYSTEM_WITH_CREATE_AND_DOWNSTREAM_QUERY, engine);
		processResults(sjswQuery1, dataListSystems);

		SesameJenaSelectWrapper sjswQuery2 = processQuery(GET_ALL_SYSTEM_WITH_DOWNSTREAM_AND_NO_UPSTREAM, engine);
		processResults(sjswQuery2, dataListSystems);

		SesameJenaSelectWrapper sjswQuery3 = processQuery(GET_ALL_SYSTEM_WITH_UPSTREAM, engine);
		processResults(sjswQuery3, dataListSystems);

		SesameJenaSelectWrapper sjswQuery4 = processQuery(GET_ALL_CAPABILITIES_AND_CRM, engine);
		processResults(sjswQuery4, dataListCapabilities);
	}

	private ArrayList<ArrayList<String>> getSysOrCapAndData(String cap, String capCRM, String sysCRM, boolean getSys) 
	{
		ArrayList<ArrayList<String>> resultSet = new ArrayList<ArrayList<String>>();
		ArrayList<String> capDataList = new ArrayList<String>();
		Hashtable<String, Hashtable<String, String>> searchList = new Hashtable<String, Hashtable<String, String>>();
		Hashtable<String, Hashtable<String, String>> getList = new Hashtable<String, Hashtable<String, String>>();

		if(getSys)
		{	
			searchList = dataListCapabilities;
			getList = dataListSystems;
		}
		else
		{
			getList = dataListCapabilities;
			searchList = dataListSystems;
		}

		for( String data : searchList.keySet() )
		{
			Hashtable<String, String> innerHash = searchList.get(data);
			if(innerHash.containsKey(cap))
			{
				String crm = innerHash.get(cap);
				if(capCRM.equals("C"))
				{
					if(crm.equals(capCRM) || crm.equals("M"))
					{
						capDataList.add(data);
					}
				}
				else
				{
					if(crm.equals(capCRM))
					{
						capDataList.add(data);
					}
				}
			}
		}

		for( String data : capDataList)
		{
			Hashtable<String, String> innerHash = getList.get(data);
			for( String sys : innerHash.keySet())
			{
				if(sysCRM.equals("C"))
				{				
					if(innerHash.get(sys).equals(sysCRM) || innerHash.get(sys).equals("M"))
					{
						ArrayList<String> innerArray = new ArrayList<String>();
						innerArray.add(sys);
						innerArray.add(data);
						resultSet.add(innerArray);
					}
				}
				else
				{
					if(innerHash.get(sys).equals(sysCRM))
					{
						ArrayList<String> innerArray = new ArrayList<String>();
						innerArray.add(sys);
						innerArray.add(data);
						resultSet.add(innerArray);
					}
				}
			}

		}

		return resultSet;
	}

	private void processResults(SesameJenaSelectWrapper sjsw, Hashtable<String, Hashtable<String, String>> data )
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(vars[0]).toString();
			String obj = sjss.getRawVar(vars[1]).toString();
			String crm = sjss.getRawVar(vars[2]).toString();

			Hashtable<String, String> innerHash = new Hashtable<String, String>();
			if(!data.containsKey(obj))
			{
				innerHash.put(sub, crm);
				data.put(obj, innerHash);
			}
			else if(!data.get(obj).containsKey(sub))
			{
				innerHash = data.get(obj);
				innerHash.put(sub,  crm);
			}
			else
			{
				innerHash = data.get(obj);
				if((crm.equals("C") || crm.equals("M")) && innerHash.get(sub).equals("R"))
				{
					innerHash.put(sub, crm);
				}
			}
		}
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
}
