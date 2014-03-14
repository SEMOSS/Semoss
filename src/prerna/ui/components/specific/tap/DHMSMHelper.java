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

	private String GET_ALL_SYSTEM_WITH_UPSTREAM = "SELECT DISTINCT ?system ?data ?crm WHERE { BIND(\"R\" as ?crm) FILTER NOT EXISTS{?icd <http://semoss.org/ontologies/Relation/Consume> ?system} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system <http://semoss.org/ontologies/Relation/Provide> ?data} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} }";

	private String GET_ALL_CAPABILITIES_AND_CRM = "SELECT DISTINCT ?cap ?data ?crm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?task ?needs ?data} {?needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} }";

	private Hashtable<String, Hashtable<String, String>> dataListSystems = new Hashtable<String, Hashtable<String, String>>();
	private Hashtable<String, Hashtable<String, String>> dataListCapabilities = new Hashtable<String, Hashtable<String, String>>();


	public void runData(IEngine engine)
	{
		SesameJenaSelectWrapper sjswQuery1 = processQuery(GET_ALL_SYSTEM_WITH_CREATE_AND_DOWNSTREAM_QUERY, engine);
		processSysResults(sjswQuery1);

		SesameJenaSelectWrapper sjswQuery2 = processQuery(GET_ALL_SYSTEM_WITH_DOWNSTREAM_AND_NO_UPSTREAM, engine);
		processSysResults(sjswQuery2);

		SesameJenaSelectWrapper sjswQuery3 = processQuery(GET_ALL_SYSTEM_WITH_UPSTREAM, engine);
		processSysResults(sjswQuery3);

		SesameJenaSelectWrapper sjswQuery4 = processQuery(GET_ALL_CAPABILITIES_AND_CRM, engine);
		processCapResults(sjswQuery4);

		return;
	}
	
	public ArrayList<Integer> getNumOfCapabilitiesSupported(String system)
	{

		String capabilityGroup = "SELECT DISTINCT ?CapabilityFunctionalArea ?Capability WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;} {?CapabilityGroup ?ConsistsOfCapability ?Capability;}}";

		SesameJenaSelectWrapper sjswQuery = processQuery(capabilityGroup, (IEngine)DIHelper.getInstance().getLocalProp("HR_Core"));
		
		ArrayList<String> allHSDCapabilities = new ArrayList<String>();
		ArrayList<String> allHSSCapabilities = new ArrayList<String>();
		ArrayList<String> allFHPCapabilities = new ArrayList<String>();
		
		String[] vars = sjswQuery.getVariables();
		while(sjswQuery.hasNext())
		{
			SesameJenaSelectStatement sjss = sjswQuery.next();
			String group = sjss.getVar(vars[0]).toString();
			String cap = sjss.getVar(vars[1]).toString();
			if(group.contains("HSD"))
				allHSDCapabilities.add(cap);
			else if(group.contains("HSS"))
				allHSSCapabilities.add(cap);
			else if(group.contains("FHP"))
				allFHPCapabilities.add(cap);
		}
		
		
		ArrayList<ArrayList<String>> sysCreateCapCreate = getSysOrCapAndData(system,"C","C",false);
		ArrayList<ArrayList<String>> sysCreateCapRead = getSysOrCapAndData(system, "C","R",false);
		
		ArrayList<String> HSDCapabilities = new ArrayList<String>();
		ArrayList<String> HSSCapabilities = new ArrayList<String>();
		ArrayList<String> FHPCapabilities = new ArrayList<String>();
		
		for(ArrayList<String> row : sysCreateCapCreate)
		{
			String capability = row.get(0);
			if(!HSDCapabilities.contains(capability)&&allHSDCapabilities.contains(capability))
				HSDCapabilities.add(capability);
			else if(!HSSCapabilities.contains(capability)&&allHSSCapabilities.contains(capability))
				HSSCapabilities.add(capability);
			else if(!FHPCapabilities.contains(capability)&&allFHPCapabilities.contains(capability))
				FHPCapabilities.add(capability);
		}
		for(ArrayList<String> row : sysCreateCapRead)
		{
			String capability = row.get(0);
			if(!HSDCapabilities.contains(capability)&&allHSDCapabilities.contains(capability))
				HSDCapabilities.add(capability);
			else if(!HSSCapabilities.contains(capability)&&allHSSCapabilities.contains(capability))
				HSSCapabilities.add(capability);
			else if(!FHPCapabilities.contains(capability)&&allFHPCapabilities.contains(capability))
				FHPCapabilities.add(capability);
		}
		
		ArrayList<Integer> retVals = new ArrayList<Integer>();
		retVals.add(HSDCapabilities.size());
		retVals.add(HSSCapabilities.size());
		retVals.add(FHPCapabilities.size());
		
		return retVals;
	}

	public ArrayList<ArrayList<String>> getSysOrCapAndData(String capOrSys, String capCRM, String sysCRM, boolean getSys) 
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
			if(innerHash.containsKey(capOrSys))
			{
				String crm = innerHash.get(capOrSys);
				if(capCRM.contains("C"))
				{
					if(crm.contains(capCRM) || crm.contains("M"))
					{
						capDataList.add(data);
					}
				}
				else
				{
					if(crm.contains(capCRM))
					{
						capDataList.add(data);
					}
				}
			}
		}

		for( String data : capDataList)
		{
			Hashtable<String, String> innerHash = getList.get(data);
			if(innerHash != null)
			{
				for( String sys : innerHash.keySet())
				{
					if(sysCRM.contains("C"))
					{				
						if(innerHash.get(sys).contains(sysCRM) || innerHash.get(sys).contains("M"))
						{
							ArrayList<String> innerArray = new ArrayList<String>();
							innerArray.add(sys);
							innerArray.add(data);
							resultSet.add(innerArray);
						}
					}
					else
					{
						if(innerHash.get(sys).contains(sysCRM))
						{
							ArrayList<String> innerArray = new ArrayList<String>();
							innerArray.add(sys);
							innerArray.add(data);
							resultSet.add(innerArray);
						}
					}
				}
			}

		}

		return resultSet;
	}
	
	public ArrayList<String> getDataObjectList(String capOrSys, String capCRM, String sysCRM, boolean getSys) 
	{
		ArrayList<String> resultSet = new ArrayList<String>();
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
			if(innerHash.containsKey(capOrSys))
			{
				String crm = innerHash.get(capOrSys);
				if(capCRM.contains("C"))
				{
					if(crm.contains(capCRM) || crm.contains("M"))
					{
						capDataList.add(data);
					}
				}
				else
				{
					if(crm.contains(capCRM))
					{
						capDataList.add(data);
					}
				}
			}
		}

		for( String data : capDataList)
		{
			Hashtable<String, String> innerHash = getList.get(data);
			if(innerHash != null)
			{
				for( String sys : innerHash.keySet())
				{
					if(sysCRM.contains("C"))
					{				
						if(innerHash.get(sys).contains(sysCRM) || innerHash.get(sys).contains("M"))
						{
							if(!resultSet.contains(data))
								resultSet.add(data);
						}
					}
					else
					{
						if(innerHash.get(sys).contains(sysCRM))
						{
							if(!resultSet.contains(data))
								resultSet.add(data);
						}
					}
				}
			}

		}

		return resultSet;
	}
	
	public ArrayList<String> getAllDataFromSysOrCap(String capOrSys, String crm, boolean getSys)
	{
		ArrayList<String> resultSet = new ArrayList<String>();
		Hashtable<String, Hashtable<String, String>> dataList = new Hashtable<String, Hashtable<String, String>>();
		
		if(getSys)
		{	
			dataList = dataListSystems;
		}
		else
		{
			dataList = dataListCapabilities;
		}
		
		for( String data : dataList.keySet() )
		{
			Hashtable<String, String> innerHash = dataList.get(data);
			if(innerHash.containsKey(capOrSys))
			{			
				if(crm.contains("C"))
				{
					String dataCRM = innerHash.get(capOrSys);
					if(dataCRM.contains(crm) || dataCRM.contains("M"))
					{
						resultSet.add(data);
					}
				}
				else
				{
					String dataCRM = innerHash.get(capOrSys);
					if(dataCRM.contains(crm))
					{
						resultSet.add(data);
					}
				}
			}
		}
		
		return resultSet;
	}

	private void processSysResults(SesameJenaSelectWrapper sjsw)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String sub = sjss.getVar(vars[0]).toString();
			String obj = sjss.getVar(vars[1]).toString();
			String crm = sjss.getVar(vars[2]).toString();

			if(!dataListSystems.containsKey(obj))
			{
				Hashtable<String, String> innerHash = new Hashtable<String, String>();
				innerHash.put(sub, crm);
				dataListSystems.put(obj, innerHash);
			}
			else if(!dataListSystems.get(obj).containsKey(sub))
			{
				Hashtable<String, String> innerHash = dataListSystems.get(obj);
				innerHash.put(sub,  crm);
			}
			else
			{
				Hashtable<String, String> innerHash = dataListSystems.get(obj);
				if((crm.equals("\"C\"") || crm.equals("\"M\"")) && innerHash.get(sub).equals("\"R\""))
				{
					innerHash.put(sub, crm);
				}
			}
		}
	}

	private void processCapResults(SesameJenaSelectWrapper sjsw)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String sub = sjss.getVar(vars[0]).toString();
			String obj = sjss.getVar(vars[1]).toString();
			String crm = sjss.getVar(vars[2]).toString();

			if(!dataListCapabilities.containsKey(obj))
			{
				Hashtable<String, String> innerHash = new Hashtable<String, String>();
				innerHash.put(sub, crm);
				dataListCapabilities.put(obj, innerHash);
			}
			else if(!dataListCapabilities.get(obj).containsKey(sub))
			{
				Hashtable<String, String> innerHash = dataListCapabilities.get(obj);
				innerHash.put(sub,  crm);
			}
			else
			{
				Hashtable<String, String> innerHash = dataListCapabilities.get(obj);
				if((crm.equals("\"C\"") || crm.equals("\"M\"")) && innerHash.get(sub).equals("\"R\""))
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
