/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;

public class DHMSMHelper {

	private String GET_ALL_SYSTEM_WITH_CREATE_AND_DOWNSTREAM_QUERY = "SELECT DISTINCT ?system ?data ?crm WHERE { filter( !regex(str(?crm),\"R\")) {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system ?provideData ?data} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} }";

	private String GET_ALL_SYSTEM_WITH_DOWNSTREAM_AND_NO_UPSTREAM = "SELECT DISTINCT ?System ?Data ?CRM WHERE { BIND(\"C\" as ?CRM) {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;}{?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem ;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}FILTER(!BOUND(?icd2)) } ORDER BY ?System";	

	private String GET_ALL_SYSTEM_WITH_UPSTREAM = "SELECT DISTINCT ?system ?data ?crm WHERE { BIND(\"R\" as ?crm) {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?otherSystem <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Consume> ?system} {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} }";

	private String GET_ALL_DHMSM_CAPABILITIES_AND_CRM = "SELECT DISTINCT ?cap ?data ?crm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?task ?needs ?data} {?needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} }";
	
	private String GET_ALL_HR_CAPABILITIES_AND_CRM = "SELECT DISTINCT ?cap ?data ?crm WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?CapabilityGroup ?ConsistsOfCapability ?cap;} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?task ?needs ?data} {?needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSD>)(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSS>)(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/FHP>)}";

	private Hashtable<String, Hashtable<String, String>> dataListSystems = new Hashtable<String, Hashtable<String, String>>();
	private Hashtable<String, Hashtable<String, String>> dataListCapabilities = new Hashtable<String, Hashtable<String, String>>();
	
	private boolean useDHMSMOnly = true;


	public void runData(IEngine engine)
	{
		ISelectWrapper sjswQuery1 = processQuery(GET_ALL_SYSTEM_WITH_CREATE_AND_DOWNSTREAM_QUERY, engine);
		processAllResults(sjswQuery1, true);

		ISelectWrapper sjswQuery2 = processQuery(GET_ALL_SYSTEM_WITH_DOWNSTREAM_AND_NO_UPSTREAM, engine);
		processAllResults(sjswQuery2, true);

		ISelectWrapper sjswQuery3 = processQuery(GET_ALL_SYSTEM_WITH_UPSTREAM, engine);
		processAllResults(sjswQuery3, true);

		if(useDHMSMOnly)
		{
			ISelectWrapper sjswQuery4 = processQuery(GET_ALL_DHMSM_CAPABILITIES_AND_CRM, engine);
			processAllResults(sjswQuery4, false);
		}
		else
		{
			ISelectWrapper sjswQuery4 = processQuery(GET_ALL_HR_CAPABILITIES_AND_CRM, engine);
			processAllResults(sjswQuery4, false);
		}
		return;
	}
	public void setUseDHMSMOnly(boolean useDHMSMOnly)
	{
		this.useDHMSMOnly = useDHMSMOnly;
	}
	
	public ArrayList<Integer> getNumOfCapabilitiesSupported(String system)
	{

		String capabilityGroup = "SELECT DISTINCT ?CapabilityFunctionalArea ?Capability WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;} {?CapabilityGroup ?ConsistsOfCapability ?Capability;}}";

		ISelectWrapper sjswQuery = processQuery(capabilityGroup, (IEngine)DIHelper.getInstance().getLocalProp("HR_Core"));
		
		ArrayList<String> allHSDCapabilities = new ArrayList<String>();
		ArrayList<String> allHSSCapabilities = new ArrayList<String>();
		ArrayList<String> allFHPCapabilities = new ArrayList<String>();
		
		String[] vars = sjswQuery.getVariables();
		while(sjswQuery.hasNext())
		{
			ISelectStatement sjss = sjswQuery.next();
			String group = sjss.getVar(vars[0]).toString();
			String cap = sjss.getVar(vars[1]).toString();
			if(group.contains("HSD"))
				allHSDCapabilities.add(cap);
			else if(group.contains("HSS"))
				allHSSCapabilities.add(cap);
			else if(group.contains("FHP"))
				allFHPCapabilities.add(cap);
		}
		
		
		ArrayList<ArrayList<String>> sysCreateCapCreate = getCapAndData(system,"C","C");
		ArrayList<ArrayList<String>> sysCreateCapRead = getCapAndData(system, "R","C");
		
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

	public ArrayList<ArrayList<String>> getSysAndData(String cap, String capCRM, String sysCRM) 
	{
		return processSysOrCapAndData(cap, capCRM, sysCRM, dataListCapabilities, dataListSystems);
	}
	
	public ArrayList<ArrayList<String>> getCapAndData(String sys, String capCRM, String sysCRM)
	{
		return processSysOrCapAndData(sys, sysCRM, capCRM, dataListSystems, dataListCapabilities);
	}
	
	private ArrayList<ArrayList<String>> processSysOrCapAndData(String capOrSys, String searchCRM, String getCRM, 
			Hashtable<String, Hashtable<String, String>> searchList, 
			Hashtable<String, Hashtable<String, String>> getList ) 
	{
		ArrayList<ArrayList<String>> resultSet = new ArrayList<ArrayList<String>>();
		ArrayList<String> capDataList = new ArrayList<String>();

		for( String data : searchList.keySet() )
		{
			Hashtable<String, String> innerHash = searchList.get(data);
			if(innerHash.containsKey(capOrSys))
			{
				String crm = innerHash.get(capOrSys);
				if(searchCRM.contains("C"))
				{
					if(crm.contains(searchCRM) || crm.contains("M"))
					{
						capDataList.add(data);
					}
				}
				else
				{
					if(crm.contains(searchCRM))
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
					if(getCRM.contains("C"))
					{				
						if(innerHash.get(sys).contains(getCRM) || innerHash.get(sys).contains("M"))
						{
							ArrayList<String> innerArray = new ArrayList<String>();
							innerArray.add(sys);
							innerArray.add(data);
							resultSet.add(innerArray);
						}
					}
					else
					{
						if(innerHash.get(sys).contains(getCRM))
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
	
	public ArrayList<String> getDataObjectListSupportedFromSystem(String sys, String sysCRM, String capCRM)
	{
		return processDataObjectList(sys, sysCRM, capCRM, dataListSystems, dataListCapabilities);
	}
	
	public ArrayList<String> getDataObjectListSupportedFromCapabilities(String cap, String capCRM, String sysCRM)
	{
		return processDataObjectList(cap, capCRM, sysCRM, dataListCapabilities, dataListSystems);
	}
	
	private ArrayList<String> processDataObjectList(String capOrSys, String searchCRM, String getCRM, 
			Hashtable<String, Hashtable<String, String>> searchList, Hashtable<String, Hashtable<String, String>> getList) 
	{
		ArrayList<String> resultSet = new ArrayList<String>();
		ArrayList<String> capOrSysDataList = new ArrayList<String>();

		for( String data : searchList.keySet() )
		{
			Hashtable<String, String> innerHash = searchList.get(data);
			if(innerHash.containsKey(capOrSys))
			{
				String crm = innerHash.get(capOrSys);
				if(searchCRM.contains("C"))
				{
					if(crm.contains(searchCRM) || crm.contains("M"))
					{
						capOrSysDataList.add(data);
					}
				}
				else
				{
					if(crm.contains(searchCRM))
					{
						capOrSysDataList.add(data);
					}
				}
			}
		}

		for( String data : capOrSysDataList)
		{
			Hashtable<String, String> innerHash = getList.get(data);
			if(innerHash != null)
			{
				for( String sys : innerHash.keySet())
				{
					if(getCRM.contains("C"))
					{				
						if(innerHash.get(sys).contains(getCRM) || innerHash.get(sys).contains("M"))
						{
							if(!resultSet.contains(data))
								resultSet.add(data);
						}
					}
					else
					{
						if(innerHash.get(sys).contains(getCRM))
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
	
	public ArrayList<String> getAllDataFromSys(String sys, String crm)
	{
		return processAllDataFromSysOrCap(sys, crm, dataListSystems);
	}
	
	public ArrayList<String> getAllDataFromCap(String cap, String crm)
	{
		return processAllDataFromSysOrCap(cap, crm, dataListCapabilities);
	}
	
	
	private ArrayList<String> processAllDataFromSysOrCap(String capOrSys, String crm, Hashtable<String, Hashtable<String, String>> dataList)
	{
		ArrayList<String> resultSet = new ArrayList<String>();
		
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

	private void processAllResults(ISelectWrapper sjsw, boolean sys)
	{
		if(sys)
		{
			dataListSystems = processResults(sjsw, dataListSystems);
		}
		else
		{
			dataListCapabilities = processResults(sjsw, dataListCapabilities);
		}
	}
	
	private  Hashtable<String, Hashtable<String, String>> processResults(ISelectWrapper sjsw, Hashtable<String, Hashtable<String, String>> dataList)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String sub = sjss.getVar(vars[0]).toString();
			String obj = sjss.getVar(vars[1]).toString();
			String crm = sjss.getVar(vars[2]).toString();

			if(!dataList.containsKey(obj))
			{
				Hashtable<String, String> innerHash = new Hashtable<String, String>();
				innerHash.put(sub, crm);
				dataList.put(obj, innerHash);
			}
			else if(!dataList.get(obj).containsKey(sub))
			{
				Hashtable<String, String> innerHash = dataList.get(obj);
				innerHash.put(sub,  crm);
			}
			else
			{
				Hashtable<String, String> innerHash = dataList.get(obj);
				if((crm.contains("C") || crm.contains("M")) && innerHash.get(sub).contains("R"))
				{
					innerHash.put(sub, crm);
				}
			}
		}
		return dataList;
	}

	//process the query
	private ISelectWrapper processQuery(String query, IEngine engine){
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();		
		sjsw.getVariables();
		return sjsw;*/
		return wrapper;
	}
}
