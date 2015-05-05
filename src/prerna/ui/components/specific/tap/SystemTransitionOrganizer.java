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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;

public class SystemTransitionOrganizer {
	protected static final Logger logger = LogManager.getLogger(SystemTransitionOrganizer.class.getName());
	private static String healthMilDataURI = "http://health.mil/ontologies/Concept/DataObject";
	private static String siteDB = "TAP_Site_Data";
	private static String costDB = "TAP_Cost_Data";
	//private static String coreDB = "TAP_Core_Data";
	private static String hrCoreDB = "HR_Core";
	private static String systemProbQuery = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?HighProb} FILTER(?HighProb in('High','Question'))}";
	private static String systemSiteQuery = "SELECT DISTINCT ?System ?DCSite WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;}  {?SystemDCSite ?DeployedAt ?DCSite;}{?System ?DeployedAt1 ?SystemDCSite;} }";
	private static String systemCostQuery = "SELECT DISTINCT ?sys ?data ?ser (SUM(?loe) AS ?cost) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase>} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass}{?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start}  {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser }{?data <http://semoss.org/ontologies/Relation/Input> ?GLitem}} GROUP BY ?sys ?data ?ser BINDINGS ?data {@DATABINDINGS@}";
	private static String systemDataQuery = "SELECT DISTINCT ?System ?Data ?CRM WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .} }BINDINGS ?Data {@DATABINDINGS@}";
	//private static String capDataQuery = "SELECT DISTINCT ?Capability ?Data ?Crm WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} {?Capability <http://semoss.org/ontologies/Relation/Contains/Source> \"HSD\"}}";
    private static String siteLocationQuery = "SELECT DISTINCT ?dcSite ?lat ?long WHERE { {?dcSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?dcSite <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat }  {?dcSite <http://semoss.org/ontologies/Relation/Contains/LONG> ?long } }";
    
    ArrayList<String> highProbSystemsList = new ArrayList<String>();
    //hashtable storing all the sites and the systems they have
    //and the reverse, hashtable storing all the systems and their sites
    Hashtable<String, ArrayList<String>> siteToSysHash = new Hashtable<String, ArrayList<String>>();
    Hashtable<String, ArrayList<String>> sysToSiteHash = new Hashtable<String, ArrayList<String>>();
    
    //hashtable storing all the systems, their data objects, and their costs
    Hashtable<String, Hashtable<String,Double>> sysToDataToCostHash = new Hashtable<String, Hashtable<String,Double>>();
    
    //hashtable storing all the systems, their data objects they read, and whether they claimed to be C or R
    Hashtable<String, Hashtable<String,String>> sysReadDataHash = new Hashtable<String, Hashtable<String,String>>();
	
    //hashtable storing lat and long for each site
    Hashtable<String, Hashtable<String, Double>> siteLatLongHash = new Hashtable<String, Hashtable<String, Double>>();
	
	public SystemTransitionOrganizer(ArrayList<String> dataBindings)
	{
		addBindingToQueries(dataBindings);
		createAllData();
	}
	
	private void addBindingToQueries(ArrayList<String> dataBindings)
	{
		String bindingString ="";
		for (String data : dataBindings)
		{
			bindingString = bindingString+"(<" + healthMilDataURI + "/"+data + ">)";
		}
		systemCostQuery=systemCostQuery.replace("@DATABINDINGS@", bindingString);
		systemDataQuery=systemDataQuery.replace("@DATABINDINGS@", bindingString);
	}
	
	private void createAllData()
	{
		ArrayList <Object []> list = createData(hrCoreDB, systemProbQuery);
		processHighProbSystemList(list);
		list = createData(siteDB, systemSiteQuery);
		processSystemSiteHashTables(list);
		list = createData(costDB, systemCostQuery);
		processSystemDataLOE(list);
		list = createData(hrCoreDB, systemDataQuery);
		processSystemReadData(list);
		list = createData(siteDB, siteLocationQuery);
		processSiteLocationData(list);
	}
	
	private void processHighProbSystemList(ArrayList<Object[]> list)
	{
		for (int i=0; i<list.size(); i++)
		{
			highProbSystemsList.add((String)list.get(i)[0]);
		}
	}
	
	private void processSiteLocationData(ArrayList<Object[]> list) 
	{
		for (int i=0; i<list.size(); i++)
		{
			try{
			Object[] elementArray = list.get(i);
			String site = (String) elementArray[0];

			Double latVal;
			Double longVal;
			if(elementArray[1] instanceof Double)
				latVal = (Double) elementArray[1];
			else
				latVal = Double.parseDouble((String)elementArray[1]);
			if(elementArray[2] instanceof Double)
				longVal = (Double) elementArray[2];
			else
				longVal = Double.parseDouble((String)elementArray[2]);
			Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
			innerHash.put("LAT", latVal);
			innerHash.put("LONG", longVal);
			siteLatLongHash.put(site, innerHash);
			}
			catch(RuntimeException e)
			{
				Object[] elementArray = list.get(i);
				logger.error("Problem with longitude and latitude for "+(String)elementArray[0]);
			}
		}
	}

	private void processSystemSiteHashTables(ArrayList <Object []> list)
	{
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			if(highProbSystemsList.contains(system))
			{
				String site = (String) elementArray[1];
				if(siteToSysHash.containsKey(site))
				{
					ArrayList<String> sysList = siteToSysHash.get(site);
					sysList.add(system);
				}
				else
				{
					ArrayList<String> sysList = new ArrayList<String>();
					sysList.add(system);
					siteToSysHash.put(site, sysList);
				}
				if(sysToSiteHash.containsKey(system))
				{
					ArrayList<String> siteList = sysToSiteHash.get(system);
					siteList.add(site);
				}
				else
				{
					ArrayList<String> siteList = new ArrayList<String>();
					siteList.add(site);
					sysToSiteHash.put(system, siteList);
				}
			}
		}
	}
	
	private void processSystemDataLOE(ArrayList <Object []> list)
	{
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String data = (String) elementArray[1];
			String ser = (String) elementArray[2];
			Double loe = (Double) elementArray[3];
			if(sysToDataToCostHash.containsKey(system))
			{
				Hashtable<String, Double> dataCostHash = sysToDataToCostHash.get(system);
				dataCostHash.put(data+"$"+ser, loe);
			}
			else
			{
				Hashtable<String, Double> dataCostHash = new Hashtable<String, Double> ();
				dataCostHash.put(data+"$"+ser,  loe);
				sysToDataToCostHash.put(system, dataCostHash);
			}
		}
	}
	
	private void processSystemReadData(ArrayList <Object []> list)
	{
		System.out.println(systemDataQuery);
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String data = (String) elementArray[1];
			String crm = (String) elementArray[2];
			if(sysReadDataHash.containsKey(system))
			{
				Hashtable<String, String> dataReadHash = sysReadDataHash.get(system);
				if(sysToDataToCostHash.get(system)!=null&&sysToDataToCostHash.get(system).containsKey(data))
				{
					dataReadHash.put(data, crm);
				}
			}
			else
			{
				Hashtable<String, String> dataReadHash = new Hashtable<String, String>();
				if(sysToDataToCostHash.get(system)!=null&&sysToDataToCostHash.get(system).containsKey(data))
				{
					dataReadHash.put(data, crm);
					sysReadDataHash.put(system,dataReadHash);
				}
			}
		}
	}
	
	public ArrayList <Object []> createData(String engineName, String query) {
		
		ArrayList <Object []> list = new ArrayList<Object[]>();

		//SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		/*wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.setEngineType(IEngine.ENGINE_TYPE.SESAME);
		try{
			wrapper.executeQuery();	
		}
		catch (RuntimeException e)
		{
			e.printStackTrace();
		}*/		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		// get the bindings from it
		String[] names = wrapper.getVariables();
		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();

				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = getVariable(names[colIndex], sjss);
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}
				logger.debug("Creating new Value " + values);
				list.add(count, values);
				count++;
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
		return list;
	}
	
	public Object getVariable(String varName, ISelectStatement sjss){
		return sjss.getVar(varName);
	}
	
	public Hashtable<String, ArrayList<String>> getSiteToSysHash()
	{
		return siteToSysHash;
	}
	
	public Hashtable<String, ArrayList<String>> getSysToSiteHash()
	{
		return sysToSiteHash;
	}
	
	public Hashtable<String, Hashtable<String,Double>> getSysDataLOEHash()
	{
		return sysToDataToCostHash;
	}
	
	public Hashtable<String, Hashtable<String,String>> getSysReadDataHash()
	{
		return sysReadDataHash;
	}
	
	public Hashtable<String, Hashtable<String, Double>> getSiteLatLongHash()
	{
		return siteLatLongHash;
	}

}
