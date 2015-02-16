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

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class SysDHMSMInfoAtSitePlaySheet extends GridPlaySheet {

	private static final Logger logger = LogManager.getLogger(SysDHMSMInfoAtSitePlaySheet.class.getName());
	private String GET_SYSTEMS_AT_SITE = "SELECT DISTINCT ?DCSite ?System WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite;} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite;} } ORDER BY ?DCSite";
	private String siteEngineName = "TAP_Site_Data";

	private String GET_SYSTEM_INFO = "SELECT DISTINCT ?System ?Probability ?Integrate WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> ?Integrate} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} }";
	private String hrCoreEngineName = "HR_Core";

	private Hashtable<String, Hashtable<String, Integer>> dataToAdd = new Hashtable<String, Hashtable<String, Integer>>();

	@Override
	public void createData() {
		list = new ArrayList<Object[]>();
		Hashtable<String, ArrayList<String>> siteData = new Hashtable<String, ArrayList<String>>();
		siteData = runSiteQuery(); 
		if(siteData.keySet().size() != 0) {
			Hashtable<String, Hashtable<String, String>> hrCoreData = new Hashtable<String, Hashtable<String, String>>();
			hrCoreData = runHRCoreQuery();
			if(hrCoreData.keySet().size() != 0) {
				list = processQuery(siteData, hrCoreData);
			}
		}
	}

	private ArrayList<Object[]> processQuery(Hashtable<String, ArrayList<String>> siteData, Hashtable<String, Hashtable<String, String>> hrCoreData) {
		ArrayList<Object[]> newList = new ArrayList<Object[]>();

		for(String site : siteData.keySet())
		{
			ArrayList<String> sysAtSiteList = siteData.get(site);
			for(String sysAtSite : sysAtSiteList)
			{
				Hashtable<String, String> sysPropHash = hrCoreData.get(sysAtSite);
				if(sysPropHash != null)
				{
					String probability = sysPropHash.get("Probability");
					String integrate = sysPropHash.get("Integrate");

					if(!dataToAdd.containsKey(site))
					{
						Hashtable<String, Integer> innerHash = new Hashtable<String, Integer>();
						innerHash.put("LPI_Count", (Integer) 0);
						innerHash.put("HPS_Count", (Integer) 0);
						innerHash.put("LPNI_Count", (Integer) 0);

						if(probability.equals("High")) 
						{
							Integer HPS_Count = innerHash.get("HPS_Count");
							HPS_Count = HPS_Count + 1;
							innerHash.put("HPS_Count", HPS_Count);
						}

						if(probability.equals("Low") && integrate.equals("Yes"))
						{
							Integer LPI_Count = innerHash.get("LPI_Count");
							LPI_Count = LPI_Count + 1;
							innerHash.put("LPI_Count", LPI_Count);
						} 

						if(probability.equals("Low") && integrate.equals("No"))
						{
							Integer LPNI_Count = innerHash.get("LPNI_Count");
							LPNI_Count = LPNI_Count + 1;
							innerHash.put("LPNI_Count", LPNI_Count);
						}

						dataToAdd.put(site, innerHash);
					} else {
						Hashtable<String, Integer> innerHash = dataToAdd.get(site);

						if(probability.equals("High")) 
						{
							Integer HPS_Count = innerHash.get("HPS_Count");
							HPS_Count = HPS_Count + 1;
							innerHash.put("HPS_Count", HPS_Count);
						}

						if(probability.equals("Low") && integrate.equals("Yes"))
						{
							Integer LPI_Count = innerHash.get("LPI_Count");
							LPI_Count = LPI_Count + 1;
							innerHash.put("LPI_Count", LPI_Count);
						} 

						if(probability.equals("Low") && integrate.equals("No"))
						{
							Integer LPNI_Count = innerHash.get("LPNI_Count");
							LPNI_Count = LPNI_Count + 1;
							innerHash.put("LPNI_Count", LPNI_Count);
						}
					}
				}
			}
		}

		for(String site : dataToAdd.keySet()) {
			Hashtable<String, Integer> innerHash = dataToAdd.get(site);
			Integer HPS_Count = innerHash.get("HPS_Count");
			Integer LPI_Count = innerHash.get("LPI_Count");
			Integer LPNI_Count = innerHash.get("LPNI_Count");

			newList.add(new Object[]{site, LPI_Count, LPNI_Count, HPS_Count});

		}
		// add the new column in output to the names array
		String[] newNames = new String[]{"Site", "Low Prob Integrated Count", "Low Prob Not Integrate Count", "High Prob Count"};
		names = newNames;

		return newList;
	}

	private Hashtable<String, Hashtable<String, String>> runHRCoreQuery() {
		Hashtable<String, Hashtable<String, String>> hrCoreData = new Hashtable<String, Hashtable<String, String>>();

		logger.info("PROCESSING QUERY: " + GET_SYSTEM_INFO);
		

		//SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		IEngine hrCoreEngine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngineName);


		if(hrCoreEngineName != null)
		{
			ISelectWrapper sjsw = WrapperManager.getInstance().getSWrapper(hrCoreEngine, GET_SYSTEM_INFO);
			
			/*sjsw.setEngine(hrCoreEngine);
			sjsw.setQuery(GET_SYSTEM_INFO);
			sjsw.executeQuery();
			*/
			names = sjsw.getVariables();

			while(sjsw.hasNext())
			{
				ISelectStatement sjss = sjsw.next();
				String sys = sjss.getVar(names[0]).toString();
				String prob = sjss.getVar(names[1]).toString();
				String integrate = sjss.getVar(names[2]).toString();
				sys = sys.replace("\"", "");
				prob = prob.replace("\"", "");
				integrate = integrate.replace("\"", "");

				if(prob.equals("Question"))
				{
					prob = "High";
				} else if (prob.equals("Medium") || prob.equals("Medium-High"))
				{
					prob = "Low";
				}

				if(integrate.equals("Y"))
				{
					integrate = "Yes";
				} else {
					integrate = "No";
				}

				Hashtable<String, String> innerHash = new Hashtable<String, String>();
				innerHash.put("Probability", prob);
				innerHash.put("Integrate", integrate);

				hrCoreData.put(sys, innerHash);
			}
			return hrCoreData;
		}  else {
			Utility.showError("Cannot find HR_Core database");
		}
		return hrCoreData;
	}

	private Hashtable<String, ArrayList<String>> runSiteQuery() {
		Hashtable<String, ArrayList<String>> siteData = new Hashtable<String, ArrayList<String>>();

		logger.info("PROCESSING QUERY: " + GET_SYSTEMS_AT_SITE);
		//SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the site engine provided
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp(siteEngineName);

		if(siteEngine != null)
		{
			ISelectWrapper sjsw = WrapperManager.getInstance().getSWrapper(siteEngine, GET_SYSTEMS_AT_SITE);

			/*sjsw.setEngine(siteEngine);
			sjsw.setQuery(GET_SYSTEMS_AT_SITE);
			sjsw.executeQuery();
			*/
			
			names = sjsw.getVariables();

			while(sjsw.hasNext())
			{
				ISelectStatement sjss = sjsw.next();
				String site = sjss.getVar(names[0]).toString();
				String sys = sjss.getVar(names[1]).toString();
				site = site.replace("\"", "");
				sys = sys.replace("\"", "");
				
				if(!siteData.containsKey(site)) {
					ArrayList<String> sysList = new ArrayList<String>();
					sysList.add(sys);
					siteData.put(site, sysList);
				} else {
					ArrayList<String> sysList = siteData.get(site);
					sysList.add(sys);
				}
			}
			return siteData;
		} else {
			Utility.showError("Cannot find TAP_Site_Data database");
		}
		return siteData;
	}

}
