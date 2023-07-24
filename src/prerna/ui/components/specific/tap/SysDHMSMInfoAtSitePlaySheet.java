/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabase;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class SysDHMSMInfoAtSitePlaySheet extends GridPlaySheet {

	//TODO does anyone actually use this playsheet?
	private static final Logger logger = LogManager.getLogger(SysDHMSMInfoAtSitePlaySheet.class.getName());
	private String GET_SYSTEMS_AT_SITE = "SELECT DISTINCT ?DCSite ?System WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite;} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite;} } ORDER BY ?DCSite";
	private String tapSiteDB = "TAP_Site_Data";
	private IDatabase tapSiteEngine;

	private String GET_SYSTEM_INFO = "SELECT DISTINCT ?System ?Disposition WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/Disposition> ?Disposition} {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))}";
	private String tapCoreDB = "TAP_Core_Data";
	private IDatabase tapCoreEngine;

	private Hashtable<String, Hashtable<String, Integer>> dataToAdd = new Hashtable<String, Hashtable<String, Integer>>();

	List<Object[]> list;
	String[] names;
	
	@Override
	public List<Object[]> getList() {
		return this.list;
	}
	
	@Override
	public String[] getNames() {
		return this.names;
	}
	
	@Override
	public void createData() {
		//checking to see if databases are loaded
		try
		{
			tapCoreEngine = (IDatabase) DIHelper.getInstance().getLocalProp(tapCoreDB);
			tapSiteEngine = (IDatabase) DIHelper.getInstance().getLocalProp(tapSiteDB);
			if(tapCoreEngine == null || tapSiteEngine==null)
				throw new NullPointerException();
		} catch (NullPointerException e) {
			Utility.showError("Cannot find TAP Core Data or TAP Site engine.");
			return;
		}

		list = new ArrayList<Object[]>();
		Hashtable<String, ArrayList<String>> siteData = new Hashtable<String, ArrayList<String>>();
		siteData = runSiteQuery(); 
		if(siteData.keySet().size() != 0) {
			Hashtable<String, String> tapCoreData = new Hashtable<String, String>();
			tapCoreData = runTapCoreQuery();
			if(tapCoreData.keySet().size() != 0) {
				list = processQuery(siteData, tapCoreData);
			}
		}
	}

	private ArrayList<Object[]> processQuery(Hashtable<String, ArrayList<String>> siteData, Hashtable<String, String> tapCoreData) {
		ArrayList<Object[]> newList = new ArrayList<Object[]>();

		for(String site : siteData.keySet())
		{
			ArrayList<String> sysAtSiteList = siteData.get(site);
			for(String sysAtSite : sysAtSiteList)
			{
				String disposition = tapCoreData.get(sysAtSite);
				if(disposition != null)
				{
					if(!dataToAdd.containsKey(site))
					{
						Hashtable<String, Integer> innerHash = new Hashtable<String, Integer>();
						innerHash.put("LPI_Count", (Integer) 0);
						innerHash.put("HPS_Count", (Integer) 0);
						innerHash.put("LPNI_Count", (Integer) 0);

						if(disposition.equals("High")) 
						{
							Integer HPS_Count = innerHash.get("HPS_Count");
							HPS_Count = HPS_Count + 1;
							innerHash.put("HPS_Count", HPS_Count);
						}

						if(disposition.equals("LPI"))
						{
							Integer LPI_Count = innerHash.get("LPI_Count");
							LPI_Count = LPI_Count + 1;
							innerHash.put("LPI_Count", LPI_Count);
						} 

						if(disposition.equals("LPNI"))
						{
							Integer LPNI_Count = innerHash.get("LPNI_Count");
							LPNI_Count = LPNI_Count + 1;
							innerHash.put("LPNI_Count", LPNI_Count);
						}

						dataToAdd.put(site, innerHash);
					} else {
						Hashtable<String, Integer> innerHash = dataToAdd.get(site);

						if(disposition.equals("High")) 
						{
							Integer HPS_Count = innerHash.get("HPS_Count");
							HPS_Count = HPS_Count + 1;
							innerHash.put("HPS_Count", HPS_Count);
						}

						if(disposition.equals("LPI"))
						{
							Integer LPI_Count = innerHash.get("LPI_Count");
							LPI_Count = LPI_Count + 1;
							innerHash.put("LPI_Count", LPI_Count);
						} 

						if(disposition.equals("LPNI"))
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
		String[] newNames = new String[]{"Site", "LPI Count", "LPNI Count", "High Count"};
		names = newNames;

		return newList;
	}

	private Hashtable<String, String> runTapCoreQuery() {
		Hashtable<String, String> tapCoreData = new Hashtable<String, String>();

		logger.info("PROCESSING QUERY: " + GET_SYSTEM_INFO);
		

		ISelectWrapper sjsw = WrapperManager.getInstance().getSWrapper(tapCoreEngine, GET_SYSTEM_INFO);
		
		names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString();
			String disposition = sjss.getVar(names[1]).toString();
			sys = sys.replace("\"", "");
			disposition = disposition.replace("\"", "");

			tapCoreData.put(sys, disposition);
		}
		return tapCoreData;
	}

	private Hashtable<String, ArrayList<String>> runSiteQuery() {
		Hashtable<String, ArrayList<String>> siteData = new Hashtable<String, ArrayList<String>>();

		logger.info("PROCESSING QUERY: " + GET_SYSTEMS_AT_SITE);
		ISelectWrapper sjsw = WrapperManager.getInstance().getSWrapper(tapSiteEngine, GET_SYSTEMS_AT_SITE);
			
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
	}

}
