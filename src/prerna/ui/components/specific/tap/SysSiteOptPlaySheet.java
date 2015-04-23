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

import prerna.algorithm.impl.specific.tap.SysOptUtilityMethods;
import prerna.algorithm.impl.specific.tap.SysSiteOptimizer;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.google.gson.Gson;

/**
 * The GridPlaySheet class creates the panel and table for a grid view of data from a SPARQL query.
 */
@SuppressWarnings("serial")
public class SysSiteOptPlaySheet extends BasicProcessingPlaySheet{
	
	SysOptCheckboxListUpdater sysUpdater;
	SysSiteOptimizer opt;
	
	/**
	 * Method addPanel.  Creates a panel and adds the table to the panel.
	 */
	private static final Logger LOGGER = LogManager.getLogger(SysSiteOptPlaySheet.class.getName());
	
	public SysSiteOptPlaySheet() {
		sysUpdater = new SysOptCheckboxListUpdater(engine, true, false, false);
	}
	
	public Hashtable getSystems(Hashtable<String, Object> webDataHash) {
		Hashtable returnHash = (Hashtable) super.getData();
		Hashtable dataHash = new Hashtable();
		
		Gson gson = new Gson();
		Boolean low = gson.fromJson(gson.toJson(webDataHash.get("low")), Boolean.class);
		Boolean high = gson.fromJson(gson.toJson(webDataHash.get("high")), Boolean.class);
		Boolean intDHMSM = gson.fromJson(gson.toJson(webDataHash.get("interface")), Boolean.class);
		Boolean notIntDHMSM = gson.fromJson(gson.toJson(webDataHash.get("nointerface")), Boolean.class);
		Boolean theater;
		Boolean garrison;
		Boolean mhsSpecific;
		Boolean ehrCore;
		
		if(webDataHash.get("theater") == null)
			theater = false;
		else
			theater = gson.fromJson(gson.toJson(webDataHash.get("theater")), Boolean.class);
		if(webDataHash.get("garrison") == null)
			garrison = false;
		else
			garrison = gson.fromJson(gson.toJson(webDataHash.get("garrison")), Boolean.class);
		if(webDataHash.get("mhsspecific") == null)
			mhsSpecific = false;
		else
			mhsSpecific = gson.fromJson(gson.toJson(webDataHash.get("mhsspecific")), Boolean.class);
		if(webDataHash.get("ehrcore") == null)
			ehrCore = false;
		else
			ehrCore = gson.fromJson(gson.toJson(webDataHash.get("ehrcore")), Boolean.class);
		
		dataHash.put("systems",sysUpdater.getSelectedSystemList(intDHMSM, notIntDHMSM, theater, garrison, low, high, mhsSpecific, ehrCore));
		
		if (dataHash != null)
			returnHash.put("data", dataHash);
		return returnHash;
	}
	
	@Override
	public void createData() {
		
		//check to make sure site engine is loaded
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		if(siteEngine == null) {
			Utility.showError("Cannot find database: " + "TAP_Site_Data");
			return;

		}
		
		//all systems to consider: must have received information, not be a device, and have a sustainment budget > 0.
		String sysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'}{?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0)} ORDER BY ?System";
		ArrayList<String> sysList = SysOptUtilityMethods.runListQuery(engine, sysQuery);
		
		//all systems that must be modernized: same as above (received info, not devices and have sustainment budgets > 0) AND must be MHS Specific or LPI
		String sysModQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?System <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'}{?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0){{?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> 'Y'} }UNION{{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'Low'}{?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'}}} ORDER BY ?System";
		ArrayList<String> sysModList = SysOptUtilityMethods.runListQuery(engine, sysModQuery);
		
		//all systems that must be decomissioned: same as above (received info, not devices and have sustainment budgets > 0) AND be EHR Core
		String sysDecomQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'}{?System <http://semoss.org/ontologies/Relation/Contains/EHR_Core> 'YN'}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0)} ORDER BY ?System";
		ArrayList<String> sysDecomList = SysOptUtilityMethods.runListQuery(engine, sysDecomQuery);
		
		//check to make sure user did not put the same system in the mod list and the decom list... may not be issue on web depending how UI is implemented
		ArrayList<String> duplicates = SysOptUtilityMethods.inBothLists(sysModList, sysDecomList);
		if (!duplicates.isEmpty()) {
			Utility.showError("There is at least one system on the manually modernize and manually decommission. Please resolve the lists for systems: " + duplicates.toString());
			return;
		}

		
		//actually running the algorithm
		opt = new SysSiteOptimizer();
		opt.setEngines(engine, siteEngine); //likely hr core and tap site
		opt.setVariables(10000, 10); //budget per year and the number of years
		opt.setAdvancedVariables(1.5, 2.5, 1); //OPTIONAL: there are default values if not selected, inflation rate = 1.5, discount rate = 2.5, and number of starting points = 1
		opt.setUseDHMSMFunctionality(false); //whether the data objects will come from the list of systems or the dhmsm provided capabilities
		opt.setOptimizationType("irr"); //eventually will be savings, roi, or irr
		opt.setIsOptimizeBudget(true); //true means that we are looking for optimal budget. false means that we are running LPSolve just for the single budget input
		opt.setSysList(sysList); //list of all systems to use in analysis
		opt.setMustModDecomList(sysModList, sysDecomList); //list of systems to force modernize/decommision. Decommision is not implemented yet
		opt.execute();
		
	}
	@Override
	public void runAnalytics() {
		
	}
	
	@Override
	public void createView() {
		opt.display();
	}
}
