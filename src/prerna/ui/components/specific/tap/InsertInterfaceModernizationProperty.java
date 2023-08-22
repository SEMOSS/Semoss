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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import prerna.engine.api.IDatabaseEngine;
import prerna.ui.components.specific.tap.AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class InsertInterfaceModernizationProperty {

	static final Logger LOGGER = LogManager.getLogger(InsertInterfaceModernizationProperty.class.getName());

	private final String sysURIPrefix = "http://health.mil/ontologies/Concept/System/";
	private final String costPropertyURI = "http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost";

	private IDatabaseEngine tapCore;
	double costPerHr = 150.0;

	public void insert() throws IOException
	{
		try{
			tapCore = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
			if(tapCore==null)
				throw new IOException("Database not found");
		} catch(IOException e) {
			Utility.showError("Could not find necessary database: TAP_Core_Data. Cannot generate report.");
			return;
		}
		generateCost();
	}

	private void generateCost() throws IOException 
	{
		IDatabaseEngine tapCost = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(tapCost == null) {
			throw new IOException("TAP_Cost_Data Database not found");
		}

		IDatabaseEngine futureDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureDB");
		if(futureDB == null) {
			throw new IOException("FutureDB Database not found");
		}
		
		IDatabaseEngine futureCostDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureCostDB");
		if(futureCostDB == null) {
			throw new IOException("FutureDB Database not found");
		}
		
		Map<String, Double> selfReportedSysCost = DHMSMTransitionUtility.getSystemSelfReportedP2PCost(futureCostDB, tapCost);
		Set<String> selfReportedSystems = DHMSMTransitionUtility.getAllSelfReportedSystemNames(futureDB);
		Set<String> sorV = DHMSMTransitionUtility.processSysDataSOR(tapCore);
		Map<String, String> sysTypeHash = DHMSMTransitionUtility.processReportTypeQuery(tapCore);
		
		LPInterfaceCostProcessor processor = new LPInterfaceCostProcessor();
		processor.setEngine(tapCore);
		for(String sysName : sysTypeHash.keySet()){
			double loe = 0.0;
			if(selfReportedSystems.contains(sysName)) {
				loe = selfReportedSysCost.get(sysName);
			} else {
				loe = processor.generateSystemCost(sysName, selfReportedSystems, sorV, sysTypeHash, COST_FRAMEWORK.P2P); //TODO: pass in p2p
			}
			loe = loe * costPerHr;
			addProperty(sysURIPrefix.concat(sysName), costPropertyURI, loe, false);
		}
		tapCore.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{costPropertyURI, RDF.TYPE, "http://semoss.org/ontolgoies/Relation/Contains"});
		tapCore.commit();
	}

	private void addProperty(String sub, String pred, Object obj, boolean concept_triple) {
		tapCore.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, pred, obj, concept_triple});
		System.out.println(sub + " >>> " + pred + " >>> " + obj);
	}
}
