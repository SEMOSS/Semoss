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

import java.util.HashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.error.EngineException;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class InsertInterfaceModernizationProperty {

	static final Logger LOGGER = LogManager.getLogger(InsertInterfaceModernizationProperty.class.getName());

	private final String sysURIPrefix = "http://health.mil/ontologies/Concept/System/";
	private final String costPropertyURI = "http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost";

	private IEngine HR_Core;

	public void insert() throws EngineException
	{
		try{
			HR_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
			if(HR_Core==null)
				throw new EngineException("Database not found");
		} catch(EngineException e) {
			Utility.showError("Could not find necessary database: HR_Core. Cannot generate report.");
			return;
		}
		getCostFromInterfaceReport();
	}

	private void getCostFromInterfaceReport() throws EngineException 
	{
		HashMap<String,String> reportTypeHash = DHMSMTransitionUtility.processReportTypeQuery(HR_Core);
		LPInterfaceProcessor processor = new LPInterfaceProcessor();

		IEngine TAP_Cost_Data = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(TAP_Cost_Data == null) {
			throw new EngineException("TAP_Cost_Data Database not found");
		}
		
		processor.setEngine(HR_Core);
		processor.getCostInfo(TAP_Cost_Data);
		for(String sysName : reportTypeHash.keySet()){
			sysName = sysName.replaceAll("\\(", "\\\\\\\\\\(").replaceAll("\\)", "\\\\\\\\\\)");
			processor.setDownstreamQuery(DHMSMTransitionUtility.lpSystemDownstreamInterfaceQuery.replace("@SYSTEMNAME@", sysName));
			processor.setUpstreamQuery(DHMSMTransitionUtility.lpSystemUpstreamInterfaceQuery.replace("@SYSTEMNAME@", sysName));
			processor.isGenerateCost(true);
			processor.generateReport();
			
			Object cost = (Double) processor.getTotalDirectCost();
			if(cost == null) {
				cost = "NA";
			}
			addProperty(sysURIPrefix.concat(sysName), costPropertyURI, cost, false);
		}
	}

	private void addProperty(String sub, String pred, Object obj, boolean concept_triple) 
	{
		( (BigDataEngine) HR_Core).addStatement(new Object[]{sub, pred, obj, concept_triple});
		( (BigDataEngine) HR_Core).commit();
		System.out.println(sub + " >>> " + pred + " >>> " + obj);
	}
}
