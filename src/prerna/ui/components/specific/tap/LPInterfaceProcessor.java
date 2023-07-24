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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabase;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;

public class LPInterfaceProcessor extends AbstractLPInterfaceProcessor{
	
	private static final Logger LOGGER = LogManager.getLogger(LPInterfaceProcessor.class.getName());
	
	public void setEngine(IDatabase engine) {
		this.engine = engine;
	}
	
	public Map<String, Object> generateGridReport() {
		Set<String> selfReportedSystems = new HashSet<String>();
		IDatabase futureDB = (IDatabase) DIHelper.getInstance().getLocalProp("FutureDB");
		if(futureDB != null) {
			selfReportedSystems = DHMSMTransitionUtility.getAllSelfReportedSystemNames(futureDB);
		}
		Set<String> sorV = DHMSMTransitionUtility.processSysDataSOR(engine);
		Map<String, String> sysTypeHash = DHMSMTransitionUtility.processReportTypeQuery(engine);

		List<Object[]> retList = new ArrayList<Object[]>();
		// Process main query
		ISelectWrapper wrapper1 = WrapperManager.getInstance().getSWrapper(engine, upstreamQuery);
		ISelectWrapper wrapper2 = WrapperManager.getInstance().getSWrapper(engine, downstreamQuery);
		
		ISelectWrapper[] wrappers = new ISelectWrapper[]{wrapper1, wrapper2};
		String[] headers = wrapper1.getVariables();
		Set<String> consumeSet = new HashSet<String>();
		Set<String> provideSet = new HashSet<String>();
		for(ISelectWrapper wrapper : wrappers) {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				// get var's
				String sysName = "";
				String interfaceType = "";
				String interfacingSysName = "";
				String interfacingSysDisposition = "";
				String icd = "";
				String data = "";
				String format = "";
				String freq = "";
				String prot = "";
				String dhmsmSOR = "";
				
				String sysProbability = sysTypeHash.get(sysName);
				if (sysProbability == null || sysProbability.equals("TBD")) {
					sysProbability = "No Probability";
				}
				if (sjss.getVar(SYS_KEY) != null) {
					sysName = sjss.getVar(SYS_KEY).toString();
				}
				if (sjss.getVar(INTERFACE_TYPE_KEY) != null) {
					interfaceType = sjss.getVar(INTERFACE_TYPE_KEY).toString();
				}
				if (sjss.getVar(INTERFACING_SYS_KEY) != null) {
					interfacingSysName = sjss.getVar(INTERFACING_SYS_KEY).toString();
				}
				if (sjss.getVar(DISPOSITION_KEY) != null) {
					interfacingSysDisposition = sjss.getVar(DISPOSITION_KEY).toString();
				}
				if (sjss.getVar(ICD_KEY) != null) {
					icd = sjss.getVar(ICD_KEY).toString();
				}
				if (sjss.getVar(DATA_KEY) != null) {
					data = sjss.getVar(DATA_KEY).toString();
				}
				if (sjss.getVar(FORMAT_KEY) != null) {
					format = sjss.getVar(FORMAT_KEY).toString();
				}
				if (sjss.getVar(FREQ_KEY) != null) {
					freq = sjss.getVar(FREQ_KEY).toString();
				}
				if (sjss.getVar(PROT_KEY) != null) {
					prot = sjss.getVar(PROT_KEY).toString();
				}
				if (sjss.getVar(DHMSM) != null) {
					dhmsmSOR = sjss.getVar(DHMSM).toString();
				}
				
				FutureStateInterfaceResult result = FutureStateInterfaceProcessor.processICD(
						sysName, 
						interfaceType, 
						interfacingSysName, 
						icd, 
						data, 
						dhmsmSOR, 
						selfReportedSystems, 
						sorV, 
						sysTypeHash,
						consumeSet,
						provideSet);
				
				Object[] newRow = new Object[headers.length];
				newRow[0] = sysName;
				newRow[1] = interfaceType;
				newRow[2] = interfacingSysName;
				newRow[3] = interfacingSysDisposition;
				newRow[4] = icd;
				newRow[5] = data;
				newRow[6] = format;
				newRow[7] = freq;
				newRow[8] = prot;
				newRow[9] = dhmsmSOR;
				newRow[10] = result.get(FutureStateInterfaceResult.COMMENT);
				if(newRow[10] == null || newRow[10].toString().isEmpty()) {
					newRow[10] = "System has defined it's own interfaces in future state.";
				} else if(result.isCostTakenIntoConsideration()) {
					newRow[10] = "Interface cost already taken into consideration.";
				}
				retList.add(newRow);
			}
		}
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("data", retList);
		retMap.put("headers", headers);
		return retMap;
	}
}
