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
import java.util.HashMap;
import java.util.List;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class EAFunctionalGapPlaySheet extends GridPlaySheet {
	private IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
	
	private final String getEHRDataQuery = "SELECT DISTINCT ?data WHERE {{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?system ?provide ?data}} UNION {{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?provide <http://semoss.org/ontologies/Relation/Contains/SOR> 'Yes'}{?system ?provide ?data}}}} BINDINGS ?system {(<http://health.mil/ontologies/Concept/System/AHLTA>)(<http://health.mil/ontologies/Concept/System/CHCS>)(<http://health.mil/ontologies/Concept/System/CIS-Essentris>)}";
	private final String getEHRBLUQuery = "SELECT DISTINCT ?blu WHERE {{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?system <http://semoss.org/ontologies/Relation/Provide> ?blu}} BINDINGS ?system {(<http://health.mil/ontologies/Concept/System/AHLTA>)(<http://health.mil/ontologies/Concept/System/CHCS>)(<http://health.mil/ontologies/Concept/System/CIS-Essentris>)}";
	private final String getDHMSMDataQuery = "SELECT DISTINCT ?data WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Consists> ?task}{?task ?needs ?data}}";
	private final String getDHMSMBLUQuery = "SELECT DISTINCT ?blu WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Consists> ?task}{?task ?needs ?blu}}";
	private final String getfgDataMapQuery = "SELECT DISTINCT ?activity ?data WHERE {{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?activity <http://semoss.org/ontologies/Relation/Assigned> ?fError}{?fError <http://semoss.org/ontologies/Relation/Needs> ?data}}";
	private final String getfgBLUMapQuery = "SELECT DISTINCT ?activity ?blu WHERE {{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?activity <http://semoss.org/ontologies/Relation/Assigned> ?fError}{?fError <http://semoss.org/ontologies/Relation/Needs> ?blu}}";
	private final String getActivityDataWeightQuery = "SELECT DISTINCT ?activity ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?activity <http://semoss.org/ontologies/Relation/Contains/Dataweight> ?activityWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}}";
	private final String getActivityBLUWeightQuery = "SELECT DISTINCT ?activity ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?activity <http://semoss.org/ontologies/Relation/Contains/BLUweight> ?activityWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}}";
	private final String getActivityDataQuery = "SELECT DISTINCT ?activity ?data ?dataWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?needs <http://semoss.org/ontologies/Relation/Contains/weight> ?dataWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}{?activity ?needs ?data}}";
	private final String getActivityBLUQuery = "SELECT DISTINCT ?activity ?blu ?bluWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?needs <http://semoss.org/ontologies/Relation/Contains/weight> ?bluWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}{?activity ?needs ?blu}}";
	private final String getBusinessProcessQuery = "SELECT DISTINCT ?businessProcess ?activity ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>}{?consists <http://semoss.org/ontologies/Relation/Contains/weight> ?activityWeight}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess ?consists ?activity}}";
	private final String getActivityBPQuery = "SELECT DISTINCT ?businessProcess ?activity ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>}{?consists <http://semoss.org/ontologies/Relation/Contains/weight> ?activityWeight}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess ?consists ?activity}}";
	private final String getFCCQuery = "SELECT DISTINCT ?BusinessProcess ?FCC ?weight WHERE { {?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>;}{?PartOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/PartOf> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?PartOf <http://semoss.org/ontologies/Relation/Contains/PercentBilled> ?weight}{?FCC ?PartOf ?BusinessProcess ;} }";
	private final String getBPCategory = "SELECT DISTINCT ?businessProcess ?ProcessCategory WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Contains/ProcessCategory> ?ProcessCategory}}";
	
	private ArrayList<String> ehrData;
	private ArrayList<String> ehrBLU;
	private ArrayList<String> dhmsmData;
	private ArrayList<String> dhmsmBLU;
	private HashMap<String, ArrayList<String>> fgDataMap;
	private HashMap<String, ArrayList<String>> fgBLUMap;
	private HashMap<String, String> activityDataWeight;
	private HashMap<String, String> activityBLUWeight;
	private HashMap<String, ArrayList<String[]>> activityDataMap;
	private HashMap<String, ArrayList<String[]>> activityBLUMap;
	private HashMap<String, ArrayList<String[]>> bpActivityMap;
	private HashMap<String, ArrayList<String[]>> activityBPMap;
	private HashMap<String, ArrayList<String[]>> bpFCCs;
	private HashMap<String, Double> fccCosts;
	
	HashMap<String, Double> efficiencyPercent = new HashMap<String, Double>();
	
	List<Object[]> list;
	String[] names;
	
	@Override
	public List<Object[]> getTabularData() {
		return this.list;
	}
	
	@Override
	public String[] getColumnHeaders() {
		return this.names;
	}
	
	@Override
	public void createData() {
		ehrData = getStringList(getEHRDataQuery);
		ehrBLU = getStringList(getEHRBLUQuery);
		dhmsmData = getStringList(getDHMSMDataQuery);
		dhmsmBLU = getStringList(getDHMSMBLUQuery);
		fgDataMap = getStringListMap(getfgDataMapQuery);
		fgBLUMap = getStringListMap(getfgBLUMapQuery);
		activityDataWeight = getActivityWeightMap(getActivityDataWeightQuery);
		activityBLUWeight = getActivityWeightMap(getActivityBLUWeightQuery);
		activityDataMap = getActivityMap(getActivityDataQuery);
		activityBLUMap = getActivityMap(getActivityBLUQuery);
		bpActivityMap = getActivityMap(getBusinessProcessQuery);
		bpFCCs = getActivityMap(getFCCQuery);
		fccCosts = fillFCCCostHash();
	}
	
	@Override
	public void runAnalytics() {
		list = new ArrayList<Object[]>();
		if (query != null) {
			if (query.equals("SamData")) {
				names = new String[3];
				names[0] = "Activity";
				names[1] = "Data Object";
				names[2] = "Adjusted Data Weight";
			}
			if (query.equals("SamBLU")) {
				names = new String[3];
				names[0] = "Activity";
				names[1] = "BLU";
				names[2] = "Adjusted BLU Weight";
			}
			if (query.equals("EfficiencyBreakdown")) {
				names = new String[7];
				names[0] = "Business Logic Unit";
				names[1] = "Activity";
				names[2] = "Business Process";
				names[3] = "Dollars Impacted";
				names[4] = "Activity BLU Weight";
				names[5] = "Improvement Factor";
				names[6] = "Dollars Saved";
			}
			if (query.equals("Activity")) {
				names = new String[2];
				names[0] = "Activity";
				names[1] = "delta Efficiency";
			} else if (query.equals("Business Process")) {
				names = new String[4];
				names[0] = "Business Process";
				names[1] = "delta Efficiency";
				names[2] = "BP Cost";
				names[3] = "Efficiency Savings";
			}
		}
		HashMap<String, Double> activityEfficiency = new HashMap<String, Double>();
		HashMap<String, Double> bpCosts = new HashMap<String, Double>();
		HashMap<String, HashMap<String, Double>> samDataMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> samBLUMap = new HashMap<String, HashMap<String, Double>>();
		
		for (String activity : activityDataWeight.keySet()) {
			Double dataEfficiency = 0.0;
			dataEfficiency = getEffiencyScore("data", activityDataMap.get(activity), ehrData, fgDataMap.get(activity), dhmsmData);
			if (query.equals("SamData")) {
				if (activityDataMap.get(activity) != null) {
					samDataMap.put(activity, getSamMap("data", activityDataMap.get(activity), ehrData, fgDataMap.get(activity), dhmsmData));
				}
			}
			dataEfficiency *= Double.parseDouble(activityDataWeight.get(activity));
			activityEfficiency.put(activity, dataEfficiency);
		}
		
		for (String activity : activityBLUWeight.keySet()) {
			Double bluEfficiency = 0.0;
			bluEfficiency = getEffiencyScore("blu", activityBLUMap.get(activity), ehrBLU, fgBLUMap.get(activity), dhmsmBLU);
			if (activityBLUMap.get(activity) != null) {
				if (query.equals("SamBLU")) {
					if (activityBLUMap.get(activity) != null) {
						samBLUMap.put(activity, getSamMap("blu", activityBLUMap.get(activity), ehrBLU, fgBLUMap.get(activity), dhmsmBLU));
					}
				}
			}
			bluEfficiency *= Double.parseDouble(activityBLUWeight.get(activity));
			if (activityEfficiency.containsKey(activity)) {
				Double dataEfficiency = activityEfficiency.get(activity);
				activityEfficiency.put(activity, dataEfficiency + bluEfficiency);
			} else {
				activityEfficiency.put(activity, bluEfficiency);
			}
		}
		if (query.equals("SamData")) {
			for (String activity : samDataMap.keySet()) {
				for (String data : samDataMap.get(activity).keySet()) {
					Object[] temp = { activity, data, samDataMap.get(activity).get(data) };
					list.add(temp);
				}
			}
		}
		
		if (query.equals("SamBLU")) {
			for (String activity : samBLUMap.keySet()) {
				for (String blu : samBLUMap.get(activity).keySet()) {
					Object[] temp = { activity, blu, samBLUMap.get(activity).get(blu) };
					list.add(temp);
				}
			}
		}
		if (query.equals("EfficiencyBreakdown")) {
			for (String bp : bpFCCs.keySet()) {
				Double bpCost = 0.0;
				for (String[] FCC : bpFCCs.get(bp)) {
					if (fccCosts.containsKey(FCC[0])) {
						bpCost += (Double.parseDouble(FCC[1]) * fccCosts.get(FCC[0]));
					}
				}
				bpCosts.put(bp, bpCost);
			}
			for (String bp : bpActivityMap.keySet()) {
				for (String[] activity : bpActivityMap.get(bp)) {
					if (activityBLUMap.get(activity[0]) != null) {
						for (String[] blu : activityBLUMap.get(activity[0])) {
							Double dollarsImpacted = bpCosts.get(bp) * Double.parseDouble(activity[1]);
							Double bluActivityWeight = Double.parseDouble(blu[1]);
							Double improvementFactor = activityEfficiency.get(activity[0]);
							Double savings = bluActivityWeight * improvementFactor * dollarsImpacted;
							Object[] temp = { blu[0], activity[0], bp, dollarsImpacted, bluActivityWeight, improvementFactor, savings };
							list.add(temp);
						}
					}
				}
			}
		}
		if (query.equals("Activity")) {
			for (String activity : activityEfficiency.keySet()) {
				Object[] temp = { activity, activityEfficiency.get(activity) };
				list.add(temp);
			}
		}
		
		else if (query.contains("Schedule") || query.equals("Business Process")) {
			for (String bp : bpFCCs.keySet()) {
				Double bpCost = 0.0;
				for (String[] bpFCC : bpFCCs.get(bp)) {
					if (fccCosts.containsKey(bpFCC[0])) {
						bpCost += Double.parseDouble(bpFCC[1]) * fccCosts.get(bpFCC[0]);
					}
				}
				bpCosts.put(bp, bpCost);
			}
			
			for (String businessProcess : bpActivityMap.keySet()) {
				Double bpEfficiency = 0.0;
				
				for (String[] bpActivity : bpActivityMap.get(businessProcess)) {
					if (activityEfficiency.keySet().contains(bpActivity[0])) {
						bpEfficiency += (Double.parseDouble(bpActivity[1]) * activityEfficiency.get(bpActivity[0]));
					}
				}
				Double bpCost = 0.0;
				if (bpCosts.containsKey(businessProcess))
					bpCost = bpCosts.get(businessProcess);
				Object[] temp = { businessProcess, bpEfficiency, bpCost, bpEfficiency * bpCost };
				efficiencyPercent.put(businessProcess, bpEfficiency);
				list.add(temp);
			}
		}
	}
	
	public HashMap<String, Double> getSamMap(String type, ArrayList<String[]> objectList, ArrayList<String> ehrList, ArrayList<String> fgList,
			ArrayList<String> dhmsmList) {
		HashMap<String, Double> finalMap = new HashMap<String, Double>();
		double adjustedWeight = 0.0;
		if (objectList != null) {
			int size = objectList.size();
			for (int i = 0; i < objectList.size(); i++) {
				String[] data = objectList.get(i);
				boolean dataRemoved = false;
				if (ehrList.contains(data[0])) {
					objectList.remove(i);
					i--;
					dataRemoved = true;
				}
				if ((!dataRemoved) && fgList != null) {
					if (fgList.contains(data[0])) {
						objectList.remove(i);
						i--;
						dataRemoved = true;
					}
				}
				if (!dataRemoved) {
					if (!dhmsmList.contains(data[0])) {
						objectList.remove(i);
						i--;
						dataRemoved = true;
					}
				}
				if (!dataRemoved) {
					if (size < 4 && type.equals("blu")) {
						adjustedWeight = (Double.parseDouble(data[1]) * .25);
					} else if (size < 3 && type.equals("data")) {
						adjustedWeight = (Double.parseDouble(data[1]) / 3);
						// }
					} else {
						adjustedWeight = Double.parseDouble(data[1]);
					}
				}
				finalMap.put(data[0], adjustedWeight);
			}
		}
		return finalMap;
	}
	
	public ArrayList<String> getStringList(String query) {
		ArrayList<String> finalList = new ArrayList<String>();
		ISelectWrapper sjsw = Utility.processQuery(hrCore, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			finalList.add(sjss.getVar(values[0]).toString());
		}
		return finalList;
	}
	
	public HashMap<String, ArrayList<String>> getStringListMap(String query) {
		HashMap<String, ArrayList<String>> finalMap = new HashMap<String, ArrayList<String>>();
		ISelectWrapper sjsw = Utility.processQuery(hrCore, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			ArrayList<String> temp;
			
			String key = sjss.getVar(values[0]).toString();
			if (!finalMap.containsKey(key)) {
				temp = new ArrayList<String>();
				finalMap.put(key, temp);
			}
			finalMap.get(key).add(sjss.getVar(values[1]).toString());
		}
		return finalMap;
	}
	
	public HashMap<String, String> getActivityWeightMap(String query) {
		HashMap<String, String> finalMap = new HashMap<String, String>();
		ISelectWrapper sjsw = Utility.processQuery(hrCore, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String key = sjss.getVar(values[0]).toString();
			String value = sjss.getVar(values[1]).toString();
			finalMap.put(key, value);
		}
		return finalMap;
	}
	
	public HashMap<String, ArrayList<String[]>> getActivityMap(String query) {
		HashMap<String, ArrayList<String[]>> finalMap = new HashMap<String, ArrayList<String[]>>();
		ISelectWrapper sjsw = Utility.processQuery(hrCore, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			
			String key = sjss.getVar(values[0]).toString();
			if (!finalMap.containsKey(key)) {
				finalMap.put(key, new ArrayList<String[]>());
			}
			String[] temp = { sjss.getVar(values[1]).toString(), sjss.getVar(values[2]).toString() };
			finalMap.get(key).add(temp);
		}
		return finalMap;
	}
	
	public double getEffiencyScore(String type, ArrayList<String[]> objectList, ArrayList<String> ehrList, ArrayList<String> fgList,
			ArrayList<String> dhmsmList) {
		double unweightedEfficiency = 0.0;
		if (objectList != null) {
			int size = objectList.size();
			for (int i = 0; i < objectList.size(); i++) {
				String[] data = objectList.get(i);
				boolean dataRemoved = false;
				if (ehrList.contains(data[0])) {
					objectList.remove(i);
					i--;
					dataRemoved = true;
				}
				if ((!dataRemoved) && fgList != null) {
					if (fgList.contains(data[0])) {
						objectList.remove(i);
						i--;
						dataRemoved = true;
					}
				}
				if (!dataRemoved) {
					if (!dhmsmList.contains(data[0])) {
						objectList.remove(i);
						i--;
						dataRemoved = true;
					}
				}
				if (!dataRemoved) {
					if (size < 4 && type.equals("blu")) {
						unweightedEfficiency += (Double.parseDouble(data[1]) * .25);
					} else if (size < 3 && type.equals("data")) {
						unweightedEfficiency += (Double.parseDouble(data[1]) / 3);
						// }
					} else {
						unweightedEfficiency += Double.parseDouble(data[1]);
					}
				}
			}
		}
		return unweightedEfficiency;
	}
	
	/**
	 * This should provide you with the cost for each FCC.
	 **/
	
	private HashMap<String, Double> fillFCCCostHash() {
		HashMap<String, Double> fccCost = new HashMap<String, Double>();
		String siteEngineName = "TAP_Site_Data";
		String fccQuery = "SELECT DISTINCT ?FCC (SUM(?TotalCost) AS ?Cost) WHERE { SELECT DISTINCT ?MTF ?FCC ?TotalCost WHERE{ {?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>}{?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?FCCMTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost }{?FCC <http://semoss.org/ontologies/Relation/Has> ?FCCMTF}{?FCCMTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}}} GROUP BY ?FCC";
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp(siteEngineName);
		
		ISelectWrapper siteWrapper = WrapperManager.getInstance().getSWrapper(siteEngine, fccQuery);
		
		/*
		 * ISelectWrapper siteWrapper = new ISelectWrapper(); if (siteEngine == null) {
		 * Utility.showError("The database \"TAP_Site_Data\" could not be found. Process unable to continue"); } siteWrapper.setQuery(fccQuery);
		 * siteWrapper.setEngine(siteEngine); siteWrapper.executeQuery();
		 */
		
		// get the bindings from it
		String[] fccColNames = siteWrapper.getVariables();
		try {
			while (siteWrapper.hasNext()) {
				ISelectStatement sjss = siteWrapper.next();
				String fcc = (String) sjss.getVar(fccColNames[0]);
				Object cost = sjss.getVar(fccColNames[1]);
				double doubleCost = 0.0;
				if (cost instanceof Double)
					doubleCost = (Double) cost;
				else if (cost instanceof Integer) {
					doubleCost = (Integer) cost * 1.0;
				}
				fccCost.put(fcc, doubleCost);
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		return fccCost;
	}
}
