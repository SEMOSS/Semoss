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

import prerna.engine.api.IEngine;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;

/**
 * Used in EA perspective of HR_Core to display Effectiveness calculations for Economic Analysis
 * 
 * @author kepark
 * 
 */
public class EAEffectivenessPlaySheet extends GridPlaySheet {
	private IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
	private EAFunctionalGapHelper helper = new EAFunctionalGapHelper();
	
	// Commented out queries are used for Chrissy's effectiveness calculation
	// "SELECT DISTINCT ?DataObject WHERE{FILTER(?Review_Status = 'FAC_Approved')FILTER(?RIBEE = 'Low')FILTER(?Device_InterfaceYN = 'N'){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Designated <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Designated>;}{?DataObjectSource <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObjectSource>;}{?System ?Designated ?DataObjectSource.}{?LabeledAs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/LabeledAs>;}BIND(<http://health.mil/ontologies/Concept/SourceType/CurrentCreator> AS ?SourceType){?DataObjectSource ?LabeledAs ?SourceType.}{?Delivers <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Delivers>;}{?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?DataObjectSource ?Delivers ?DataObject.}{?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status;} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?RIBEE;} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?Device_InterfaceYN;} {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> ?Interface_Needed_w_DHMSM;} }";
	private final String getDHMSMDataQuery = "SELECT DISTINCT ?data WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Consists> ?task}{?task ?needs ?data}}";
	// "SELECT DISTINCT ?BusinessLogicUnit WHERE{FILTER(?Review_Status = 'FAC_Approved')FILTER(?RIBEE = 'Low')FILTER(?Device_InterfaceYN = 'N'){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System ?Provide ?BusinessLogicUnit.}{?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status;} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?RIBEE;} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?Device_InterfaceYN;} {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> ?Interface_Needed_w_DHMSM;} }";
	private final String getDHMSMBLUQuery = "SELECT DISTINCT ?blu WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Consists> ?task}{?task ?needs ?blu}}";
	private final String getActivityFGQuery = "SELECT DISTINCT ?activity ?fError WHERE {{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?activity <http://semoss.org/ontologies/Relation/Assigned> ?fError}}";
	private final String getFGDataQuery = "SELECT DISTINCT ?fError ?data ?dataWeight WHERE {{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?activity <http://semoss.org/ontologies/Relation/Assigned> ?fError}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?needs <http://semoss.org/ontologies/Relation/Contains/weight> ?dataWeight}{?fError ?needs ?data}}";
	private final String getFGBLUQuery = "SELECT DISTINCT ?fError ?blu ?bluWeight WHERE {{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?needs <http://semoss.org/ontologies/Relation/Contains/weight> ?bluWeight}{?activity <http://semoss.org/ontologies/Relation/Assigned> ?fError}{?fError ?needs ?blu}}";
	private final String getFGPropsQuery = "SELECT DISTINCT ?fError ?frequency ?dataWeight ?bluWeight WHERE {{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?fError <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency}{?fError <http://semoss.org/ontologies/Relation/Contains/Dataweight> ?dataWeight}{?fError <http://semoss.org/ontologies/Relation/Contains/BLUweight> ?bluWeight}}";
	private final String getActivityDataWeightQuery = "SELECT DISTINCT ?activity ?activityWeight WHERE {{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?activity <http://semoss.org/ontologies/Relation/Contains/Dataweight> ?activityWeight}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}}";
	private final String getActivityBLUWeightQuery = "SELECT DISTINCT ?activity ?activityWeight WHERE {{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?activity <http://semoss.org/ontologies/Relation/Contains/BLUweight> ?activityWeight}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}}";
	private final String getActivityDataQuery = "SELECT DISTINCT ?activity ?data ?dataWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?needs <http://semoss.org/ontologies/Relation/Contains/weight> ?dataWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}{?activity ?needs ?data}}";
	private final String getActivityBLUQuery = "SELECT DISTINCT ?activity ?blu ?bluWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?needs <http://semoss.org/ontologies/Relation/Contains/weight> ?bluWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}{?activity ?needs ?blu}}";
	private final String getBusinessProcessQuery = "SELECT DISTINCT ?businessProcess ?activity ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>}{?consists <http://semoss.org/ontologies/Relation/Contains/weight> ?activityWeight}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess ?consists ?activity}}";
	private final String getFCCQuery = "SELECT DISTINCT ?BusinessProcess ?FCC ?weight WHERE { {?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>;}{?PartOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/PartOf> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?PartOf <http://semoss.org/ontologies/Relation/Contains/PercentBilled> ?weight}{?FCC ?PartOf ?BusinessProcess ;} }";
	private final String getTotalActivityCountQuery = "SELECT DISTINCT (COUNT(?activity) AS ?count) WHERE {{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}}";
	private final String getFGActivityCountQuery = "SELECT DISTINCT ?fError (COUNT(?activity) AS ?count) WHERE {{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}{?activity <http://semoss.org/ontologies/Relation/Assigned> ?fError}} GROUP BY ?fError";
	private final String getActivityBPQuery = "SELECT DISTINCT ?activity ?businessProcess ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>}{?consists <http://semoss.org/ontologies/Relation/Contains/weight> ?activityWeight}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess ?consists ?activity}}";
	
	private ArrayList<String> dhmsmData;
	private ArrayList<String> dhmsmBLU;
	private HashMap<String, ArrayList<String>> activityFGMap;
	private HashMap<String, ArrayList<String[]>> fgDataMap;
	private HashMap<String, ArrayList<String[]>> fgBLUMap;
	private HashMap<String, String[]> fgProps; // index 0 = frequency, 1 = dataWeight, 2 = bluWeight
	private HashMap<String, String> activityDataWeight;
	private HashMap<String, String> activityBLUWeight;
	private HashMap<String, ArrayList<String[]>> activityDataMap;
	private HashMap<String, ArrayList<String[]>> activityBLUMap;
	private HashMap<String, ArrayList<String[]>> bpActivityMap;
	private HashMap<String, ArrayList<String[]>> bpFCCs;
	private HashMap<String, Double> fccCosts;
	private Double totalActivityCount;
	private HashMap<String, String> fgActivityCount;
	
	private HashMap<String, ArrayList<String[]>> activityBP;
	private HashMap<String, HashMap<String, Double>> yearFCCCost;
	HashMap<String, Double> effectPercent = new HashMap<String, Double>();
	
	/**
	 * Gathers data from all queries to be used in Effectiveness calculation
	 */
	@Override
	public void createData() {
		dhmsmData = helper.getStringList(getDHMSMDataQuery, hrCore);
		dhmsmBLU = helper.getStringList(getDHMSMBLUQuery, hrCore);
		activityFGMap = helper.getStringListMap(getActivityFGQuery, hrCore);
		fgDataMap = helper.getStringListArrayMap(getFGDataQuery, hrCore);
		fgBLUMap = helper.getStringListArrayMap(getFGBLUQuery, hrCore);
		activityDataWeight = helper.getStringMap(getActivityDataWeightQuery, hrCore);
		activityBLUWeight = helper.getStringMap(getActivityBLUWeightQuery, hrCore);
		activityDataMap = helper.getStringListArrayMap(getActivityDataQuery, hrCore);
		activityBLUMap = helper.getStringListArrayMap(getActivityBLUQuery, hrCore);
		fgProps = helper.getFGPropArray(getFGPropsQuery, hrCore);
		fgActivityCount = helper.getStringMap(getFGActivityCountQuery, hrCore);
		totalActivityCount = helper.getTotalCount(getTotalActivityCountQuery, hrCore);
		bpActivityMap = helper.getStringListArrayMap(getBusinessProcessQuery, hrCore);
		bpFCCs = helper.getStringListArrayMap(getFCCQuery, hrCore);
		fccCosts = helper.fillFCCCostHash();
		yearFCCCost = helper.getCostPerYear();
		activityBP = helper.getStringListArrayMap(getActivityBPQuery, hrCore);
	}
	
	/**
	 * Performs Effectiveness calculations and displays different information: 1. Activity and functional gap combination with all parameters used in
	 * calculation, 2. Activity and G value, 3. Business Process, G value, total cost of business process, and savings realized by effectiveness
	 */
	@Override
	public void runAnalytics() {
		HashMap<String[], String[]> activityFGDelta = new HashMap<String[], String[]>();
		HashMap<String[], String[]> bpFGActivityDelta = new HashMap<String[], String[]>();
		HashMap<String, Double> activityDelta = new HashMap<String, Double>();
		HashMap<String, Double> bpCosts = new HashMap<String, Double>();
		HashMap<String, HashMap<String, Double>> yearBPCosts = new HashMap<String, HashMap<String, Double>>();
		HashMap<String[], Double[]> activityFGFDID = new HashMap<String[], Double[]>();
		list = new ArrayList<Object[]>();
		
		// for column headers
		if (query != null) {
			if (query.contains("Functional Gap")) {
				names = new String[19];
				names[0] = "Functional Gap";
				names[1] = "Activity";
				names[2] = "Business Process";
				names[3] = "Dollars Impacted";
				names[4] = "Failure Rate";
				names[5] = "Dollars Lost";
				names[6] = "Improvement Factor";
				names[7] = "Dollars Saved";
				names[8] = "G";
				names[9] = "Freq";
				names[10] = "J";
				names[11] = "FG DataW";
				names[12] = "FG BLUW";
				names[13] = "Activity DataW";
				names[14] = "Activity BLUW";
				names[15] = "FG Data Sum";
				names[16] = "FG BLU Sum";
				names[17] = "Activity Data Sum";
				names[18] = "Activity BLU Sum";
			} else if (query.contains("Activity")) {
				names = new String[2];
				names[0] = "Activity";
				names[1] = "G";
			} else if (query.contains("Business Process")) {
				names = new String[4];
				names[0] = "Business Process";
				names[1] = "G";
				names[2] = "BP Cost";
				names[3] = "Effectiveness Savings";
			} else if (query.contains("Per Year")) {
				names = new String[4];
				names[0] = "FG";
				names[1] = "Dollars Touched by FG";
				names[2] = "Dollars Lost";
				names[3] = "Dollars Saved";
			}
		}
		for (String bp : bpFCCs.keySet()) {
			Double bpCost = 0.0;
			for (String[] FCC : bpFCCs.get(bp)) {
				if (fccCosts.containsKey(FCC[0])) {
					bpCost += (Double.parseDouble(FCC[1]) * fccCosts.get(FCC[0]));
				}
			}
			bpCosts.put(bp, bpCost);
		}
		// Iterate through every activity that has a functional gap
		for (String activity : activityFGMap.keySet()) {
			for (String fg : activityFGMap.get(activity)) {
				Double fd = 0.0;
				Double id = 0.0;
				Double g = 0.0;
				fd = helper.getFD(fgProps.get(fg), totalActivityCount, fgActivityCount.get(fg), fgDataMap.get(fg), fgBLUMap.get(fg), activityDataMap
						.get(activity), activityBLUMap.get(activity));
				id = helper.getID(activityDataWeight.get(activity), activityBLUWeight.get(activity), dhmsmData, dhmsmBLU, fgDataMap.get(fg), fgBLUMap
						.get(fg), activityDataMap.get(activity), activityBLUMap.get(activity));
				g = fd * id;
				String[] tempKey = { activity, fg };
				Double[] tempVal = { fd, id };
				activityFGFDID.put(tempKey, tempVal);
				// try {
				// try {
				String[] tempValue = { g.toString(), fgProps.get(fg)[0], fgActivityCount.get(fg), fgProps.get(fg)[1], fgProps.get(fg)[2],
						activityDataWeight.get(activity), activityBLUWeight.get(activity), helper.fgDataSum.toString(), helper.fgBLUSum.toString(),
						helper.activityDataSum.toString(), helper.activityBLUSum.toString() };
				activityFGDelta.put(tempKey, tempValue);
				if (activityBP.get(activity) != null) {
					for (String[] bpAndWeight : activityBP.get(activity)) {
						String[] tempFGActBPKey = { fg, activity, bpAndWeight[0] };
						Double activityCost = bpCosts.get(bpAndWeight[0]) * Double.parseDouble(bpAndWeight[1]);
						Double dollarsLost = activityCost * fd;
						Double dollarsSaved = dollarsLost * id;
						String[] tempBPValue = { activityCost.toString(), fd.toString(), dollarsLost.toString(), id.toString(),
								dollarsSaved.toString(), g.toString(), fgProps.get(fg)[0], fgActivityCount.get(fg), fgProps.get(fg)[1],
								fgProps.get(fg)[2], activityDataWeight.get(activity), activityBLUWeight.get(activity), helper.fgDataSum.toString(),
								helper.fgBLUSum.toString(), helper.activityDataSum.toString(), helper.activityBLUSum.toString() };
						bpFGActivityDelta.put(tempFGActBPKey, tempBPValue);
					}
				}
				// } catch (NullPointerException e) {
				// e.printStackTrace();
				// Utility.showError("FError properties are missing. Please make sure all FErrors have Frequency, Dataweight, and BLUweight.");
				// throw new Exception();
				// }
			}
		}
		for (String[] activityFG : activityFGDelta.keySet()) {
			String[] temp = activityFGDelta.get(activityFG);
			if (!activityDelta.containsKey(activityFG[0])) {
				activityDelta.put(activityFG[0], 0.0);
			}
			Double tempG = activityDelta.get(activityFG[0]);
			tempG += Double.parseDouble(temp[0]);
			activityDelta.put(activityFG[0], tempG);
		}
		
		if (query.contains("Functional Gap")) {
			for (String[] activityFG : bpFGActivityDelta.keySet()) {
				String[] temp = bpFGActivityDelta.get(activityFG);
				Object[] finalRow = { activityFG[0], activityFG[1], activityFG[2], temp[0], temp[1], temp[2], temp[3], temp[4], temp[5], temp[6],
						temp[7], temp[8], temp[9], temp[10], temp[11], temp[12], temp[13], temp[14], temp[15] };
				list.add(finalRow);
			}
			// Roll up to activity level
		} else if (query.contains("Activity")) {
			for (String activity : activityDelta.keySet()) {
				Object[] temp = { activity, activityDelta.get(activity) };
				list.add(temp);
			}
			
			// Roll up to business process level
		} else if (query.contains("Schedule") || query.contains("Business Process")) {
			for (String businessProcess : bpActivityMap.keySet()) {
				Double bpG = 0.0;
				for (String activity[] : bpActivityMap.get(businessProcess)) {
					if (activityDelta.keySet().contains(activity[0])) {
						bpG += (Double.parseDouble(activity[1]) * activityDelta.get(activity[0]));
					}
				}
				Double bpCost = 0.0;
				if (bpCosts.containsKey(businessProcess))
					bpCost = bpCosts.get(businessProcess);
				Object[] temp = { businessProcess, bpG, bpCost, bpG * bpCost };
				effectPercent.put(businessProcess, bpG);
				list.add(temp);
			}
		} else if (query.contains("Per Year")) {
			HashMap<String, Double[]> fgCost = new HashMap<String, Double[]>();
			for (String[] activityFG : activityFGDelta.keySet()) {
				if (!fgCost.containsKey(activityFG[1])) {
					Double[] temp = { 0.0, 0.0, 0.0 };
					fgCost.put(activityFG[1], temp);
				}
				Double[] fgCalc = activityFGFDID.get(activityFG);
				Double totalCost = fgCost.get(activityFG[1])[0];
				Double fdCost = fgCost.get(activityFG[1])[1];
				Double fdIDCost = fgCost.get(activityFG[1])[2];
				if (activityBP.get(activityFG[0]) != null) {
					for (String bp[] : activityBP.get(activityFG[0])) {
						if (bp != null & bpCosts.get(bp[0]) != null) {
							totalCost += Double.parseDouble(bp[1]) * bpCosts.get(bp[0]);
							if (fgCalc != null) {
								fdCost += Double.parseDouble(bp[1]) * bpCosts.get(bp[0]) * fgCalc[0];
								fdIDCost += Double.parseDouble(bp[1]) * bpCosts.get(bp[0]) * fgCalc[0] * fgCalc[1];
							}
						}
					}
				}
				Double[] finalCalc = { totalCost, fdCost, fdIDCost };
				fgCost.put(activityFG[1], finalCalc);
			}
			for (String fg : fgCost.keySet()) {
				Object[] temp = { fg, fgCost.get(fg)[0], fgCost.get(fg)[1], fgCost.get(fg)[2] };
				list.add(temp);
			}
		}
	}
}