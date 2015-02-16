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

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class EAFunctionalGapHelper {
	Double fgDataSum = 0.0;
	Double fgBLUSum = 0.0;
	Double activityDataSum = 0.0;
	Double activityBLUSum = 0.0;
	
	ArrayList<String> getStringList(String query, IEngine engine) {
		ArrayList<String> finalList = new ArrayList<String>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			finalList.add(sjss.getVar(values[0]).toString());
		}
		return finalList;
	}
	
	HashMap<String, ArrayList<String>> getStringListMap(String query, IEngine engine) {
		HashMap<String, ArrayList<String>> finalMap = new HashMap<String, ArrayList<String>>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
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
	
	HashMap<String, String> getStringMap(String query, IEngine engine) {
		HashMap<String, String> finalMap = new HashMap<String, String>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String key = sjss.getVar(values[0]).toString();
			String value = sjss.getVar(values[1]).toString();
			finalMap.put(key, value);
		}
		return finalMap;
	}
	
	HashMap<String, String> getWaveMap(String query, IEngine engine) {
		HashMap<String, String> finalMap = new HashMap<String, String>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String mtf = sjss.getVar(values[0]).toString();
			String year = sjss.getVar(values[1]).toString();
			if (!finalMap.containsKey(mtf)) {
				finalMap.put(mtf, year);
			} else if (Integer.parseInt(finalMap.get(mtf)) < Integer.parseInt(year)) {
				finalMap.put(mtf, year);
			}
		}
		return finalMap;
	}
	
	HashMap<String, ArrayList<String[]>> getStringListArrayMap(String query, IEngine engine) {
		HashMap<String, ArrayList<String[]>> finalMap = new HashMap<String, ArrayList<String[]>>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
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
	
	double getEffiencyScore(ArrayList<String[]> objectList, ArrayList<String> ehrList, ArrayList<String> fgList, ArrayList<String> dhmsmList) {
		double unweightedEfficiency = 0.0;
		if (objectList != null) {
			for (int i = 0; i < objectList.size(); i++) {
				String[] data = objectList.get(i);
				boolean dataRemoved = false;
				if (ehrList.contains(data[0])) {
					objectList.remove(i);
					i--;
					dataRemoved = true;
				}
				if ((!dataRemoved) && fgList != null && fgList.contains(data[0])) {
					objectList.remove(i);
					i--;
					dataRemoved = true;
				}
				if (!dataRemoved && !dhmsmList.contains(data[0])) {
					objectList.remove(i);
					i--;
					dataRemoved = true;
				}
				if (!dataRemoved) {
					unweightedEfficiency += Double.parseDouble(data[1]);
				}
			}
		}
		return unweightedEfficiency;
	}
	
	// Effectiveness methods
	HashMap<String, String[]> getFGPropArray(String query, IEngine engine) {
		HashMap<String, String[]> fgMap = new HashMap<String, String[]>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String key = sjss.getVar(values[0]).toString();
			String[] temp = { sjss.getVar(values[1]).toString(), sjss.getVar(values[2]).toString(), sjss.getVar(values[3]).toString() };
			fgMap.put(key, temp);
		}
		return fgMap;
	}
	
	HashMap<String, HashMap<String, Double>> getDoubleMap(String query, IEngine engine) {
		HashMap<String, HashMap<String, Double>> finalMap = new HashMap<String, HashMap<String, Double>>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			
			String oneKey = sjss.getVar(values[0]).toString();
			String twoKey = sjss.getVar(values[1]).toString();
			if (!finalMap.containsKey(oneKey)) {
				finalMap.put(oneKey, new HashMap<String, Double>());
			}
			finalMap.get(oneKey).put(twoKey, Double.parseDouble(sjss.getVar(values[2]).toString()));
		}
		return finalMap;
	}
	
	Double getTotalCount(String query, IEngine engine) {
		Double total = 0.0;
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] values = sjsw.getVariables();
		ISelectStatement sjss = sjsw.next();
		total = Double.parseDouble(sjss.getVar(values[0]).toString());
		return total;
	}
	
	Double getFD(String[] fgProps, Double totalActivityCount, String fgActivityCount, ArrayList<String[]> fgData, ArrayList<String[]> fgBLU,
			ArrayList<String[]> activityData, ArrayList<String[]> activityBLU) {
		Double fd = 0.0;
		Double dataFD = 0.0;
		Double bluFD = 0.0;
		
		String[] empty = { "0.0", "0.0" };
		ArrayList<String[]> emptyList = new ArrayList<String[]>();
		emptyList.add(empty);
		if (fgProps == null) {
			fgProps = new String[3];
			fgProps[0] = "0.0";
			fgProps[1] = "0.0";
			fgProps[2] = "0.0";
		}
		if (fgActivityCount == null) {
			fgActivityCount = "0.0";
		}
		if (fgData == null) {
			fgData = emptyList;
		}
		if (fgBLU == null) {
			fgBLU = emptyList;
		}
		if (activityData == null) {
			activityData = emptyList;
		}
		if (activityBLU == null) {
			activityBLU = emptyList;
		}
		
		Double fgCount = Double.parseDouble(fgActivityCount);
		Double frequency = Double.parseDouble(fgProps[0]);
		Double fgDataWeight = Double.parseDouble(fgProps[1]);
		Double fgBLUWeight = Double.parseDouble(fgProps[2]);
		for (String[] fg : fgData) {
			for (String[] activity : activityData) {
				if (fg[0].equals(activity[0])) {
					dataFD += Double.parseDouble(fg[1]);
					break;
				}
			}
		}
		for (String[] fg : fgBLU) {
			for (String[] activity : activityBLU) {
				if (fg[0].equals(activity[0])) {
					bluFD += Double.parseDouble(fg[1]);
					break;
				}
			}
		}
		
		fgDataSum = dataFD;
		fgBLUSum = bluFD;
		Double fdMultiplierPercentage = frequency * (totalActivityCount / fgCount);
		if (fdMultiplierPercentage > 1.0) {
			fd = ((fgDataWeight * dataFD) + (fgBLUWeight * bluFD));
		} else {
			fd = fdMultiplierPercentage * ((fgDataWeight * dataFD) + (fgBLUWeight * bluFD));
		}
		return fd;
	}
	
	Double getID(String activityDataWeight, String activityBLUWeight, ArrayList<String> dhmsmData, ArrayList<String> dhmsmBLU,
			ArrayList<String[]> fgData, ArrayList<String[]> fgBLU, ArrayList<String[]> activityData, ArrayList<String[]> activityBLU) {
		Double id = 0.0;
		Double dataID = 0.0;
		Double bluID = 0.0;
		
		String[] empty = { "0.0", "0.0" };
		ArrayList<String[]> emptyList = new ArrayList<String[]>();
		emptyList.add(empty);
		if (activityDataWeight == null) {
			activityDataWeight = "0.0";
		}
		if (activityBLUWeight == null) {
			activityBLUWeight = "0.0";
		}
		if (fgData == null) {
			fgData = emptyList;
		}
		if (fgBLU == null) {
			fgBLU = emptyList;
		}
		if (activityData == null) {
			activityData = emptyList;
		}
		if (activityBLU == null) {
			activityBLU = emptyList;
		}
		
		Double dataWeight = Double.parseDouble(activityDataWeight);
		Double bluWeight = Double.parseDouble(activityBLUWeight);
		for (String[] activity : activityData) {
			for (String[] fg : fgData) {
				if (activity[0].equals(fg[0]) && dhmsmData.contains(activity[0])) {
					dataID += Double.parseDouble(activity[1]);
					break;
				}
			}
		}
		for (String[] activity : activityBLU) {
			for (String[] fg : fgBLU) {
				if (activity[0].equals(fg[0]) && dhmsmBLU.contains(activity[0])) {
					bluID += Double.parseDouble(activity[1]);
					break;
				}
			}
		}
		
		activityDataSum = dataID;
		activityBLUSum = bluID;
		id = (dataWeight * dataID) + (bluWeight * bluID);
		return id;
	}
	
	/**
	 * This should provide you with the cost for each FCC.
	 **/
	HashMap<String, Double> fillFCCCostHash() {
		HashMap<String, Double> fccCost = new HashMap<String, Double>();
		String siteEngineName = "TAP_Site_Data";
		String fccQuery = "SELECT DISTINCT ?FCC (SUM(?TotalCost) AS ?Cost) WHERE {{?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>} {?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?FCCMTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost }{?FCC <http://semoss.org/ontologies/Relation/Has> ?FCCMTF}{?FCCMTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}} GROUP BY ?FCC";
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp(siteEngineName);
		

		
		/*ISelectWrapper siteWrapper = new ISelectWrapper();
		if (siteEngine == null) {
			Utility.showError("The database \"TAP_Site_Data\" could not be found. Process unable to continue");
		}
		siteWrapper.setQuery(fccQuery);
		siteWrapper.setEngine(siteEngine);
		siteWrapper.executeQuery();
		*/
		
		ISelectWrapper siteWrapper = WrapperManager.getInstance().getSWrapper(siteEngine, fccQuery);

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
	
	HashMap<String, HashMap<String, Double>> getCostPerYear() {
		HashMap<String, HashMap<String, Double>> fccCost = new HashMap<String, HashMap<String, Double>>();
		String siteEngineName = "TAP_Site_Data";
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp(siteEngineName);
		String fccmtfYearQuery = "SELECT DISTINCT ?FCCMTF ?Year WHERE {{?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?FCCMTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}} ORDER BY ASC(?Year)";
		String fccmtfCostQuery = "SELECT DISTINCT ?FCCMTF ?TotalCost WHERE {{?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?FCCMTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost }{?FCCMTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}}";
		HashMap<String, String> fccmtfYear = getStringMap(fccmtfYearQuery, siteEngine);
		HashMap<String, String> fccmtfCost = getStringMap(fccmtfCostQuery, siteEngine);
		
		for (String fccmtf : fccmtfYear.keySet()) {
			String fcc = fccmtf.substring(0, fccmtf.indexOf("-"));
			String year = fccmtfYear.get(fccmtf);
			Double singleCost;
			if (fccmtfCost.get(fccmtf) != null) {
				singleCost = Double.parseDouble(fccmtfCost.get(fccmtf));
			} else {
				singleCost = 0.0;
			}
			if (!fccCost.containsKey(year)) {
				fccCost.put(year, new HashMap<String, Double>());
			}
			if (!fccCost.get(year).containsKey(fcc)) {
				fccCost.get(year).put(fcc, 0.0);
			}
			Double totalCost = fccCost.get(year).get(fcc);
			totalCost += singleCost;
			fccCost.get(year).put(fcc, totalCost);
		}
		
		return fccCost;
	}
}
