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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.ui.components.playsheets.OCONUSMapPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class OCONUSSavingsMapPlaySheet extends OCONUSMapPlaySheet {
	private IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
	private IEngine tapSite = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
	
	// Queries used for savings
	private final String getBPProdQuery = "SELECT DISTINCT ?businessProcess ?Productivity WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?businessProcess <http://semoss.org/ontologies/Relation/Contains/EA-Productivity> ?Productivity}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}}";
	private final String getFCCBPQuery = "SELECT DISTINCT ?FCC ?BusinessProcess ?weight WHERE { {?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>;}{?PartOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/PartOf> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?PartOf <http://semoss.org/ontologies/Relation/Contains/PercentBilled> ?weight}{?FCC ?PartOf ?BusinessProcess ;} }";
	private final String getMTFFCCQuery = "SELECT DISTINCT ?MTF ?FCC ?TotalCost WHERE {{?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>}{?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?FCCMTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost }{?FCC <http://semoss.org/ontologies/Relation/Has> ?FCCMTF}{?FCCMTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}}";
	private final String getDCSiteMTFQuery = "SELECT DISTINCT ?DCSite ?MTF WHERE {{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}}";
	private final String getInitialQuery = "SELECT ?DCSite ?lat ?lon ?size WHERE { {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite> ;} {?DCSite  <http://semoss.org/ontologies/Relation/Contains/LONG> ?lon}{?DCSite  <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat} }";
	
	// Queries used for total functional gap loss
	private final String getActivityFGWeightQuery = "SELECT DISTINCT ?activity ?fg ?weight WHERE {{?fg a <http://semoss.org/ontologies/Concept/FError>}{?activity a <http://semoss.org/ontologies/Concept/Activity>}{?assigned <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned>}{?assigned <http://semoss.org/ontologies/Relation/Contains/weight> ?weight}{?activity ?assigned ?fg}}";
	private final String getBPActivityWeightQuery = "SELECT DISTINCT ?bp ?activity ?weight WHERE {{?bp a <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity a <http://semoss.org/ontologies/Concept/Activity>}{?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>}{?consists <http://semoss.org/ontologies/Relation/Contains/weight> ?weight}{?bp ?consists ?activity}}";
	
	// Query used for single functional gap loss
	private final String getFGActivityWeightQuery = "SELECT DISTINCT ?fg ?activity ?weight WHERE {{?fg a <http://semoss.org/ontologies/Concept/FError>}{?activity a <http://semoss.org/ontologies/Concept/Activity>}{?assigned <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned>}{?assigned <http://semoss.org/ontologies/Relation/Contains/weight> ?weight}{?activity ?assigned ?fg}}";
	
	List<Object[]> list;
	String[] names;
	
	@Override
	public List<Object[]> getTabularData() {
		return this.list;
	}
	
	@Override
	public String[] getNames() {
		return this.names;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void processQueryData() {
		EAFunctionalGapHelper helper = new EAFunctionalGapHelper();
		list = getInitialList(getInitialQuery, tapSite);
		
		// Maps for savings information
		HashMap<String, String> bpProdMap = helper.getBPProdMap(getBPProdQuery, hrCore);
		HashMap<String, ArrayList<String[]>> fccBPMap = helper.getRawOCONArrayMap(getFCCBPQuery, hrCore);
		HashMap<String, ArrayList<String[]>> mtfFCCMap = helper.getRawOCONArrayMap(getMTFFCCQuery, tapSite);
		
		// Maps for total loss information
		HashMap<String, ArrayList<String[]>> activityFGMap = helper.getRawOCONArrayMap(getActivityFGWeightQuery, hrCore);
		HashMap<String, ArrayList<String[]>> bpActivityMap = helper.getRawOCONArrayMap(getBPActivityWeightQuery, hrCore);
		
		// Maps for individual fg loss
		HashMap<String, ArrayList<String[]>> fgActivityMap = helper.getRawOCONArrayMap(getFGActivityWeightQuery, hrCore);
		
		HashMap<String, ArrayList<String>> dcSiteMTFMap = helper.getRawOCONListMap(getDCSiteMTFQuery, tapSite);
		
		// For savings this holds mapping of fcc to total productivity for the fcc
		// For losses this holds mapping of fcc to total weight of loss
		HashMap<String, Double> fccProdMap = new HashMap<String, Double>();
		HashMap<String, Double> mtfDollarsMap = new HashMap<String, Double>();
		
		HashMap<String, Double> activityWeightMap = new HashMap<String, Double>();
		
		HashMap<String, Double> dcSiteDollarsMap = new HashMap<String, Double>();
		
		if (query.contains("Savings")) {
			for (String fcc : fccBPMap.keySet()) {
				Double prod = 0.0;
				if (fccBPMap.get(fcc) != null) {
					for (String[] bp : fccBPMap.get(fcc)) {
						if (bpProdMap.get(bp[0]) != null) {
							prod += (Double.parseDouble(bp[1]) * Double.parseDouble(bpProdMap.get(bp[0])));
						}
					}
					fccProdMap.put(fcc, prod);
				}
			}
			for (String mtf : mtfFCCMap.keySet()) {
				Double savings = 0.0;
				if (mtfFCCMap.get(mtf) != null) {
					for (String[] fcc : mtfFCCMap.get(mtf)) {
						if (fccProdMap.get(fcc[0]) != null) {
							savings += (fccProdMap.get(fcc[0]) * Double.parseDouble(fcc[1]));
						}
					}
					mtfDollarsMap.put(mtf, savings);
				}
			}
		} else if (query.contains("TotalLosses")) {
			if (query.contains("SingleFG")) {
				String fg = query.substring(query.indexOf("http://health.mil/ontologies/Concept/FError/"), query.lastIndexOf(">"));
				System.out.println(fg);
				for (String[] activity : fgActivityMap.get(fg)) {
					Double lossWeight = 0.0;
					lossWeight = Double.parseDouble(activity[1]);
					if (!activityWeightMap.containsKey(activity[0])) {
						activityWeightMap.put(activity[0], lossWeight);
					} else {
						lossWeight += activityWeightMap.get(activity[0]);
						activityWeightMap.put(activity[0], lossWeight);
					}
				}
				for (String fcc : fccBPMap.keySet()) {
					Double lossPercent = 0.0;
					if (fccBPMap.get(fcc) != null) {
						for (String[] bp : fccBPMap.get(fcc)) {
							if (bpActivityMap.get(bp[0]) != null) {
								for (String[] activity : bpActivityMap.get(bp[0])) {
									if (activityWeightMap.get(activity[0]) != null) {
										lossPercent += (activityWeightMap.get(activity[0]) * Double.parseDouble(activity[1]) * Double
												.parseDouble(bp[1]));
									}
								}
							}
						}
					}
					fccProdMap.put(fcc, lossPercent);
				}
			} else {
				for (String fcc : fccBPMap.keySet()) {
					Double lossPercent = 0.0;
					if (fccBPMap.get(fcc) != null) {
						for (String[] bp : fccBPMap.get(fcc)) {
							if (bpActivityMap.get(bp[0]) != null) {
								for (String[] activity : bpActivityMap.get(bp[0])) {
									if (activityFGMap.get(activity[0]) != null) {
										for (String[] fg : activityFGMap.get(activity[0])) {
											lossPercent += (Double.parseDouble(fg[1]) * Double.parseDouble(activity[1]) * Double.parseDouble(bp[1]));
										}
									}
								}
							}
							fccProdMap.put(fcc, lossPercent);
						}
					}
				}
			}
			
			for (String mtf : mtfFCCMap.keySet()) {
				Double losses = 0.0;
				if (mtfFCCMap.get(mtf) != null) {
					for (String[] fcc : mtfFCCMap.get(mtf)) {
						if (fccProdMap.get(fcc[0]) != null) {
							losses += (fccProdMap.get(fcc[0]) * Double.parseDouble(fcc[1]));
						}
					}
					mtfDollarsMap.put(mtf, losses);
				}
			}
		}
		
		for (String dcSite : dcSiteMTFMap.keySet()) {
			Double savings = 0.0;
			if (dcSiteMTFMap.get(dcSite) != null) {
				for (String mtf : dcSiteMTFMap.get(dcSite)) {
					if (mtfDollarsMap.get(mtf) != null) {
						savings += mtfDollarsMap.get(mtf);
					}
				}
				dcSiteDollarsMap.put(dcSite, savings);
			}
		}
		
		data = new HashSet();
		String[] var = getVariableArray();
		
		// Possibly filter out all US Facilities from the query?
		
		for (int i = 0; i < list.size(); i++) {
			LinkedHashMap elementHash = new LinkedHashMap();
			Object[] listElement = list.get(i);
			if (dcSiteDollarsMap.get(listElement[0]) != null) {
				listElement[3] = dcSiteDollarsMap.get(listElement[0]);
			} else {
				listElement[3] = 0;
			}
			String colName;
			for (int j = 0; j < var.length; j++) {
				colName = var[j];
				Double dollars = 0.0;
				
				if (dcSiteDollarsMap.get(listElement[0].toString()) != null) {
					Double size = (double) Math.round(dcSiteDollarsMap.get(listElement[0].toString()));
					elementHash.put("size", size);
				} else {
					elementHash.put("size", 0);
				}
				if (j != 3) {
					elementHash.put(colName, listElement[j]);
				}
				
			}
			data.add(elementHash);
		}
		
		allHash = new Hashtable();
		allHash.put("dataSeries", data);
		
		allHash.put("lat", "lat");
		allHash.put("lon", "lon");
		allHash.put("size", "size");
		allHash.put("locationName", var[0]);
		/*
		 * allHash.put("xAxisTitle", var[0]); allHash.put("yAxisTitle", var[1]); allHash.put("value", var[2]);
		 */
		
		this.dataHash = allHash;
	}
	
	private ArrayList<Object[]> getInitialList(String query, IEngine engine) {
		ArrayList<Object[]> finalList = new ArrayList<Object[]>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		names = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Object[] temp = { sjss.getRawVar(names[0]), sjss.getVar(names[1]), sjss.getVar(names[2]), sjss.getVar(names[3]) };
			finalList.add(temp);
		}
		return finalList;
	}
}
