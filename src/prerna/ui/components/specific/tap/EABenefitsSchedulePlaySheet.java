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

import javax.swing.JDesktopPane;
import prerna.engine.api.IEngine;
import prerna.ui.components.playsheets.ColumnChartPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class EABenefitsSchedulePlaySheet extends GridPlaySheet {
	private IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
	private IEngine tapSite = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
	private EAFunctionalGapHelper helper = new EAFunctionalGapHelper();
	
	private final String getBusinessProcessQuery = "SELECT DISTINCT ?businessProcess WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}}";
	private final String getBPFCCQuery = "SELECT DISTINCT ?BusinessProcess ?FCC ?weight WHERE { {?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>;}{?PartOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/PartOf> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?PartOf <http://semoss.org/ontologies/Relation/Contains/PercentBilled> ?weight}{?FCC ?PartOf ?BusinessProcess ;} }";
	private final String getFCCBPQuery = "SELECT DISTINCT ?FCC ?BusinessProcess ?weight WHERE { {?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>;}{?PartOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/PartOf> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?PartOf <http://semoss.org/ontologies/Relation/Contains/PercentBilled> ?weight}{?FCC ?PartOf ?BusinessProcess ;} }";
	private final String getMTFQuery = "SELECT DISTINCT ?MTF ?FCC ?TotalCost WHERE {{?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>}{?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?FCCMTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost }{?FCC <http://semoss.org/ontologies/Relation/Has> ?FCCMTF}{?FCCMTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}}";
	private final String getMTFYearQuery = "SELECT DISTINCT ?MTF ?Year WHERE {{?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>}{?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?FCCMTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost }{?FCC <http://semoss.org/ontologies/Relation/Has> ?FCCMTF}{?FCCMTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}}";
	private final String getYearInflationQuery = "SELECT DISTINCT ?Year ?Inflation WHERE {{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?Year <http://semoss.org/ontologies/Relation/Contains/InflationRate> ?Inflation}}";
	private final String getMTFRegionQuery = "SELECT DISTINCT ?MTF ?Region WHERE {{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>}{?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}}";
	
	private ArrayList<String> bpList;
	private HashMap<String, ArrayList<String[]>> bpFCCs;
	private HashMap<String, ArrayList<String[]>> fccBP;
	private HashMap<String, HashMap<String, Double>> yearFCCCost;
	private HashMap<String, HashMap<String, Double>> mtfFCCCost;
	private HashMap<String, String> mtfYear;
	private HashMap<String, String> yearInflation;
	private HashMap<String, ArrayList<String>> mtfRegion;
	
	HashMap<String, Double> effectPercentMap;
	HashMap<String, Double> efficiencyPercentMap;
	HashMap<String, Double> productivityPercentMap;
	
	@Override
	public void createData() {
		bpList = helper.getStringList(getBusinessProcessQuery, hrCore);
		bpFCCs = helper.getStringListArrayMap(getBPFCCQuery, hrCore);
		fccBP = helper.getStringListArrayMap(getFCCBPQuery, hrCore);
		yearFCCCost = helper.getCostPerYear();
		mtfFCCCost = helper.getDoubleMap(getMTFQuery, tapSite);
		mtfYear = helper.getWaveMap(getMTFYearQuery, tapSite);
		yearInflation = helper.getStringMap(getYearInflationQuery, tapSite);
		mtfRegion = helper.getStringListMap(getMTFRegionQuery, tapSite);
	}
	
	@Override
	public void runAnalytics() {
		list = new ArrayList<Object[]>();
		// for column headers
		if (query.contains("1")) { // EA-Perspective question 16
			names = new String[4];
			names[0] = "Year";
			names[1] = "Effectiveness";
			names[2] = "Efficiency";
			names[3] = "Productivity";
		} else if (query.contains("2")) { // EA-Perspective question 18
			names = new String[17];
			names[0] = "Region";
			names[1] = "MTF";
			names[2] = "FY18";
			names[3] = "FY19";
			names[4] = "FY20";
			names[5] = "FY21";
			names[6] = "FY22";
			names[7] = "FY23";
			names[8] = "FY24";
			names[9] = "FY25";
			names[10] = "FY26";
			names[11] = "FY27";
			names[12] = "FY28";
			names[13] = "FY29";
			names[14] = "FY30";
			names[15] = "FY31";
			names[16] = "FY32";
			
		} else if (query.contains("3")) { // EA-Perspective question 17
			names = new String[16];
			names[0] = "BP";
			names[1] = "Effectiveness";
			names[2] = "Efficiency";
			names[3] = "Productivity";
			names[4] = "FY18 Total Cost";
			names[5] = "FY18 Total Savings";
			names[6] = "FY19 Total Cost";
			names[7] = "FY19 Total Savings";
			names[8] = "FY20 Total Cost";
			names[9] = "FY20 Total Savings";
			names[10] = "FY21 Total Cost";
			names[11] = "FY21 Total Savings";
			names[12] = "FY22 Total Cost";
			names[13] = "FY22 Total Savings";
			names[14] = "FY23 Total Cost";
			names[15] = "FY23 Total Savings";
		}
		
		effectPercentMap = new HashMap<String, Double>();
		efficiencyPercentMap = new HashMap<String, Double>();
		productivityPercentMap = new HashMap<String, Double>();
		HashMap<String, HashMap<String, Double>> effectSavingsMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> efficiencySavingsMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> productivitySavingsMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> bpCosts = new HashMap<String, HashMap<String, Double>>();
		
		EAEffectivenessPlaySheet effectiveness = new EAEffectivenessPlaySheet();
		EAFunctionalGapPlaySheet efficiency = new EAFunctionalGapPlaySheet();
		
		effectiveness.createData();
		effectiveness.setQuery("Schedule");
		effectiveness.runAnalytics();
		effectPercentMap = effectiveness.effectPercent;
		
		efficiency.createData();
		efficiency.setQuery("Schedule");
		efficiency.runAnalytics();
		efficiencyPercentMap = efficiency.efficiencyPercent;
		
		for (String bp : bpList) {
			Double effectPercent = effectPercentMap.get(bp);
			Double efficiencyPercent = efficiencyPercentMap.get(bp);
			Double productivityPercent = 0.0;
			
			productivityPercent = effectPercent + efficiencyPercent;
			productivityPercentMap.put(bp, productivityPercent);
		}
		
		for (String year : yearFCCCost.keySet()) {
			for (String bp : bpList) {
				Double bpCost = 0.0;
				if (bpFCCs.get(bp) != null) {
					for (String[] bpFCC : bpFCCs.get(bp)) {
						if (yearFCCCost.get(year).containsKey(bpFCC[0])) {
							bpCost += Double.parseDouble(bpFCC[1]) * yearFCCCost.get(year).get(bpFCC[0]);
						}
					}
				}
				if (!bpCosts.containsKey(year)) {
					bpCosts.put(year, new HashMap<String, Double>());
				}
				bpCosts.get(year).put(bp, bpCost);
			}
		}
		
		for (String year : bpCosts.keySet()) {
			if (!effectSavingsMap.containsKey(year)) {
				effectSavingsMap.put(year, new HashMap<String, Double>());
			}
			if (!efficiencySavingsMap.containsKey(year)) {
				efficiencySavingsMap.put(year, new HashMap<String, Double>());
			}
			if (!productivitySavingsMap.containsKey(year)) {
				productivitySavingsMap.put(year, new HashMap<String, Double>());
			}
			
			for (String bp : bpCosts.get(year).keySet()) {
				effectSavingsMap.get(year).put(bp, effectPercentMap.get(bp) * bpCosts.get(year).get(bp));
				efficiencySavingsMap.get(year).put(bp, efficiencyPercentMap.get(bp) * bpCosts.get(year).get(bp));
				productivitySavingsMap.get(year).put(bp, productivityPercentMap.get(bp) * bpCosts.get(year).get(bp));
			}
		}
		
		if (query.contains("1")) {
			HashMap<String, Double[]> savingsPerYear = new HashMap<String, Double[]>();
			for (String year : bpCosts.keySet()) {
				Double totalEffect = 0.0;
				Double totalEfficiency = 0.0;
				Double totalProductivity = 0.0;
				for (String bp : bpCosts.get(year).keySet()) {
					totalEffect += effectSavingsMap.get(year).get(bp);
					totalEfficiency += efficiencySavingsMap.get(year).get(bp);
					totalProductivity += productivitySavingsMap.get(year).get(bp);
				}
				Double[] temp = { totalEffect, totalEfficiency, totalProductivity };
				savingsPerYear.put(year, temp);
			}
			for (Integer year = 2017; year < 2032; year++) {
				Double totalEffect = 0.0;
				Double totalEfficiency = 0.0;
				Double totalProductivity = 0.0;
				for (Integer i = year; i > 2016; i--) {
					if (savingsPerYear.get(i.toString()) != null) {
						totalEffect += savingsPerYear.get(i.toString())[0];
						totalEfficiency += savingsPerYear.get(i.toString())[1];
						totalProductivity += savingsPerYear.get(i.toString())[2];
					}
				}
				Integer realizedYear = year + 1;
				Double inflation = Double.parseDouble(yearInflation.get(realizedYear.toString()));
				totalEffect *= inflation;
				totalEfficiency *= inflation;
				totalProductivity *= inflation;
				Object[] temp = { realizedYear, totalEffect, totalEfficiency, totalProductivity };
				list.add(temp);
			}
			// if (query.contains("barGraph")) {
			ColumnChartPlaySheet graph = new ColumnChartPlaySheet();
			graph.setNames(names);
			graph.setList(list);
			JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
			graph.setJDesktopPane(pane);
			graph.setDataHash(graph.processQueryData());
			graph.createView();
			// }
		} else if (query.contains("2")) {
			if (query.contains("mtfParameter")) {
				
			} else {
				for (String mtf : mtfFCCCost.keySet()) {
					Object[] temp = new Object[17];
					temp[1] = mtf;
					String regionList = null;
					for (String region : mtfRegion.get(mtf)) {
						if (regionList == null) {
							regionList = region;
						} else {
							regionList = regionList + ", " + region;
						}
					}
					temp[0] = regionList;
					for (int i = 2; i < 17; i++) {
						temp[i] = 0.0;
					}
					Double totalCost = 0.0;
					for (String fcc : mtfFCCCost.get(mtf).keySet()) {
						if (fccBP.get(fcc) != null) {
							for (String bp[] : fccBP.get(fcc)) {
								if (productivityPercentMap.get(bp[0]) != null) {
									totalCost += (mtfFCCCost.get(mtf).get(fcc) * Double.parseDouble(bp[1]) * productivityPercentMap.get(bp[0]));
								}
							}
						}
					}
					Integer year = Integer.parseInt(mtfYear.get(mtf));
					for (Integer i = 2015; (year - i) < 17; year++) {
						Integer inflationYear = year + 1;
						Double inflation = Double.parseDouble(yearInflation.get(inflationYear.toString()));
						temp[year - i] = (totalCost * inflation); // ***inflation
					}
					list.add(temp);
				}
			}
		} else if (query.contains("3")) {
			if (query.contains("bpParameter")) {
				String singleBP = query.substring(query.indexOf("BusinessProcess/") + 16, query.indexOf(">"));
				Object[] temp = new Object[16];
				temp[0] = singleBP;
				temp[1] = effectPercentMap.get(singleBP);
				temp[2] = efficiencyPercentMap.get(singleBP);
				temp[3] = productivityPercentMap.get(singleBP);
				int step = 0;
				for (Integer year = 2017; year < 2023; year++) {
					temp[year - 2013 + step] = bpCosts.get(year.toString()).get(singleBP);
					temp[year - 2012 + step] = productivitySavingsMap.get(year.toString()).get(singleBP);
					step++;
				}
				list.add(temp);
			} else {
				for (String bp : bpList) {
					Object[] temp = new Object[16];
					temp[0] = bp;
					temp[1] = effectPercentMap.get(bp);
					temp[2] = efficiencyPercentMap.get(bp);
					temp[3] = productivityPercentMap.get(bp);
					int step = 0;
					for (Integer year = 2017; year < 2023; year++) {
						temp[year - 2013 + step] = bpCosts.get(year.toString()).get(bp);
						temp[year - 2012 + step] = productivitySavingsMap.get(year.toString()).get(bp);
						step++;
					}
					list.add(temp);
				}
			}
		}
	}
}
