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

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.OCONUSMapPlaySheet;
import prerna.util.DIHelper;

public class OCONUSSavingsMapPlaySheet extends OCONUSMapPlaySheet {
	private IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
	private IEngine tapSite = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
	
	private final String getBPProdQuery = "SELECT DISTINCT ?businessProcess ?Productivity WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?businessProcess <http://semoss.org/ontologies/Relation/Contains/EA-Productivity> ?Productivity}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}}";
	private final String getFCCBPQuery = "SELECT DISTINCT ?FCC ?BusinessProcess ?weight WHERE { {?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>;}{?PartOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/PartOf> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?PartOf <http://semoss.org/ontologies/Relation/Contains/PercentBilled> ?weight}{?FCC ?PartOf ?BusinessProcess ;} }";
	private final String getMTFFCCQuery = "SELECT DISTINCT ?MTF ?FCC ?TotalCost WHERE {{?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>}{?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?FCCMTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost }{?FCC <http://semoss.org/ontologies/Relation/Has> ?FCCMTF}{?FCCMTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}}";
	private final String getDCSiteMTFQuery = "SELECT DISTINCT ?DCSite ?MTF WHERE {{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?YearQuarter}{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}}";
	
	@SuppressWarnings("unchecked")
	@Override
	public Hashtable processQueryData() {
		EAFunctionalGapHelper helper = new EAFunctionalGapHelper();
		HashMap<String, String> bpProdMap = helper.getStringMap(getBPProdQuery, hrCore);
		HashMap<String, ArrayList<String[]>> fccBPMap = helper.getStringListArrayMap(getFCCBPQuery, hrCore);
		HashMap<String, ArrayList<String[]>> mtfFCCMap = helper.getStringListArrayMap(getMTFFCCQuery, tapSite);
		HashMap<String, ArrayList<String>> dcSiteMTFMap = helper.getStringListMap(getDCSiteMTFQuery, tapSite);
		
		HashMap<String, Double> fccProdMap = new HashMap<String, Double>();
		HashMap<String, Double> mtfSavingsMap = new HashMap<String, Double>();
		HashMap<String, Double> dcSiteSavingsMap = new HashMap<String, Double>();
		
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
				mtfSavingsMap.put(mtf, savings);
			}
		}
		for (String dcSite : dcSiteMTFMap.keySet()) {
			Double savings = 0.0;
			if (dcSiteMTFMap.get(dcSite) != null) {
				for (String mtf : dcSiteMTFMap.get(dcSite)) {
					if (mtfSavingsMap.get(mtf) != null) {
						savings += mtfSavingsMap.get(mtf);
					}
				}
				dcSiteSavingsMap.put(dcSite, savings);
			}
		}
		
		data = new HashSet();
		String[] var = getVariableArray();
		
		// Possibly filter out all US Facilities from the query?
		
		for (int i = 0; i < list.size(); i++) {
			LinkedHashMap elementHash = new LinkedHashMap();
			Object[] listElement = list.get(i);
			if (dcSiteSavingsMap.get(listElement[0]) != null) {
				listElement[3] = dcSiteSavingsMap.get(listElement[0]) / 10;
			} else {
				listElement[3] = 0;
			}
			String colName;
			Double value;
			for (int j = 0; j < var.length; j++) {
				colName = var[j];
				
				if(j==3) {
				if (dcSiteSavingsMap.get(listElement[0]) != null) {
					Integer size = (int) Math.round(dcSiteSavingsMap.get(listElement[0])) / 10;
					elementHash.put("size", size);
					System.out.println("size"+size);
				} else {
					elementHash.put("size", 0);
				} }
				else{
				if (listElement[j] instanceof String) {
					String text = (String) listElement[j];
					elementHash.put(colName, text);
					System.out.println(colName+text);
				} else if (j != 3) {
					value = (Double) listElement[j];
					elementHash.put(colName, value);
					System.out.println(colName+value);
				}
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
		
		return allHash;
	}
}
