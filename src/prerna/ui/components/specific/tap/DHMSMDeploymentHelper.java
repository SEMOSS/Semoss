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
import java.util.List;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.ui.components.BooleanProcessor;
import prerna.util.Utility;

public final class DHMSMDeploymentHelper {

	public static final String SYS_AT_SITE_IN_DEPLOYMENT_PLAN_QUERY = "SELECT DISTINCT ?HostSite ?System WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } ORDER BY ?HostSite ?System";
	public static final String CENTRALLY_DEPLOYED_SYS_QUERY = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> 'Y'} }";
	
	public static final String SYS_SUSTIANMENT_BUDGET_QUERY = "SELECT DISTINCT ?System (SUM(?Cost) AS ?Sustainment) ?FY WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Grand_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } GROUP BY ?System ?FY ORDER BY ?System";
	public static final String SYS_SITE_SUPPORT_COST_QUERY = "SELECT DISTINCT ?System ?DCSite ?Cost ?FYTag WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SysSiteGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemSiteSupportGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SysSiteGLItem} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?DCSite <http://semoss.org/ontologies/Relation/Has> ?SysSiteGLItem} {?SysSiteGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SysSiteGLItem <http://semoss.org/ontologies/Relation/OccursIn> ?FYTag} } ORDER BY ?System ?DCSite";
	public static final String SYS_FLOATER_COST_QUERY = "SELECT DISTINCT ?System ?Floater ?Cost ?FYTag WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?FloaterGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FloaterGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?FloaterGLItem} {?Floater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} {?Floater <http://semoss.org/ontologies/Relation/Has> ?FloaterGLItem} {?FloaterGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?FloaterGLItem <http://semoss.org/ontologies/Relation/OccursIn> ?FYTag} } ORDER BY ?System ?Floater";
	
	public static final String SYS_COUNT_AT_SITES = "SELECT ?System (COUNT(?HostSite) AS ?NumSites) WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} } GROUP BY ?System";
	
	public static final String REGION_ORDER_QUERY = "SELECT DISTINCT ?Region1 ?Region2 WHERE {{?Region1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region1 <http://semoss.org/ontologies/Relation/Precedes> ?Region2}}";
	public static final String FIRST_REGION_QUERY = "SELECT DISTINCT ?Region1 WHERE { {?Region1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} MINUS{ {?Region2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region2 <http://semoss.org/ontologies/Relation/Precedes> ?Region1}}  }";
	public static final String CONTAIN_IOC_QUERY = "ASK WHERE {BIND(<http://semoss.org/ontologies/Concept/Region/IOC> AS ?IOC)}";

	public static final String WAVES_IN_REGION_QUERY = "SELECT DISTINCT ?Region ?Wave WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave}}";
	
	public static final String WAVE_ORDER_QUERY = "SELECT DISTINCT ?Wave1 ?Wave2 WHERE {{?Wave1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave1 <http://semoss.org/ontologies/Relation/Precedes> ?Wave2}} ";
	public static final String FIRST_WAVE_QUERY = "SELECT DISTINCT ?Wave1 WHERE { {?Wave1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} MINUS{ {?Wave2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave2 <http://semoss.org/ontologies/Relation/Precedes> ?Wave1}} }";
	public static final String WAVE_START_END_DATE = "SELECT DISTINCT ?Wave ?StartDate ?EndDate WHERE { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?StartDate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Wave <http://semoss.org/ontologies/Relation/BeginsOn> ?StartDate} {?EndDate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?EndDate} }";
	public static final String SYS_IN_WAVES_QUERY = "SELECT DISTINCT ?System ?Wave WHERE { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} }";

	public static final String SYS_DEPLOYED_AT_SITE_QUERY = "SELECT DISTINCT ?HostSite ?System WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } ORDER BY ?HostSite ?System";
	public static final String SITE_IN_WAVES_QUERY = "SELECT DISTINCT ?HostSite ?Wave WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite}} ORDER BY ?HostSite";
	public static final String SITE_IN_MULTIPLE_WAVES_QUERY = "SELECT DISTINCT ?HostSite ?Wave WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} FILTER( ?WaveNum > 1) { SELECT DISTINCT ?HostSite (COUNT(DISTINCT ?Wave) AS ?WaveNum) WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} } GROUP BY ?HostSite } } ORDER BY ?HostSite";
	public static final String SITE_IN_MULTIPLE_WAVES_COUNT_QUERY = "SELECT DISTINCT ?HostSite (COUNT(DISTINCT ?Wave) AS ?WaveCount) WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} FILTER( ?WaveNum > 1) { SELECT DISTINCT ?HostSite (COUNT(DISTINCT ?Wave) AS ?WaveNum) WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} } GROUP BY ?HostSite } } GROUP BY ?HostSite ORDER BY ?HostSite";
	
	public static final String FLOATER_SUPPORTS_WAVE_QUERY = "SELECT DISTINCT ?Floater ?Wave WHERE { {?Floater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Floater <http://semoss.org/ontologies/Relation/Supports> ?Wave} } ORDER BY ?Floater";
	
	public static final String GET_HP_SYSTEM_LIST = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob }  } BINDINGS ?Prob {('High')('Question')}";
	
	public static final String GET_SITE_LOCATION_QUERY = "SELECT DISTINCT ?dcSite ?lat ?long WHERE { {?dcSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?dcSite <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat }  {?dcSite <http://semoss.org/ontologies/Relation/Contains/LONG> ?long } }";
	
	public static final String REGION_START_Q_KEY = "REGION_START_Q";
	public static final String REGION_START_Y_KEY = "REGION_START_Y";
	public static final String REGION_END_Q_KEY = "REGION_END_Q";
	public static final String REGION_END_Y_KEY = "REGION_END_Y";
	
	private DHMSMDeploymentHelper() {
		
	}
	
	public static HashMap<String, HashMap<String, Double>> getSiteLocation(IEngine engine) {
		HashMap<String, HashMap<String, Double>> retHash = new HashMap<String, HashMap<String, Double>>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, GET_SITE_LOCATION_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String site = sjss.getVar(names[0]).toString();
			Double lat = (double) sjss.getVar(names[1]);
			Double lon = (double) sjss.getVar(names[2]);
			
			HashMap<String, Double> innerHash = new HashMap<String, Double>();
			innerHash.put("Long", lon);
			innerHash.put("Lat", lat);
			retHash.put(site, innerHash);
		}
		
		return retHash;
	}
	
	public static Set<String> getHPSysList(IEngine engine) {
		Set<String> sysList = new HashSet<String>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, GET_HP_SYSTEM_LIST);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			sysList.add(sjsw.next().getVar(names[0]).toString());
		}
		
		return sysList;
	}
	
	public static HashMap<String, String> getFirstWaveForEachSystem(IEngine engine) {
		ArrayList<String> waveOrder = getWaveOrder(engine);
		return getFirstWaveForEachSystem(engine, waveOrder);
	}
	
	public static HashMap<String, String> getFirstWaveForEachSystem(IEngine engine, ArrayList<String> waveOrder) {
		HashMap<String, List<String>> inputHash = new HashMap<String, List<String>>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_IN_WAVES_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString();
			String wave = sjss.getVar(names[1]).toString();
			List<String> waveList;
			if(inputHash.containsKey(sys)) {
				waveList = inputHash.get(sys);
				waveList.add(wave);
			} else {
				waveList = new ArrayList<String>();
				waveList.add(wave);
				inputHash.put(sys, waveList);
			}
		}
		
		return determineFirstWaveForInput(waveOrder, inputHash);
	}
	
	public static HashMap<String, String> getLastWaveForEachSystem(IEngine engine) {
		ArrayList<String> waveOrder = getWaveOrder(engine);
		return getLastWaveForEachSystem(engine, waveOrder);
	}
	
	public static HashMap<String, String> getLastWaveForEachSystem(IEngine engine, ArrayList<String> waveOrder) {
		HashMap<String, List<String>> inputHash = new HashMap<String, List<String>>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_IN_WAVES_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString();
			String wave = sjss.getVar(names[1]).toString();
			List<String> waveList;
			if(inputHash.containsKey(sys)) {
				waveList = inputHash.get(sys);
				waveList.add(wave);
			} else {
				waveList = new ArrayList<String>();
				waveList.add(wave);
				inputHash.put(sys, waveList);
			}
		}
		
		return determineLastWaveForInput(waveOrder, inputHash);
	}
	
	public static HashMap<String, String> determineLastWaveForInput(ArrayList<String> waveOrder, HashMap<String, List<String>> waveHash) {
		HashMap<String, String> retHash = new HashMap<String, String>();

		for(String entity : waveHash.keySet()) {
			List<String> waveList = waveHash.get(entity);
			int lastWaveIndex = 0;
			String lastWave = "";
			for(String wave : waveList) {
				int index = waveOrder.indexOf(wave);
				if(lastWaveIndex < index) {
					lastWaveIndex = index;
					lastWave = wave;
				}
			}
			retHash.put(entity, lastWave);
		}
		
		return retHash;
	}
	
	public static HashMap<String, String> determineFirstWaveForInput(ArrayList<String> waveOrder, HashMap<String, List<String>> waveHash) {
		HashMap<String, String> retHash = new HashMap<String, String>();

		for(String entity : waveHash.keySet()) {
			List<String> waveList = waveHash.get(entity);
			int firstWaveIndex = waveOrder.size()-1;
			String firstWave = "";
			for(String wave : waveList) {
				int index = waveOrder.indexOf(wave);
				if(firstWaveIndex > index) {
					firstWaveIndex = index;
					firstWave = wave;
				}
			}
			retHash.put(entity, firstWave);
		}
		
		return retHash;
	}
	
	public static HashMap<String, HashMap<String, Double>> getSysFloaterCost(IEngine engine) {
		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_FLOATER_COST_QUERY);
		
		return processCosts(sjsw);
	}
	
	public static HashMap<String, HashMap<String, Double>> getSysSiteSupportCost(IEngine engine) {
		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_SITE_SUPPORT_COST_QUERY);

		return processCosts(sjsw);
	}
	
	private static HashMap<String, HashMap<String, Double>> processCosts(ISelectWrapper sjsw) {
		HashMap<String, HashMap<String, Double>> retHash = new HashMap<String, HashMap<String, Double>>();
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String system = sjss.getVar(names[0]).toString();
			String floater = sjss.getVar(names[1]).toString();
			Double cost = (Double) sjss.getVar(names[2]);
			
			HashMap<String, Double> innerHash;
			if(retHash.containsKey(system)) {
				innerHash = retHash.get(system);
				// each floater/dcsite occurs once so no need to worry about overriding keys
				innerHash.put(floater, cost);
			} else {
				innerHash = new HashMap<String, Double>();
				innerHash.put(floater, cost);
				retHash.put(system, innerHash);
			}
		}
		
		return retHash;
	}
	
	public static HashMap<String, List<String>> getFloatersAndWaves(IEngine engine) {
		HashMap<String, List<String>> retHash = new HashMap<String, List<String>>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, FLOATER_SUPPORTS_WAVE_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String floater = sjss.getVar(names[0]).toString();
			String wave = sjss.getVar(names[1]).toString();
			List<String> waveList;
			if(retHash.containsKey(floater)) {
				waveList = retHash.get(floater);
				waveList.add(wave);
			} else {
				waveList = new ArrayList<String>();
				waveList.add(wave);
				retHash.put(floater, waveList);
			}
		}
		
		return retHash;
	}
	
	public static Hashtable<String,Hashtable<String,Integer>> getRegionStartAndEndDate(IEngine siteEngine,Hashtable<String, List<String>> regionWaveHash,HashMap<String, String[]> waveStartEndDate) {
		Hashtable<String,Hashtable<String,Integer>> regionStartEndDate = new Hashtable<String,Hashtable<String,Integer>>();
		
		Hashtable<String,Integer> regionStartQ = new Hashtable<String,Integer>();
		Hashtable<String,Integer> regionStartY = new Hashtable<String,Integer>();
		Hashtable<String,Integer> regionEndQ = new Hashtable<String,Integer>();
		Hashtable<String,Integer> regionEndY = new Hashtable<String,Integer>();
		
		for(String region : regionWaveHash.keySet()) {
			List<String> waves = regionWaveHash.get(region);
			if(!waves.isEmpty()) {
				String[] maxStartEnd = waveStartEndDate.get(waves.get(0));
				int earlyStartQ = Integer.parseInt(maxStartEnd[0].substring(1,2));
				int earlyStartY = Integer.parseInt(maxStartEnd[0].substring(6));
				int lateEndQ = Integer.parseInt(maxStartEnd[1].substring(1,2));
				int lateEndY = Integer.parseInt(maxStartEnd[1].substring(6));
				
				for(String wave : waves) {
					String[] startEnd = waveStartEndDate.get(wave);
					int startQ = Integer.parseInt(startEnd[0].substring(1,2));
					int startY = Integer.parseInt(startEnd[0].substring(6));
					int endQ = Integer.parseInt(startEnd[1].substring(1,2));
					int endY = Integer.parseInt(startEnd[1].substring(6));
					
					if(compareTo(earlyStartQ,earlyStartY,startQ,startY)<0) {
						earlyStartQ = startQ;
						earlyStartY = startY;
					}
					if(compareTo(lateEndQ,lateEndY,endQ,endY)>0) {
						lateEndQ = endQ;
						lateEndY = endY;
					}
				}
				regionStartQ.put(region,earlyStartQ);
				regionStartY.put(region,earlyStartY);
				regionEndQ.put(region,lateEndQ);
				regionEndY.put(region,lateEndY);
			}
		}

		regionStartEndDate.put(REGION_START_Q_KEY,regionStartQ);
		regionStartEndDate.put(REGION_START_Y_KEY,regionStartY);
		regionStartEndDate.put(REGION_END_Q_KEY,regionEndQ);
		regionStartEndDate.put(REGION_END_Y_KEY,regionEndY);
		
		return regionStartEndDate;
	}
	
	public static HashMap<String, String[]> getWaveStartAndEndDate(IEngine engine) {
		HashMap<String, String[]> retHash = new HashMap<String, String[]>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, WAVE_START_END_DATE);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String site = sjss.getVar(names[0]).toString();
			String[] dateArr = new String[2];
			dateArr[0] = sjss.getVar(names[1]).toString();
			dateArr[1] = sjss.getVar(names[2]).toString();
			retHash.put(site, dateArr);
		}
		
		return retHash;
	}
	
	public static HashMap<String, Double> getSiteAndMultipleWaveCount(IEngine engine) {
		HashMap<String, Double> retHash = new HashMap<String, Double>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, SITE_IN_MULTIPLE_WAVES_COUNT_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String site = sjss.getVar(names[0]).toString();
			Double count = (Double) sjss.getVar(names[1]);
			retHash.put(site, count);
		}
		
		return retHash;
	}
	
	public static HashMap<String, List<String>> getSitesAndMultipleWaves(IEngine engine) {
		HashMap<String, List<String>> retHash = new HashMap<String, List<String>>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, SITE_IN_MULTIPLE_WAVES_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String site = sjss.getVar(names[0]).toString();
			String wave = sjss.getVar(names[1]).toString();
			List<String> waveList;
			if(retHash.containsKey(site)) {
				waveList = retHash.get(site);
				waveList.add(wave);
			} else {
				waveList = new ArrayList<String>();
				waveList.add(wave);
				retHash.put(site, waveList);
			}
		}
		
		return retHash;
	}
	
	public static HashMap<String, List<String>> getSitesAndWaves(IEngine engine) {
		HashMap<String, List<String>> retHash = new HashMap<String, List<String>>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, SITE_IN_WAVES_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String site = sjss.getVar(names[0]).toString();
			String wave = sjss.getVar(names[1]).toString();
			List<String> waveList;
			if(retHash.containsKey(site)) {
				waveList = retHash.get(site);
				waveList.add(wave);
			} else {
				waveList = new ArrayList<String>();
				waveList.add(wave);
				retHash.put(site, waveList);
			}
		}
		
		return retHash;
	}
	
	/**
	 * Returns the order of the regions.
	 * Includes IOC if it exists
	 * @param engine
	 * @param includeIOC
	 * @return
	 */
	public static ArrayList<String> getRegionOrder(IEngine engine,Boolean includeIOC) {

		ArrayList<String> regionOrder = new ArrayList<String>();
		if(includeIOC) {
			BooleanProcessor proc = new BooleanProcessor();
			proc.setQuery(CONTAIN_IOC_QUERY);
			proc.setEngine(engine);
			if(proc.processQuery());
				regionOrder.add("IOC");
		}
		ISelectWrapper sjsw = Utility.processQuery(engine, FIRST_REGION_QUERY);
		String[] names = sjsw.getVariables();
		String firstRegion = sjsw.next().getVar(names[0]).toString();
		
		sjsw = Utility.processQuery(engine, REGION_ORDER_QUERY);
		names = sjsw.getVariables();
		HashMap<String, String> regionHash = new HashMap<String, String>();
		int counter = 0;
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String region1 = sjss.getVar(names[0]).toString();
			String region2 = sjss.getVar(names[1]).toString();
			regionHash.put(region1, region2);
			counter++;
		}
		String nextRegion = firstRegion;
		regionOrder.add(nextRegion);
		for(int i = 0; i < counter; i++) {
			nextRegion = regionHash.get(nextRegion);
			regionOrder.add(nextRegion);
		}
		
		return regionOrder;
	}
	
	public static ArrayList<String> getWaveOrder(IEngine engine) {
		ISelectWrapper sjsw = Utility.processQuery(engine, FIRST_WAVE_QUERY);
		String[] names = sjsw.getVariables();
		String firstWave = sjsw.next().getVar(names[0]).toString();
	
	
		sjsw = Utility.processQuery(engine, WAVE_ORDER_QUERY);
		names = sjsw.getVariables();
		HashMap<String, String> regionHash = new HashMap<String, String>();
		int counter = 0;
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String region1 = sjss.getVar(names[0]).toString();
			String region2 = sjss.getVar(names[1]).toString();
			regionHash.put(region1, region2);
			counter++;
		}
		String nextWave = firstWave;
		ArrayList<String> waveOrder = new ArrayList<String>();
		waveOrder.add(nextWave);
		for(int i = 0; i < counter; i++) {
			nextWave = regionHash.get(nextWave);
			waveOrder.add(nextWave);
		}
		
		return waveOrder;
	}
	
	public static Hashtable<String, List<String>> getWavesInRegion(IEngine engine) {
		
		ISelectWrapper sjsw = Utility.processQuery(engine,WAVES_IN_REGION_QUERY);
		String[] names = sjsw.getVariables();

		Hashtable<String, List<String>> regionWaveHash = new Hashtable<String, List<String>>();
		
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String region = sjss.getVar(names[0]).toString();
			String wave = sjss.getVar(names[1]).toString();

			List<String> waveValues;
			if(regionWaveHash.containsKey(region)) {
				waveValues = regionWaveHash.get(region);
				waveValues.add(wave);
			} else {
				waveValues = new ArrayList<String>();
				waveValues.add(wave);
				regionWaveHash.put(region, waveValues);
			}
		}
		return regionWaveHash;
	}
	
//	public static HashMap<String, String> getRegionStartDate(IEngine engine) {
//		HashMap<String, String> retHash = new HashMap<String, String>();
//		
//		ISelectWrapper sjsw = Utility.processQuery(engine, REGION_START_DATE);
//		String[] names = sjsw.getVariables();
//		while(sjsw.hasNext()) {
//			ISelectStatement sjss = sjsw.next();
//			String regionName = sjss.getVar(names[0]).toString();
//			String regionStartDate = sjss.getVar(names[1]).toString();
//			retHash.put(regionName, regionStartDate);
//		}
//		
//		return retHash;
//	}
	
	public static HashMap<String, Integer> getNumSitesSysDeployedAt(IEngine engine) {
		HashMap<String, Integer> retHash = new HashMap<String, Integer>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_COUNT_AT_SITES);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String sysName = sjss.getVar(names[0]).toString();
			Integer siteCount = ((Double) sjss.getVar(names[1])).intValue();
			retHash.put(sysName, siteCount);
		}
		
		return retHash;
	}
	
	public static HashMap<String, Double[]> getSysSustainmentBudget(IEngine engine) {
		HashMap<String, Double[]> retHash = new HashMap<String, Double[]>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_SUSTIANMENT_BUDGET_QUERY);
		String[] names1 = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String sysName = sjss.getVar(names1[0]).toString();
			Double sustCost;
			if(sjss.getVar(names1[1]).toString().equals("")){
				sustCost = 0.0;
			} else {
				sustCost = Double.parseDouble(sjss.getVar(names1[1]).toString());
			}
			String fiscalYear = sjss.getVar(names1[2]).toString();;
			// order fiscal year information
			int position = 0;
			switch(fiscalYear) {
				case "FY15" : position = 0; break;
				case "FY16" : position = 1; break;
				case "FY17" : position = 2; break;
				case "FY18" : position = 3; break;
				case "FY19" : position = 4; break;
			}
			Double[] sysSustCost;
			if(retHash.containsKey(sysName)) {
				sysSustCost = retHash.get(sysName);
				sysSustCost[position] = sustCost;
			} else {
				sysSustCost = new Double[5];
				sysSustCost[position] = sustCost;
				retHash.put(sysName, sysSustCost);
			}
		}
		
		return retHash;
	}
	
	
	public static HashMap<String, ArrayList<String>> getSysAtSitesInDeploymentPlan(IEngine engine) {
		HashMap<String, ArrayList<String>> retHash = new HashMap<String, ArrayList<String>>();
		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_AT_SITE_IN_DEPLOYMENT_PLAN_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String site = sjss.getVar(names[0]).toString();
			String sys = sjss.getVar(names[1]).toString();
			ArrayList<String> sysList;
			if(retHash.containsKey(site)) {
				sysList = retHash.get(site);
				sysList.add(sys);
			} else {
				sysList = new ArrayList<String>();
				sysList.add(sys);
				retHash.put(site, sysList);
			}
		}
		return retHash;
	}
	
	public static Set<String> getCentrallyDeployedSystems(IEngine engine) {
		Set<String> retSet = new HashSet<String>();
		ISelectWrapper sjsw = Utility.processQuery(engine, CENTRALLY_DEPLOYED_SYS_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString();
			retSet.add(sys);
		}
		return retSet;
	}
	/**
	 * compares two dates to see which comes earlier.
	 * if the first date is before, returns 1
	 * if the same date, returns 0
	 * if the first date is after, returns -1
	 * @param firstQ
	 * @param firstY
	 * @param secondQ
	 * @param secondY
	 * @return
	 */
	private static int compareTo(int firstQ, int firstY, int secondQ, int secondY) {
		if(firstY<secondY) {
			return 1;
		} else if(firstY==secondY){
			if(firstQ<secondQ) {
				return 1;
			}else if(firstQ==secondQ) {
				return 0;
			}else if(firstQ > secondY) {
				return -1;
			}
		}
		return -1;
	}

}
