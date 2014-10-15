/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SavingsPerFiscalYearBySystemPlaySheet extends GridPlaySheet {

	private final String query5 = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Region ?Wave ?DCSite ?System WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} }&SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";

	private final String query1 = "SELECT DISTINCT ?System (SUM(?Cost) AS ?Sustainment) ?FY WHERE { { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?OMTag <http://semoss.org/ontologies/Relation/Includes> ?GLTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?SystemServiceBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudget} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?SystemServiceBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudget} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?OMTag <http://semoss.org/ontologies/Relation/Includes> ?GLTag} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } } GROUP BY ?System ?FY ORDER BY ?System";
	private final String query2 = "SELECT ?System (COUNT(?DCSite) AS ?Sites) WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} } GROUP BY ?System";
	private final String query3 = "SELECT DISTINCT ?Region ?Date WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>}{?Date <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Region <http://semoss.org/ontologies/Relation/BeginsOn> ?Date} }";
	private final String waveProcedesQuery = "SELECT DISTINCT ?Wave1 ?Wave2 WHERE {{?Wave1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave1 <http://semoss.org/ontologies/Relation/Preceeds> ?Wave2}} ";
	private final String regionProcedesQuery = "SELECT DISTINCT ?Region1 ?Region2 WHERE {{?Region1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region1 <http://semoss.org/ontologies/Relation/Preceeds> ?Region2}}";
	
	private final String firstRegionQuery = "SELECT DISTINCT ?Region1 WHERE { {?Region1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} MINUS{ {?Region2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region2 <http://semoss.org/ontologies/Relation/Preceeds> ?Region1}}  }";
	private final String firstWaveQuery = "SELECT DISTINCT ?Wave1 WHERE { {?Wave1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} MINUS{ {?Wave2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave2 <http://semoss.org/ontologies/Relation/Preceeds> ?Wave1}}  }";
	
	private final String engineName1 = "TAP_Portfolio";
	private final String engineName2 = "TAP_Site_Data";
	private IEngine engine1;
	private IEngine engine2;
	private DualEngineGridPlaySheet dualQueries = new DualEngineGridPlaySheet();	
	private HashMap<String, Double[]> query1Data = new HashMap<String, Double[]>();
	private HashMap<String, Double> query2Data = new HashMap<String, Double>();
	private HashMap<String, String> query3Data = new HashMap<String, String>();
	private HashMap<String, String> query4Data = new HashMap<String, String>();
	private HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>> query5Data = new HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>>();
	private HashMap<String, double[]> savingsData = new HashMap<String, double[]>();
	
	private String firstRegion;
	private String firstWave;
	private ArrayList<String> regionOrder;
	private ArrayList<String> waveOrder;
	
	@Override
	public void createData(){
		runAllQueries();
		processData();
	}
	
	private void processData() {
		HashMap<String, Double[]> siteCostSavings = new HashMap<String, Double[]>();
		
		int minYear = 3000;//arbitrarily large year
		int maxYear = 0;
		for(String region : query3Data.keySet()) {
			String startDate = query3Data.get(region);
			String time[] = startDate.split("FY");
			String year = time[1];
			int yearAsNum = Integer.parseInt(year);
			if(yearAsNum > maxYear) {
				maxYear = yearAsNum;
			} else if(yearAsNum < minYear) {
				minYear = yearAsNum;
			}
		}
		int numColumns = maxYear - minYear + 1;
		
		int i;
		for(i = 0; i < regionOrder.size(); i++) 
		{
			String region = regionOrder.get(i);
			String startDate = query3Data.get(region);
			String time[] = startDate.split("FY");
			String quarter = time[0];
			String year = time[1];
			if(query5Data.get(region) != null){
				int numWaves = query5Data.get(region).keySet().size();
				double perOfYear = 1.0/numWaves;
				double startPercent = 0;
				int sustainmentIndex = 0;
				switch(quarter) {
					case "Q1" : startPercent = 0; break;
					case "Q2" : startPercent = 0.25; break;
					case "Q3" : startPercent = 0.50; break;
					case "Q4" : startPercent = 0.75; break;
				}
				switch(year) {
					case "2015" : sustainmentIndex = 0; break;
					case "2016" : sustainmentIndex = 1; break;
					case "2017" : sustainmentIndex = 2; break;
					case "2018" : sustainmentIndex = 3; break;
					default: sustainmentIndex = 4;
				}
				int outputYear = sustainmentIndex;
				HashMap<String, HashMap<String, ArrayList<String>>> waves = query5Data.get(region);
				
				int j;
				for(j = 0; j < waveOrder.size(); j++) {
					String wave = waveOrder.get(j);
					if(waves.keySet().contains(wave)){
						HashMap<String, ArrayList<String>> sites = waves.get(wave);
						startPercent += perOfYear;
						if(startPercent >= 1.0){
							startPercent -= 1.0;
							outputYear++;//we assume all of the saving comes at the end
						}
						for(String site : sites.keySet()){
							ArrayList<String> systems = sites.get(site);
							double savings = 0.0;
							double [] yearlySavings = new double[numColumns];
							for(String system : systems){
								Double [] costs = query1Data.get(system);
								// assume cost for a specific site is total cost / num sites
								double numSites = query2Data.get(system);
								if(costs != null){
									if(costs[sustainmentIndex].equals(null)){
										savings += 0;
									} else {
										savings += costs[sustainmentIndex] / numSites;
									}
								}
							}
							yearlySavings[outputYear] = savings;
							if(savingsData.containsKey(site)) {
								double[] currSavings = savingsData.get(site);
								for(int index = 0; index < currSavings.length; index++) {
									currSavings[index] += yearlySavings[index];
								}
								savingsData.put(site, currSavings);
							} else {
								savingsData.put(site, yearlySavings);
							}
						}
					}
				}
			}
		}
		
		list = new ArrayList<Object[]>();
		
		for(String site : savingsData.keySet()) {
			double[] values = savingsData.get(site);
			if(list.isEmpty()) {
				names = new String[values.length + 1];
				names[0] = "";
				//TODO: pass start FY
				int FY = 2017;
				for(int index = 0; index < values.length; index++) {
					if(index == 0) {
						names[0] = "Site";
					}
					String FYString = "FY" + FY;
					names[index+1] = FYString;
					FY++;				}
			}
			
			Object[] row = new Object[values.length + 1];
			row[0] = site;
			for(int index = 0; index < values.length; index++) {
				row[index + 1] = values[index];
			}
			list.add(row);
		}

	}

	public void runAllQueries() {
		this.engine1 = (IEngine) DIHelper.getInstance().getLocalProp(engineName1);
		this.engine2 = (IEngine) DIHelper.getInstance().getLocalProp(engineName2);
		
		if(query1Data.isEmpty()){
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine1, query1);
			String[] names1 = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
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
				Double [] sysSustCost;
				if(query1Data.containsKey(sysName)) {
					sysSustCost = query1Data.get(sysName);
					sysSustCost[position] = sustCost;
				} else {
					sysSustCost = new Double[5];
					sysSustCost[position] = sustCost;
					query1Data.put(sysName, sysSustCost);
				}
			}
		}
		
		if(query2Data.isEmpty()) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, query2);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(names[0]).toString();
				Double siteCount = (Double) sjss.getVar(names[1]);
				query2Data.put(sysName, siteCount);
			}
		}
		
		if(query3Data.isEmpty()) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, query3);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String regionName = sjss.getVar(names[0]).toString();
				String regionStartDate = sjss.getVar(names[1]).toString();
				query3Data.put(regionName, regionStartDate);
			}
		}
		
		if(query4Data.isEmpty()) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, waveProcedesQuery);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String wave1 = sjss.getVar(names[0]).toString();
				String wave2 = sjss.getVar(names[1]).toString();
				query4Data.put(wave1, wave2);
			}
		}
		
		if(query5Data.isEmpty()) {
			//Use Dual Engine Grid to process the dual query that gets cost info
			dualQueries.setQuery(query5);
			dualQueries.createData();
			ArrayList<Object[]> deploymentInfo = dualQueries.getList();
			int i;
			int size = deploymentInfo.size();
			for(i = 0; i < size; i++) {
				Object[] info = deploymentInfo.get(i);
				// region is first index in Object[]
				// wave is second index in Object[]
				// site is third index in Object[]
				// system is fourth index in Object[]
				String region = info[0].toString();
				String wave = info[1].toString();
				String site = info[2].toString();
				String system = info[3].toString();
				if(query5Data.containsKey(region)) {
					HashMap<String, HashMap<String, ArrayList<String>>> regionInfo = query5Data.get(region);
					if(regionInfo.containsKey(wave)) {
						HashMap<String, ArrayList<String>> waveInfo = regionInfo.get(wave);
						if(waveInfo.containsKey(site)) {
							ArrayList<String> systemList = waveInfo.get(site);
							systemList.add(system);
						} else {
							// put site info
							ArrayList<String> systemList = new ArrayList<String>();
							systemList.add(system);
							waveInfo.put(site, systemList);
						}
					} else {
						// put from wave info
						ArrayList<String> systemList = new ArrayList<String>();
						systemList.add(system);
						HashMap<String, ArrayList<String>> newWaveInfo = new HashMap<String, ArrayList<String>>();
						newWaveInfo.put(site, systemList);
						regionInfo.put(wave, newWaveInfo);
					}
				} else {
					System.out.println(region);
					// put from region info
					ArrayList<String> systemList = new ArrayList<String>();
					systemList.add(system);
					HashMap<String, ArrayList<String>> newWaveInfo = new HashMap<String, ArrayList<String>>();
					newWaveInfo.put(site, systemList);
					HashMap<String, HashMap<String, ArrayList<String>>> newRegionInfo = new HashMap<String, HashMap<String, ArrayList<String>>>();
					newRegionInfo.put(wave, newWaveInfo);
					query5Data.put(region, newRegionInfo);
				}
			}
		}
		
		//get first region and wave
		if(firstRegion == null) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, firstRegionQuery);
			String[] names = sjsw.getVariables();
			firstRegion = sjsw.next().getVar(names[0]).toString();
			
		}
		if(firstWave == null) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, firstWaveQuery);
			String[] names = sjsw.getVariables();
			firstWave = sjsw.next().getVar(names[0]).toString();
		}
		
		if(regionOrder == null) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, regionProcedesQuery);
			String[] names = sjsw.getVariables();
			HashMap<String, String> regionHash = new HashMap<String, String>();
			int counter = 0;
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String region1 = sjss.getVar(names[0]).toString();
				String region2 = sjss.getVar(names[1]).toString();
				regionHash.put(region1, region2);
				counter++;
			}
			String nextRegion = firstRegion;
			regionOrder = new ArrayList<String>();
			regionOrder.add(nextRegion);
			for(int i = 0; i < counter; i++) {
				nextRegion = regionHash.get(nextRegion);
				regionOrder.add(nextRegion);
			}
		}
		
		if(waveOrder == null) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, waveProcedesQuery);
			String[] names = sjsw.getVariables();
			HashMap<String, String> regionHash = new HashMap<String, String>();
			int counter = 0;
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String region1 = sjss.getVar(names[0]).toString();
				String region2 = sjss.getVar(names[1]).toString();
				regionHash.put(region1, region2);
				counter++;
			}
			String nextWave = firstWave;
			waveOrder = new ArrayList<String>();
			waveOrder.add(nextWave);
			for(int i = 0; i < counter; i++) {
				nextWave = regionHash.get(nextWave);
				waveOrder.add(nextWave);
			}
		}
		
	}
}

