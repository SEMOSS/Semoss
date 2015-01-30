/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DHMSMIntegrationSavingsPerFiscalYearProcessor {

	private static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationSavingsPerFiscalYearProcessor.class.getName());

	private final String masterQuery = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Wave ?HostSiteAndFloater ?System WHERE { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSiteAndFloater} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSiteAndFloater} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } UNION { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?Wave} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?System} } } &SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	private String masterQueryForSingleSystem = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Wave ?HostSiteAndFloater ?System WHERE { BIND(@SYSTEM@ AS ?System) {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSiteAndFloater} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSiteAndFloater} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } UNION { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?Wave} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?System} } } &SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	private String masterQueryForListOfSystems = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Wave ?HostSiteAndFloater ?System WHERE { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSiteAndFloater} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSiteAndFloater} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } UNION { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?Wave} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?System} } } BINDINGS ?System {@BINDINGS@} &SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	
	private final String TAP_PORTFOLIO = "TAP_Portfolio";
	private final String TAP_SITE = "TAP_Site_Data";
	private final String HR_CORE = "HR_Core";
	private IEngine tapPortfolio;
	private IEngine tapSite;
	private IEngine hrCore;

	private DualEngineGridPlaySheet dualQueries = new DualEngineGridPlaySheet();	

	private HashMap<String, Double[]> sysSustainmentInfoHash;
	private HashMap<String, Integer> numSitesForSysHash;
	private HashMap<String, Integer> numSitesNotInWaveForSysHash;
	private Set<String> sysList = new HashSet<String>();
	private Set<String> systemsToAddList = new HashSet<String>();
	private HashMap<String, HashMap<String, Double>> sysSiteSupportAndFloaterCostHash;
	private HashMap<String, String[]> waveStartEndDate;
	private HashMap<String, String> lastWaveForSitesAndFloatersInMultipleWavesHash;
	private HashMap<String, String> lastWaveForEachSystem;
	private HashMap<String, HashMap<String, ArrayList<String>>> masterHash;
	private HashMap<String, double[]> savingsData = new HashMap<String, double[]>();
	
	
	private HashMap<String, HashMap<Integer, Boolean>> missingDataMap = new HashMap<String, HashMap<Integer, Boolean>>();
	private Set<String> centrallyLocatedSys = new HashSet<String>();
	private HashMap<String, Double> locallyDeployedSavingsHash = new HashMap<String, Double>();
	private final double percentRealized = .18;
	
	private ArrayList<Object[]> list;
	private String[] names;

	public ArrayList<Object[]> getList() {
		return list;
	}

	public String[] getNames() {
		return names;
	}

	public void processData() {
		Integer minYear = 3000; // arbitrarily large year
		Integer maxYear = 0;
		for(String wave : waveStartEndDate.keySet()) {
			String[] startDate = waveStartEndDate.get(wave);
			String startTime[] = startDate[0].split("FY");
			String endTime[] = startDate[1].split("FY");
			String startYear = startTime[1];
			String endYear = endTime[1];
			int startYearAsNum = Integer.parseInt(startYear);
			int endYearAsNum = Integer.parseInt(endYear);
			if(endYearAsNum > maxYear) {
				maxYear = endYearAsNum;
			} else if(startYearAsNum < minYear) {
				minYear = startYearAsNum;
			}
		}
		int numColumns = maxYear - minYear + 2; // costs gains are typically realized a year after, except for centrally distributed systems
		double[] inflationArr = new double[numColumns+1];
		int i;
		for(i = 0; i < numColumns+1; i++) {
			if(i <= 1) {
				inflationArr[i] = 1;
			} else {
				// only add inflation for years we don't have O&M budget info for
				inflationArr[i] = Math.pow(1.03, i-1);
			}
		}
		HashMap<String, Double> sysSavings = new HashMap<String, Double>();
		
		boolean[] missingDataYear = new boolean[numColumns];
		for(String wave : waveStartEndDate.keySet()) {
			String[] startDate = waveStartEndDate.get(wave);
			String endTime[] = startDate[1].split("FY");
			String endYear = endTime[1];

			int sustainmentIndex = 0;
			switch(endYear) {
				case "2017" : sustainmentIndex = 2; break;
				case "2018" : sustainmentIndex = 3; break;
				default : sustainmentIndex = 4;
			}

			int outputYear = Integer.parseInt(endYear) - minYear + 1;

			HashMap<String, ArrayList<String>> sites = masterHash.get(wave);
			if(sites != null) {
				for(String site : sites.keySet()) {//calculate per site
					boolean addSite = false;
					if(!lastWaveForSitesAndFloatersInMultipleWavesHash.containsKey(site)) {
						addSite = true;
					} else {
						String lastWave = lastWaveForSitesAndFloatersInMultipleWavesHash.get(site); 
						if(lastWave.equals(wave)) {
							addSite = true;
						} else {
							addSite = false;
						}
					}
					if(addSite) {
						ArrayList<String> systems = sites.get(site);
						systemsToAddList.removeAll(systems);
						sysList.addAll(systems);
						double[] yearlySavings = new double[numColumns];
						int counter = 0;
						for(String system : systems) {
							boolean takePercentage = true;
							// dataMissing is a flag that is used to determine if a system at a site that should have savings has $0 in savings
							// this is denoted in the report by an asterisk next to that figure
							// missingData year gets updated to put that asterisk in the right spot
							boolean dataMissing = false;
							for(int index = outputYear; index < numColumns; index++) {
								double savings = 0.0;
								if(sysSiteSupportAndFloaterCostHash.containsKey(system)) {
									//local deployment calculated by subtracting budget from curr system savings
									takePercentage = false;
									
									// if we have cost information at the site lvl
									HashMap<String, Double> siteSupportCostForSystem = sysSiteSupportAndFloaterCostHash.get(system);
									if(siteSupportCostForSystem.containsKey(site)) {
										savings += siteSupportCostForSystem.get(site);										
									} else {
										savings += 0;
										dataMissing = true;
										missingDataYear[index] = true;
									}
									// store amount saved each individual time a system is decommissioned
									if(index == numColumns - 1) {
										if(sysSavings.containsKey(system)) {
											double currSiteSavings = sysSavings.get(system);
											currSiteSavings += savings * inflationArr[index+1];
											sysSavings.put(system, currSiteSavings);
										} else {
											sysSavings.put(system, savings *inflationArr[index+1]);
										}
									}
								} else {
									// if we do not have cost information at the site lvl
									Double[] costs = sysSustainmentInfoHash.get(system);
									// assume cost for a specific site is total cost / num sites
									int numSites = numSitesForSysHash.get(system);
									if(costs != null){
										if(costs[sustainmentIndex + counter] == null || costs[sustainmentIndex + counter] == 0){
											dataMissing = true;
											missingDataYear[index] = true;
										} else {
											savings += costs[sustainmentIndex + counter] / numSites;
										}
									}
								}
								
								if(sustainmentIndex+counter < 4) {
									counter++;
								}
								if(dataMissing){
									HashMap<Integer, Boolean> innerMap;
									if(missingDataMap.containsKey(site)) {
										innerMap = missingDataMap.get(site);
										innerMap.put(index,  dataMissing);
									} else {
										innerMap = new HashMap<Integer, Boolean>();
										missingDataMap.put(site, innerMap);
										innerMap.put(index, dataMissing);
									}
								}
								// Determine if a system is Centrally Located
								// The logic to determine if a system is centrally located is performed earleir and stored in a relationship, which we access directly
								// Centrally located systems only realize their savings after TOC
								if(centrallyLocatedSys.contains(system)) {
									yearlySavings[yearlySavings.length - 1] += savings * inflationArr[yearlySavings.length - index];
									break;
								} else {
									double inflatedSavings = savings * inflationArr[index+1];
									if(takePercentage) {
										double realizedSavings = inflatedSavings * percentRealized;
										
										yearlySavings[index] += realizedSavings;
										
										if(index == outputYear) {
											if(locallyDeployedSavingsHash.containsKey(system)) {
												double currentDelayedSavings = locallyDeployedSavingsHash.get(system);
												currentDelayedSavings += inflatedSavings * (1 - percentRealized) * inflationArr[yearlySavings.length - index];
												locallyDeployedSavingsHash.put(system, currentDelayedSavings);
											} else {
												locallyDeployedSavingsHash.put(system, inflatedSavings * (1 - percentRealized) * inflationArr[yearlySavings.length - index]);
											}
										}
									} else {
										yearlySavings[index] += inflatedSavings;
									}
								}
							}
						}
	
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
		
		list = new ArrayList<Object[]>();
		int numCols = numColumns+2;
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
		double[] totalCol = new double[numCols-2];
		
		// create system column
		if(list.isEmpty()) {
			names = new String[numCols];
			int fy = minYear; // pass min year to start table
			int index;
			for(index = 0; index < numCols - 1; index++) {
				if(index == 0) {
					names[0] = "HostSite/Floater";
				}
				String fyString = "" + fy;
				fyString = "FY" + fyString.substring(2,4);
				names[index+1] = fyString;
				fy++;				
			}
			names[index] = "Total";
		}
		// calculate site data
		for(String site : savingsData.keySet()) {
			double[] values = savingsData.get(site);
			Object[] row = new Object[numCols];
			double totalRow = 0;
			row[0] = site;
			int index;
			boolean missing = false;
			for(index = 0; index < numCols - 2; index++) {
				double value = values[index];
				if(missingDataMap.containsKey(site)) {
					missing = true;
					HashMap<Integer, Boolean> innerMap = missingDataMap.get(site);
					if(innerMap.containsKey(index)){
						if(value == 0) {
							row[index + 1] = "No Cost Data";
						} else {
							totalRow += value;
							totalCol[index] += value;
							row[index + 1] = formatter.format(value) + "*"; // dataMissing flag for any system for the year and site in question
						}
					} else {
						totalRow += value;
						totalCol[index] += value;
						row[index + 1] = formatter.format(value);
					}
				} else {
					totalRow += value;
					totalCol[index] += value;
					row[index + 1] = formatter.format(value);
				}
			}
			row[index+1] = formatter.format(totalRow);
			if(missing) {
				row[index+1] += "*";
			}
			list.add(row);
		}
		
		// add in information for systems in deployment strategy but have waves not included
		double otherSiteCost = 0;
		Object[] otherSiteRow = new Object[numCols];
		otherSiteRow[0] = "Other Sites Not In Waves";
		for(i = 1; i < numCols - 2; i++) {
			otherSiteRow[i] = formatter.format(0);
		}
		for(String system : numSitesNotInWaveForSysHash.keySet()) {
			if(sysList.contains(system)) {
				Double[] costs = sysSustainmentInfoHash.get(system);
				if(costs != null) {
					int numSites = numSitesForSysHash.get(system);
					int numSitesNotIncluded = numSitesNotInWaveForSysHash.get(system);
					otherSiteCost += costs[costs.length - 1] * numSitesNotIncluded/numSites  * inflationArr[inflationArr.length - 1];
				}
			}
		}
		otherSiteRow[numCols - 2] = formatter.format(otherSiteCost * percentRealized);
		otherSiteRow[numCols - 1] = formatter.format(otherSiteCost * percentRealized);

		
		double systemsNotIncludedCost = 0;
		Object[] systemsNotIncludedRow = new Object[numCols];
		systemsNotIncludedRow[0] = "Systems Not At Host Sites";
		for(i = 1; i < numCols - 2; i++) {
			systemsNotIncludedRow[i] = formatter.format(0);
		}
		for(String system : systemsToAddList) {
			Double[] costs = sysSustainmentInfoHash.get(system);
			if(costs != null) {
				systemsNotIncludedCost += costs[costs.length - 1] * inflationArr[inflationArr.length - costs.length];
			}
		}
		systemsNotIncludedRow[numCols - 2] = formatter.format(systemsNotIncludedCost * percentRealized);
		systemsNotIncludedRow[numCols - 1] = formatter.format(systemsNotIncludedCost * percentRealized);

		//add fixed cost and column totals
		Object[] row = new Object[numCols];
		row[0] = "Total";
		double combinedTotal = 0;
		int index;
		for(index = 0; index < numCols - 2; index++) {
			if(index == numCols - 3) {
				totalCol[index] += otherSiteCost * percentRealized;
				totalCol[index] += systemsNotIncludedCost * percentRealized;
			}
			combinedTotal += totalCol[index];
		}
		
		// need to determine when last wave for each system we have information for was decommissioned
		Object[] sustainmentRow = new Object[numCols];
		double[] yearlySavings = new double[numCols - 2];
		for(String system : sysSiteSupportAndFloaterCostHash.keySet()) {
			Double currSiteSavings = sysSavings.get(system);
			if(currSiteSavings != null) {
				String wave = lastWaveForEachSystem.get(system);
				String[] startDate = waveStartEndDate.get(wave);
				String endTime[] = startDate[1].split("FY");
				String endYear = endTime[1];
				int outputYear = Integer.parseInt(endYear) - minYear + 1;
				
				// find last non-null cost information
				Double[] costArr = sysSustainmentInfoHash.get(system);
				int position = costArr.length - 1;
				boolean loop = true;
				while(loop) {
					if(costArr[position] == null) {
						position = position - 1;
					} else {
						loop = false;
					}
				}
				double savings = costArr[position]; 
				
				for(index = outputYear; index < numCols - 2; index++) {
					double inflatedSavings = savings * inflationArr[index-position+1];
					yearlySavings[index] += inflatedSavings - currSiteSavings;
					if(index == yearlySavings.length - 1) {
						yearlySavings[index] -= otherSiteCost * percentRealized;
						yearlySavings[index] -= systemsNotIncludedCost * percentRealized;
					}
				}
			}
		}
		
		sustainmentRow[0] = "Fixed_Sustainment_Cost";
		double totalSustainment = 0;
		boolean totalDataMissing = false;
		boolean totalSustainmentMissing = false;
		for(index = 1; index < numCols - 1; index++) {
			double fixedAmount = yearlySavings[index-1];
			if(index == numCols - 2) {
				// add in fixed sustainment cost for each locally deployed system at FOC
				for(String sys : locallyDeployedSavingsHash.keySet()) {
					fixedAmount += locallyDeployedSavingsHash.get(sys);
				}
			}
			
			sustainmentRow[index] = formatter.format(fixedAmount);
			row[index] = formatter.format(totalCol[index-1] + fixedAmount);
			if(missingDataYear[index-1]) {
				row[index] += "*";
				totalDataMissing = true;
			}
			if(index == numCols - 2 && missingDataYear[index-1]) {
				sustainmentRow[index] += "*";
				totalSustainmentMissing = true;
			}
			totalSustainment += fixedAmount;
		}
		
		sustainmentRow[numCols - 1] = formatter.format(totalSustainment);
		if(totalSustainmentMissing) {
			sustainmentRow[numCols - 1] += "*";
		}
		
		row[numCols - 1] = formatter.format(combinedTotal + totalSustainment);
		if(totalDataMissing) {
			row[numCols - 1] += "*";
		}
		
		list.add(sustainmentRow);
		list.add(otherSiteRow);
		list.add(systemsNotIncludedRow);
		list.add(row);
	}

	public void runSupportQueries() {
		this.tapPortfolio = (IEngine) DIHelper.getInstance().getLocalProp(TAP_PORTFOLIO);
		this.tapSite = (IEngine) DIHelper.getInstance().getLocalProp(TAP_SITE);
		this.hrCore = (IEngine) DIHelper.getInstance().getLocalProp(HR_CORE);

		sysSustainmentInfoHash = DHMSMDeploymentHelper.getSysSustainmentBudget(tapPortfolio);
		
		HashMap<String, HashMap<String, Double>> sysSiteSupportCostHash = DHMSMDeploymentHelper.getSysSiteSupportCost(tapPortfolio);
		HashMap<String, HashMap<String, Double>> sysFloaterCostHash = DHMSMDeploymentHelper.getSysFloaterCost(tapPortfolio);
		sysSiteSupportAndFloaterCostHash = addAllCostInfo(sysSiteSupportCostHash, sysFloaterCostHash);
		
		numSitesForSysHash = DHMSMDeploymentHelper.getNumSitesSysDeployedAt(tapSite);
		
		ArrayList<String> waveOrder = DHMSMDeploymentHelper.getWaveOrder(tapSite);
		HashMap<String, List<String>> sitesInMultipleWavesHash = DHMSMDeploymentHelper.getSitesAndMultipleWaves(tapSite);
		lastWaveForSitesAndFloatersInMultipleWavesHash = DHMSMDeploymentHelper.determineLastWaveForInput(waveOrder, sitesInMultipleWavesHash);
		
		HashMap<String, List<String>> floaterWaveList = DHMSMDeploymentHelper.getFloatersAndWaves(tapSite);
		lastWaveForSitesAndFloatersInMultipleWavesHash.putAll(DHMSMDeploymentHelper.determineLastWaveForInput(waveOrder, floaterWaveList));
		
		waveStartEndDate = DHMSMDeploymentHelper.getWaveStartAndEndDate(tapSite);
		
		lastWaveForEachSystem = DHMSMDeploymentHelper.getLastWaveForEachSystem(tapSite, waveOrder);
		
		centrallyLocatedSys = DHMSMDeploymentHelper.getCentrallyDeployedSystems(hrCore);
		
		numSitesNotInWaveForSysHash = new HashMap<String, Integer>();
		DHMSMDeploymentGapAnalysis gap = new DHMSMDeploymentGapAnalysis();
		gap.createData();
		ArrayList<Object[]> gapList = gap.getList();
		int size = gapList.size();
		int i = 0;
		for(; i < size; i++) {
			Object[] row = gapList.get(i);
			String sys = row[1].toString();
			if(numSitesNotInWaveForSysHash.containsKey(sys)) {
				int count = numSitesNotInWaveForSysHash.get(sys);
				numSitesNotInWaveForSysHash.put(sys, count+1);
			} else {
				numSitesNotInWaveForSysHash.put(sys, 1);
			}
		}
	}
	
	//This method does the same thing as the above Process Data, but swaps the systems for the sites, getting a total per-site
	public void processSystemData(){
		Integer minYear = 3000; // arbitrarily large year
		Integer maxYear = 0;
		for(String wave : waveStartEndDate.keySet()) {
			String[] startDate = waveStartEndDate.get(wave);
			String startTime[] = startDate[0].split("FY");
			String endTime[] = startDate[1].split("FY");
			String startYear = startTime[1];
			String endYear = endTime[1];
			int startYearAsNum = Integer.parseInt(startYear);
			int endYearAsNum = Integer.parseInt(endYear);
			if(endYearAsNum > maxYear) {
				maxYear = endYearAsNum;
			} else if(startYearAsNum < minYear) {
				minYear = startYearAsNum;
			}
		}
		int numColumns = maxYear - minYear + 2; // costs gains are realized a year after
		double[] inflationArr = new double[numColumns+1];
		int i;
		for(i = 0; i < numColumns+1; i++) {
			if(i <= 1) {
				inflationArr[i] = 1;
			} else {
				// only add inflation for years we don't have O&M budget info for
				inflationArr[i] = Math.pow(1.03, i-1);
			}
		}
		HashMap<String, Double> sysSavings = new HashMap<String, Double>();
		
		boolean[] missingDataYear = new boolean[numColumns];
		for(String wave : waveStartEndDate.keySet()) {
			String[] startDate = waveStartEndDate.get(wave);
			String endTime[] = startDate[1].split("FY");
			String endYear = endTime[1];

			int sustainmentIndex = 0;
			switch(endYear) {
				case "2017" : sustainmentIndex = 2; break;
				case "2018" : sustainmentIndex = 3; break;
				default : sustainmentIndex = 4;
			}

			int outputYear = Integer.parseInt(endYear) - minYear + 1;

			HashMap<String, ArrayList<String>> sites = masterHash.get(wave);
			if(sites != null) {
				for(String site : sites.keySet()) {
					boolean addSite = false;
					if(!lastWaveForSitesAndFloatersInMultipleWavesHash.containsKey(site)) {
						addSite = true;
					} else {
						String lastWave = lastWaveForSitesAndFloatersInMultipleWavesHash.get(site); 
						if(lastWave.equals(wave)) {
							addSite = true;
						} else {
							addSite = false;
						}
					}
					if(addSite) {
						ArrayList<String> systems = sites.get(site);
						systemsToAddList.removeAll(systems);
						sysList.addAll(systems);
						int counter = 0;
						for(String system : systems) {
							double[] yearlySavings = new double[numColumns];
							boolean dataMissing = false;
							boolean takePercentage = true;
							for(int index = outputYear; index < numColumns; index++) {
								double savings = 0.0;
								if(sysSiteSupportAndFloaterCostHash.containsKey(system)) {
									//local deployment calculated by subtracting budget from curr system savings
									takePercentage = false;
									
									// if we have cost information at the site lvl
									HashMap<String, Double> siteSupportCostForSystem = sysSiteSupportAndFloaterCostHash.get(system);
									if(siteSupportCostForSystem.containsKey(site)) {
										savings += siteSupportCostForSystem.get(site);										
									} else {
										savings += 0;
										dataMissing = true;
										missingDataYear[index] = true;
									}
									// store amount saved each individual time a system is decommissioned
									if(index == numColumns - 1) {
										if(sysSavings.containsKey(system)) {
											double currSiteSavings = sysSavings.get(system);
											currSiteSavings += savings * inflationArr[index+1];
											sysSavings.put(system, currSiteSavings);
										} else {
											sysSavings.put(system, savings * inflationArr[index+1]);
										}
									}
								} else {
									// if we do not have cost information at the site lvl
									Double[] costs = sysSustainmentInfoHash.get(system);
									// assume cost for a specific site is total cost / num sites
									double numSites = numSitesForSysHash.get(system);
									if(costs != null){
										if(costs[sustainmentIndex + counter] == null || costs[sustainmentIndex + counter] == 0){
											dataMissing = true;
											missingDataYear[index] = true;
										} else {
											savings += costs[sustainmentIndex + counter] / numSites;
										}
									}
								}
								
								if(sustainmentIndex+counter < 4) {
									counter++;
								}
								if(dataMissing){
									HashMap<Integer, Boolean> innerMap;
									if(missingDataMap.containsKey(system)) {
										innerMap = missingDataMap.get(system);
										innerMap.put(index,  dataMissing);
									} else {
										innerMap = new HashMap<Integer, Boolean>();
										missingDataMap.put(system, innerMap);
										innerMap.put(index, dataMissing);
									}
								}
								if(centrallyLocatedSys.contains(system)) {
									yearlySavings[yearlySavings.length - 1] += savings * inflationArr[yearlySavings.length - index];
									break;
								} else {
									double inflatedSavings = savings * inflationArr[index+1];
									if(takePercentage) {
										double realizedSavings = inflatedSavings * percentRealized;
										
										yearlySavings[index] += realizedSavings;
										
										if(index == outputYear) {
											if(locallyDeployedSavingsHash.containsKey(system)) {
												double currentDelayedSavings = locallyDeployedSavingsHash.get(system);
												currentDelayedSavings += inflatedSavings * (1 - percentRealized) * inflationArr[yearlySavings.length - index];
												locallyDeployedSavingsHash.put(system, currentDelayedSavings);
											} else {
												locallyDeployedSavingsHash.put(system, inflatedSavings * (1 - percentRealized) * inflationArr[yearlySavings.length - index]);
											}
										}
									} else {
										yearlySavings[index] += inflatedSavings;
									}
								}
							}
							
							if(savingsData.containsKey(system)) {
								double[] currSavings = savingsData.get(system);
								for(int index = 0; index < currSavings.length; index++) {
									currSavings[index] += yearlySavings[index];
								}
								savingsData.put(system, currSavings);
							} else {
								savingsData.put(system, yearlySavings);
							}
						}
					}
				}
			}
			
		}
		
		list = new ArrayList<Object[]>();
		int numCols = numColumns+2;
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
		double[] totalCol = new double[numCols-2];
		
		if(list.isEmpty()) {
			names = new String[numCols];
			int fy = minYear; // pass min year to start table
			int index;
			for(index = 0; index < numCols - 1; index++) {
				if(index == 0) {
					names[0] = "System";
				}
				String fyString = "" + fy;
				fyString = "FY" + fyString.substring(2,4);
				names[index+1] = fyString;
				fy++;				
			}
			names[index] = "Total";
		}
		
		for(String system : savingsData.keySet()) {
			double[] values = savingsData.get(system);
			// add in information for systems in deployment strategy but have waves not included
//			double otherSiteCost = 0;
//			if(numSitesNotInWaveForSysHash.containsKey(system)) {
//				if(sysList.contains(system)) {
//					Double[] costs = sysSustainmentInfoHash.get(system);
//					if(costs != null) {
//						int numSites = numSitesForSysHash.get(system);
//						int numSitesNotIncluded = numSitesNotInWaveForSysHash.get(system);
//						otherSiteCost += costs[costs.length - 1] * numSitesNotIncluded/numSites  * inflationArr[inflationArr.length - 1];
//					}
//				}
//			}

			Object[] row = new Object[numCols];
			double totalRow = 0;
			row[0] = system;
			int index;
			boolean missing = false;
			for(index = 0; index < numCols - 2; index++) {
				double value = values[index];
				if(index == numCols - 3) {
					Double fixedCost = locallyDeployedSavingsHash.get(system);
					if(fixedCost != null) {
						value += fixedCost;
					} else {
						Double[] costArr = sysSustainmentInfoHash.get(system);
						Double currSiteSavings = sysSavings.get(system);
						
						if(currSiteSavings != null) {
							int position = costArr.length - 1;
							boolean loop = true;
							while(loop) {
								if(costArr[position] == null) {
									position = position - 1;
								} else {
									loop = false;
								}
							}
							double savings = costArr[position]; 
							double inflatedSavings = savings * inflationArr[index-position+1];
	
							value += inflatedSavings - currSiteSavings;
						}
					}
					
					//include values not listed
//					value += otherSiteCost;
				}
				if(missingDataMap.containsKey(system)){
					missing = true;
					HashMap<Integer, Boolean> innerMap = missingDataMap.get(system);
					if(innerMap.containsKey(index)){
						if(value == 0) {
							row[index + 1] = "No Cost Data";
						} else {
							totalRow += value;
							totalCol[index] += value;
							row[index + 1] = formatter.format(value) + "*";
						}
					} else {
						totalCol[index] += value;
						totalRow += value;
						row[index + 1] = formatter.format(value);
					}
				} else {
					totalRow+=value;
					totalCol[index] += value;
					row[index + 1] = formatter.format(value);
				}

			}
			row[index+1] = formatter.format(totalRow);
			if(missing) {
				row[index+1] += "*";
			}
			list.add(row);
		}
		
		// add in systems not in deployment strategy to be decommissioned at FOC
		for(String system : systemsToAddList) {
			Object[] row = new Object[numCols];
			row[0] = system;
			for(i = 1; i < numCols - 2; i++) {
				row[i] = formatter.format(0);
			}
			Double[] costArr = sysSustainmentInfoHash.get(system);
			double inflatedSavings = 0;
			if(costArr != null) {
				inflatedSavings = costArr[costArr.length - 1] * inflationArr[inflationArr.length - costArr.length];
			}
			if(inflatedSavings == 0) {
				row[row.length - 2] = "No Cost Information";
				row[row.length - 1] = formatter.format(0) + "*";
			} else {
				row[row.length - 2] = formatter.format(inflatedSavings); 
				row[row.length - 1] = formatter.format(inflatedSavings); 
				totalCol[totalCol.length - 1] += inflatedSavings;
			}
			list.add(row);
		}
		
		//add fixed cost and column totals
		Object[] row = new Object[numCols];
		row[0] = "Total";
		double combinedTotal = 0;
		int index;
		for(index = 0; index < numCols - 2; index++) {
			combinedTotal += totalCol[index];
		}
		
		boolean totalDataMissing = false;
		for(index = 1; index < numCols - 1; index++) {
			row[index] = formatter.format(totalCol[index-1]);
			if(missingDataYear[index-1]) {
				row[index] += "*";
				totalDataMissing = true;
			}
		}
		
		row[numCols - 1] = formatter.format(combinedTotal);
		if(totalDataMissing) {
			row[numCols - 1] += "*";
		}

		list.add(row);
	}

	private HashMap<String, HashMap<String, Double>> addAllCostInfo(HashMap<String, HashMap<String, Double>> sysSiteSupportCostHash, HashMap<String, HashMap<String, Double>> sysFloaterCostHash) {
		HashMap<String, HashMap<String, Double>> retHash = new HashMap<String, HashMap<String, Double>>();
		retHash.putAll(sysSiteSupportCostHash);
		
		for(String key : sysFloaterCostHash.keySet()) {
			if(retHash.containsKey(key)) {
				HashMap<String, Double> innerHash = retHash.get(key);
				innerHash.putAll(sysFloaterCostHash.get(key));
			} else {
				retHash.put(key, sysFloaterCostHash.get(key));
			}
		}
		
		return retHash;
	}

	public void runMainQuery(String sysURI) {
		String query = masterQuery;
		if(!sysURI.isEmpty()) {
			query = masterQueryForSingleSystem.replace("@SYSTEM@", sysURI);
			systemsToAddList.add(Utility.getInstanceName(sysURI.replace("<", "").replace(">", "")));
		} else {
			systemsToAddList = DHMSMDeploymentHelper.getHPSysList(hrCore);
		}

		processQuery(query);
	}
	
	public void runMainQueryFromWorksheetList(ArrayList<String> sysNamesList) {
		String bindingsStr = "";
		for(String sysName : sysNamesList) {
			sysName = Utility.cleanString(sysName, false);
			bindingsStr += "(<http://health.mil/ontologies/Concept/System/";
			bindingsStr += sysName;
			bindingsStr += ">)";
			
			systemsToAddList.add(sysName);
		}
		String query = masterQueryForListOfSystems.replace("@BINDINGS@", bindingsStr);
		
		processQuery(query);
	}
	
	private void processQuery(String query) {
		LOGGER.info(query);

		if(masterHash == null) {
			masterHash = new HashMap<String, HashMap<String, ArrayList<String>>>();
			//Use Dual Engine Grid to process the dual query that gets cost info
			dualQueries.setQuery(query);
			dualQueries.createData();
			ArrayList<Object[]> deploymentInfo = dualQueries.getList();
			int i;
			int size = deploymentInfo.size();
			for(i = 0; i < size; i++) {
				Object[] info = deploymentInfo.get(i);
				// wave is first index in Object[]
				// site is second index in Object[]
				// system is third index in Object[]
				String wave = info[0].toString();
				String site = info[1].toString();
				String system = info[2].toString();
				if(masterHash.containsKey(wave)) {
					HashMap<String, ArrayList<String>> waveInfo = masterHash.get(wave);
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
					masterHash.put(wave, newWaveInfo);
				}
			} 
		}
	}

}