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

	private final String masterQuery = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Wave ?HostSiteAndFloater ?System WHERE { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSiteAndFloater} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSiteAndFloater} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } UNION { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?Wave} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?System} } } &SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} }BINDINGS ?Probability {('High') ('Question')}&false&false";
	private String masterQueryForSingleSystem = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Wave ?HostSiteAndFloater ?System WHERE { BIND(@SYSTEM@ AS ?System) {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSiteAndFloater} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSiteAndFloater} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } UNION { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?Wave} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?System} } } &SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	private String masterQueryForListOfSystems = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Wave ?HostSiteAndFloater ?System WHERE { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSiteAndFloater} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSiteAndFloater} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } UNION { {?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?Wave} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?System} } } BINDINGS ?System {@BINDINGS@} &SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	
	private final String TAP_PORTFOLIO = "TAP_Portfolio";
	private final String TAP_SITE = "TAP_Site_Data";
	private final String HR_CORE = "HR_Core";
	private IEngine tapPortfolio;
	private IEngine tapSite;
	private IEngine hrCore;

	private DualEngineGridPlaySheet dualQueries = new DualEngineGridPlaySheet();	

	private HashMap<String, Double[]> sysSustainmentInfoHash;
	private HashMap<String, Integer> numSitesForSysHash;
	private HashMap<String, ArrayList<String>> systemsForSiteHash;
	private HashMap<String, HashMap<String, Double>> siteLocationHash; 
	private HashMap<String, Integer> numSitesNotInWaveForSysHash;
	private Set<String> sysList = new HashSet<String>();
	private Set<String> systemsToAddList = new HashSet<String>();
	private HashMap<String, HashMap<String, Double>> sysSiteSupportAndFloaterCostHash;
	private HashMap<String, String[]> waveStartEndDate;
	private HashMap<String, String> lastWaveForSitesAndFloatersInMultipleWavesHash;
	private HashMap<String, String> firstWaveForSitesAndFloatersInMultipleWavesHash;
	private HashMap<String, String> lastWaveForEachSystem;
	private HashMap<String, String> firstWaveForEachSystem;
	private HashMap<String, List<String>>  waveForSites;
	private HashMap<String, HashMap<String, ArrayList<String>>> masterHash;
	
	private Set<String> centrallyLocatedSys = new HashSet<String>();
	private HashMap<String, Double> locallyDeployedSavingsHash = new HashMap<String, Double>();
	private final double percentRealized = .18;
	
	private ArrayList<Object[]> systemOutputList;
	private String[] sysNames;
	private ArrayList<Object[]> siteOutputList;
	private String[] siteNames;
	
	private HashMap<String, double[]> savingsDataBySystem = new HashMap<String, double[]>();
	private HashMap<String, double[]> savingsDataBySite = new HashMap<String, double[]>();
	private HashMap<String, HashMap<Integer, Boolean>> missingDataMapBySystem = new HashMap<String, HashMap<Integer, Boolean>>();
	private HashMap<String, HashMap<Integer, Boolean>> missingDataMapBySite = new HashMap<String, HashMap<Integer, Boolean>>();
	private HashMap<String, Double> sysSavings = new HashMap<String, Double>();;
	
	int numColumns;
	int minYear;
	int maxYear;
	double[] inflationArr;
	boolean[] missingDataYear;
	
	public void generateSavingsData() {
		savingsDataBySystem = new HashMap<String, double[]>();
		savingsDataBySite = new HashMap<String, double[]>();
		missingDataMapBySystem = new HashMap<String, HashMap<Integer, Boolean>>();
		missingDataMapBySite = new HashMap<String, HashMap<Integer, Boolean>>();
		sysSavings = new HashMap<String, Double>();;
		
		minYear = 3000; // arbitrarily large year
		maxYear = 0;
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
		numColumns = maxYear - minYear + 2; // costs gains are typically realized a year after, except for centrally distributed systems
		if(numColumns < 4) {
			numColumns = 4;
		}

		inflationArr = new double[numColumns+1];
		
		int i;
		for(i = 0; i < numColumns+1; i++) {
			if(i <= 1) {
				inflationArr[i] = 1;
			} else {
				// only add inflation for years we don't have O&M budget info for
				inflationArr[i] = Math.pow(1.03, i-1);
			}
		}
		
		missingDataYear = new boolean[numColumns];
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
						double[] yearlySiteSavings = new double[numColumns];
						int counter = 0;
						for(String system : systems) {
							double[] yearlySystemSavings = new double[numColumns];
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
									if(missingDataMapBySite.containsKey(site)) {
										innerMap = missingDataMapBySite.get(site);
										innerMap.put(index,  dataMissing);
									} else {
										innerMap = new HashMap<Integer, Boolean>();
										missingDataMapBySite.put(site, innerMap);
										innerMap.put(index, dataMissing);
									}
								}
								if(dataMissing){
									HashMap<Integer, Boolean> innerMap;
									if(missingDataMapBySystem.containsKey(system)) {
										innerMap = missingDataMapBySystem.get(system);
										innerMap.put(index,  dataMissing);
									} else {
										innerMap = new HashMap<Integer, Boolean>();
										missingDataMapBySystem.put(system, innerMap);
										innerMap.put(index, dataMissing);
									}
								}
								// Determine if a system is Centrally Located
								// The logic to determine if a system is centrally located is performed earleir and stored in a relationship, which we access directly
								// Centrally located systems only realize their savings after TOC
								if(centrallyLocatedSys.contains(system)) {
									yearlySystemSavings[yearlySystemSavings.length - 1] += savings * inflationArr[yearlySystemSavings.length - index];
									yearlySiteSavings[yearlySiteSavings.length - 1] += savings * inflationArr[yearlySiteSavings.length - index];

									break;
								} else {
									double inflatedSavings = savings * inflationArr[index+1];
									if(takePercentage) {
										double realizedSavings = inflatedSavings * percentRealized;
										
										yearlySystemSavings[index] += realizedSavings;
										yearlySiteSavings[index] += realizedSavings;

										if(index == outputYear) {
											if(locallyDeployedSavingsHash.containsKey(system)) {
												double currentDelayedSavings = locallyDeployedSavingsHash.get(system);
												currentDelayedSavings += inflatedSavings * (1 - percentRealized) * inflationArr[yearlySystemSavings.length - index];
												locallyDeployedSavingsHash.put(system, currentDelayedSavings);
											} else {
												locallyDeployedSavingsHash.put(system, inflatedSavings * (1 - percentRealized) * inflationArr[yearlySystemSavings.length - index]);
											}
										}
									} else {
										yearlySystemSavings[index] += inflatedSavings;
										yearlySiteSavings[index] += inflatedSavings;
									}
								}
							}
							if(savingsDataBySystem.containsKey(system)) {
								double[] currSavings = savingsDataBySystem.get(system);
								for(int index = 0; index < currSavings.length; index++) {
									currSavings[index] += yearlySystemSavings[index];
								}
								savingsDataBySystem.put(system, currSavings);
							} else {
								savingsDataBySystem.put(system, yearlySystemSavings);
							}
						}
	
						if(savingsDataBySite.containsKey(site)) {
							double[] currSavings = savingsDataBySite.get(site);
							for(int index = 0; index < currSavings.length; index++) {
								currSavings[index] += yearlySiteSavings[index];
							}
							savingsDataBySite.put(site, currSavings);
						} else {
							savingsDataBySite.put(site, yearlySiteSavings);
						}
					}
				}
			}
		}
	}
	
	
	public void processSiteData() {
		siteOutputList = new ArrayList<Object[]>();
		int numCols = numColumns+2;
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
		double[] totalCol = new double[numCols-2];
		
		// create system column
		if(siteOutputList.isEmpty()) {
			siteNames = new String[numCols];
			int fy = minYear; // pass min year to start table
			int index;
			for(index = 0; index < numCols - 1; index++) {
				if(index == 0) {
					siteNames[0] = "HostSite-Floater";
				}
				String fyString = "" + fy;
				fyString = "FY" + fyString.substring(2,4);
				siteNames[index+1] = fyString;
				fy++;				
			}
			siteNames[index] = "Total";
		}
		// calculate site data
		for(String site : savingsDataBySite.keySet()) {
			double[] values = savingsDataBySite.get(site);
			Object[] row = new Object[numCols];
			double totalRow = 0;
			row[0] = site;
			int index;
			boolean missing = false;
			for(index = 0; index < numCols - 2; index++) {
				double value = values[index];
				if(missingDataMapBySite.containsKey(site)) {
					missing = true;
					HashMap<Integer, Boolean> innerMap = missingDataMapBySite.get(site);
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
			siteOutputList.add(row);
		}
		
		boolean missingData = false;
		// add in information for systems in deployment strategy but have waves not included
		double totalOtherSiteCost = 0;
		Object[] otherSiteRow = new Object[numCols];
		otherSiteRow[0] = "Other Sites Not In Waves";
		int i;
		for(i = 1; i < numCols - 2; i++) {
			otherSiteRow[i] = formatter.format(0);
		}
		
		// used to not double count the cost for sites/systems not included
		double otherSitesNotIncludedForSysSupportAndFloaterCost = 0;
		
		for(String system : numSitesNotInWaveForSysHash.keySet()) {
			if(sysList.contains(system)) {
				Double[] costs = sysSustainmentInfoHash.get(system);
				if(costs != null) {
					int numSites = numSitesForSysHash.get(system);
					int numSitesNotIncluded = numSitesNotInWaveForSysHash.get(system);
					double otherSiteCost = costs[costs.length - 1] * numSitesNotIncluded/numSites  * inflationArr[inflationArr.length - costs.length];
					if(sysSavings.containsKey(system)) {
						double currSiteSavings = sysSavings.get(system);
						currSiteSavings += otherSiteCost * percentRealized;
						sysSavings.put(system, currSiteSavings);
						otherSitesNotIncludedForSysSupportAndFloaterCost += otherSiteCost;
					} 
					totalOtherSiteCost += otherSiteCost;
				} else {
					missingData = true;
				}
			}
		}
		otherSiteRow[numCols - 2] = formatter.format(totalOtherSiteCost * percentRealized);
		otherSiteRow[numCols - 1] = formatter.format(totalOtherSiteCost * percentRealized);
		if(missingData) {
			otherSiteRow[numCols - 2] += "*";
			otherSiteRow[numCols - 1] += "*";
		}
		missingData = false;
		double totalSystemsNotIncludedCost = 0;
		Object[] systemsNotIncludedRow = new Object[numCols];
		systemsNotIncludedRow[0] = "Systems Not At Host Sites";
		for(i = 1; i < numCols - 2; i++) {
			systemsNotIncludedRow[i] = formatter.format(0);
		}
		for(String system : systemsToAddList) {
			Double[] costs = sysSustainmentInfoHash.get(system);
			if(costs != null && costs[costs.length-1] != 0) {
				double systemNotIncludedCost = costs[costs.length - 1] * inflationArr[inflationArr.length - costs.length];
				totalSystemsNotIncludedCost += systemNotIncludedCost;
			} else {
				missingData = true;
			}
		}
		systemsNotIncludedRow[numCols - 2] = formatter.format(totalSystemsNotIncludedCost * percentRealized);
		systemsNotIncludedRow[numCols - 1] = formatter.format(totalSystemsNotIncludedCost * percentRealized);
		if(missingData) {
			systemsNotIncludedRow[numCols - 2] += "*";
			systemsNotIncludedRow[numCols - 1] += "*";
		}
		//add fixed cost and column totals
		Object[] row = new Object[numCols];
		row[0] = "Total";
		double combinedTotal = 0;
		int index;
		for(index = 0; index < numCols - 2; index++) {
			if(index == numCols - 3) {
				totalCol[index] += totalOtherSiteCost * percentRealized;
				totalCol[index] += totalSystemsNotIncludedCost * percentRealized;
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
					double inflatedSavings = savings;
					// add inflation to savings if appropriate
					if(index - position + 1 > 1) {
						inflatedSavings *= inflationArr[index-position+1];
					}
					yearlySavings[index] += inflatedSavings - currSiteSavings;
				}
			}
		}
		// subtract the other sites not included cost
		yearlySavings[yearlySavings.length - 1] -= otherSitesNotIncludedForSysSupportAndFloaterCost * (1 - percentRealized);
		
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
				fixedAmount += totalOtherSiteCost * (1-percentRealized);
				fixedAmount += totalSystemsNotIncludedCost * (1-percentRealized);
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
		
		siteOutputList.add(sustainmentRow);
		siteOutputList.add(otherSiteRow);
		siteOutputList.add(systemsNotIncludedRow);
		siteOutputList.add(row);
	}
	
	//This method does the same thing as the above Process Data, but swaps the systems for the sites, getting a total per-site
	public void processSystemData(){
		systemOutputList = new ArrayList<Object[]>();
		int numCols = numColumns+2;
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
		double[] totalCol = new double[numCols-2];
		
		if(systemOutputList.isEmpty()) {
			sysNames = new String[numCols];
			int fy = minYear; // pass min year to start table
			int index;
			for(index = 0; index < numCols - 1; index++) {
				if(index == 0) {
					sysNames[0] = "System";
				}
				String fyString = "" + fy;
				fyString = "FY" + fyString.substring(2,4);
				sysNames[index+1] = fyString;
				fy++;				
			}
			sysNames[index] = "Total";
		}
		
		for(String system : savingsDataBySystem.keySet()) {
			double[] values = savingsDataBySystem.get(system);
			// add in information for systems in deployment strategy but have waves not included
			if(numSitesNotInWaveForSysHash.containsKey(system)) {
				if(sysList.contains(system)) {
					Double[] costs = sysSustainmentInfoHash.get(system);
					if(costs != null) {
						int numSites = numSitesForSysHash.get(system);
						int numSitesNotIncluded = numSitesNotInWaveForSysHash.get(system);
						double additionalSavings = costs[costs.length - 1] * numSitesNotIncluded/numSites * inflationArr[inflationArr.length - costs.length];
						values[values.length - 1] += additionalSavings;
//						values[values.length - 1] += additionalSavings * percentRealized;
						if(sysSavings.containsKey(system)) {
							double currSysSavings = sysSavings.get(system);
							currSysSavings += additionalSavings;
							sysSavings.put(system, currSysSavings);
						}
//						} else {
//							sysSavings.put(system, additionalSavings * percentRealized);
//						}
					}
				}
			}

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
						Double currSysSavings = sysSavings.get(system);
						
						if(currSysSavings != null) {
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
	
							value += inflatedSavings - currSysSavings;
						}
					}
				}
				if(missingDataMapBySystem.containsKey(system)){
					missing = true;
					HashMap<Integer, Boolean> innerMap = missingDataMapBySystem.get(system);
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
			systemOutputList.add(row);
		}
		
		// add in systems not in deployment strategy to be decommissioned at FOC
		int i;
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
			systemOutputList.add(row);
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

		systemOutputList.add(row);
	}

	public void runSupportQueries() {
		this.tapPortfolio = (IEngine) DIHelper.getInstance().getLocalProp(TAP_PORTFOLIO);
		this.tapSite = (IEngine) DIHelper.getInstance().getLocalProp(TAP_SITE);
		this.hrCore = (IEngine) DIHelper.getInstance().getLocalProp(HR_CORE);
		if(tapPortfolio == null) {
			throw new NullPointerException("Need to add TAP_Portfolio db.");
		}
		if(tapSite == null) {
			throw new NullPointerException("Need to add TAP_Site_DB db.");
		}
		if(hrCore == null) {
			throw new NullPointerException("Need to add HR_Core db.");
		}
		
		
		sysSustainmentInfoHash = DHMSMDeploymentHelper.getSysSustainmentBudget(tapPortfolio);
		
		HashMap<String, HashMap<String, Double>> sysSiteSupportCostHash = DHMSMDeploymentHelper.getSysSiteSupportCost(tapPortfolio);
		HashMap<String, HashMap<String, Double>> sysFloaterCostHash = DHMSMDeploymentHelper.getSysFloaterCost(tapPortfolio);
		sysSiteSupportAndFloaterCostHash = addAllCostInfo(sysSiteSupportCostHash, sysFloaterCostHash);
		
		numSitesForSysHash = DHMSMDeploymentHelper.getNumSitesSysDeployedAt(tapSite);
		systemsForSiteHash = DHMSMDeploymentHelper.getSysAtSitesInDeploymentPlan(tapSite);
		siteLocationHash = DHMSMDeploymentHelper.getSiteLocation(tapSite);
		
		ArrayList<String> waveOrder = DHMSMDeploymentHelper.getWaveOrder(tapSite);
		HashMap<String, List<String>> sitesInMultipleWavesHash = DHMSMDeploymentHelper.getSitesAndMultipleWaves(tapSite);
		lastWaveForSitesAndFloatersInMultipleWavesHash = DHMSMDeploymentHelper.determineLastWaveForInput(waveOrder, sitesInMultipleWavesHash);
		
		HashMap<String, List<String>> floaterWaveList = DHMSMDeploymentHelper.getFloatersAndWaves(tapSite);
		lastWaveForSitesAndFloatersInMultipleWavesHash.putAll(DHMSMDeploymentHelper.determineLastWaveForInput(waveOrder, floaterWaveList));
		
		firstWaveForSitesAndFloatersInMultipleWavesHash = DHMSMDeploymentHelper.determineFirstWaveForInput(waveOrder, sitesInMultipleWavesHash);
		firstWaveForSitesAndFloatersInMultipleWavesHash.putAll(DHMSMDeploymentHelper.determineFirstWaveForInput(waveOrder, floaterWaveList));
		
		waveForSites = DHMSMDeploymentHelper.getSitesAndWaves(tapSite);
		
		waveStartEndDate = DHMSMDeploymentHelper.getWaveStartAndEndDate(tapSite);
		
		lastWaveForEachSystem = DHMSMDeploymentHelper.getLastWaveForEachSystem(tapSite, waveOrder);
		firstWaveForEachSystem = DHMSMDeploymentHelper.getFirstWaveForEachSystem(tapSite, waveOrder);
		
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
	
	public HashMap<String, String[]> getWaveStartEndDate() {
		return waveStartEndDate;
	}

	public void setWaveStartEndDate(HashMap<String, String[]> waveStartEndDate) {
		this.waveStartEndDate = waveStartEndDate;
	}


	public ArrayList<Object[]> getSystemOutputList() {
		return systemOutputList;
	}


	public void setSystemOutputList(ArrayList<Object[]> systemOutputList) {
		this.systemOutputList = systemOutputList;
	}


	public String[] getSysNames() {
		return sysNames;
	}


	public void setSysNames(String[] sysNames) {
		this.sysNames = sysNames;
	}


	public ArrayList<Object[]> getSiteOutputList() {
		return siteOutputList;
	}


	public void setSiteOutputList(ArrayList<Object[]> siteOutputList) {
		this.siteOutputList = siteOutputList;
	}


	public String[] getSiteNames() {
		return siteNames;
	}


	public void setSiteNames(String[] siteNames) {
		this.siteNames = siteNames;
	}
	
	public Set<String> getAllSystems() {
		Set<String> allSysList = new HashSet<String>();
		for(String sys : sysList) {
			allSysList.add(sys);
		}
		for(String sys : systemsToAddList) {
			allSysList.add(sys);
		}
		 return allSysList;
	}
	
	public HashMap<String, String> getLastWaveForEachSystem(){
		return lastWaveForEachSystem;
	}
	
	public HashMap<String, String> getFirstWaveForEachSystem(){
		return firstWaveForEachSystem;
	}
	
	public HashMap<String, String> getLastWaveForSitesAndFloatersInMultipleWavesHash() {
		return lastWaveForSitesAndFloatersInMultipleWavesHash;
	}
	
	public HashMap<String, String> getFirstWaveForSitesAndFloatersInMultipleWavesHash() {
		return firstWaveForSitesAndFloatersInMultipleWavesHash;
	}
	
	public HashMap<String, ArrayList<String>> getSystemsForSiteHash() {
		return systemsForSiteHash;
	}
	
	public HashMap<String, HashMap<String, Double>> getSiteLocationHash() {
		return siteLocationHash;
	}
	
	public HashMap<String, List<String>> getWaveForSites() {
		return waveForSites;
	}
}