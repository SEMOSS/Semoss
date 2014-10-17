package prerna.ui.components.specific.tap;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.util.DIHelper;

public class DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor {
	
	private static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor.class.getName());
	
	private final String masterQuery = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Region ?Wave ?HostSite ?System WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave} {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} }&SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	private String masterQueryForSingleSystem = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Region ?Wave ?HostSite ?System WHERE { BIND(@SYSTEM@ AS ?System) {?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave} {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} }&SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	
	private final String TAP_PORTFOLIO = "TAP_Portfolio";
	private final String TAP_SITE = "TAP_Site_Data";
	private IEngine tapPortfolio;
	private IEngine tapSite;
	
	private DualEngineGridPlaySheet dualQueries = new DualEngineGridPlaySheet();	
	
	private HashMap<String, Double[]> sysSustainmentInfoHash;
	private HashMap<String, Double> numSitesForSysHash;
	private HashMap<String, String> regionStartDateHash;
	private HashMap<String, Double> sitesInMultipleWavesHash;
	private HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>> masterHash;
	private HashMap<String, double[]> savingsData = new HashMap<String, double[]>();
	
	private ArrayList<String> regionOrder;
	private ArrayList<String> waveOrder;
	
	private ArrayList<Object[]> list;
	private String[] names;
	
	public ArrayList<Object[]> getList() {
		return list;
	}
	
	public String[] getNames() {
		return names;
	}
	
	public void processData() {
		int minYear = 3000; // arbitrarily large year
		int maxYear = 0;
		for(String region : regionStartDateHash.keySet()) {
			String startDate = regionStartDateHash.get(region);
			String time[] = startDate.split("FY");
			String year = time[1];
			int yearAsNum = Integer.parseInt(year);
			if(yearAsNum > maxYear) {
				maxYear = yearAsNum;
			} else if(yearAsNum < minYear) {
				minYear = yearAsNum;
			}
		}
		int numColumns = maxYear - minYear + 2; // costs gains are realized a year after
		double[] inflationArr = new double[numColumns];
		int i;
		for(i = 0; i < numColumns; i++) {
			if(i <= 1) {
				inflationArr[i] = 1;
			} else {
				// only add inflation for years we don't have O&M budget info for
				inflationArr[i] = Math.pow(1.03, i-1);
			}
		}
		
		for(i = 0; i < regionOrder.size(); i++) 
		{
			String region = regionOrder.get(i);
			String startDate = regionStartDateHash.get(region);
			String time[] = startDate.split("FY");
			String quarter = time[0];
			String year = time[1];
			if(masterHash.get(region) != null){
				int numWaves = masterHash.get(region).keySet().size();
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
					case "2017" : sustainmentIndex = 2; break;
					case "2018" : sustainmentIndex = 3; break;
					default : sustainmentIndex = 4;
				}
				
				int outputYear = 0;
				switch(year) { // outputYear starts at 1 since cost savings are realized the following year
					case "2017" : outputYear = 1; break;
					case "2018" : outputYear = 2; break;
					case "2019" : outputYear = 3; break;
					case "2020" : outputYear = 4; break;
					case "2021" : outputYear = 5; break;
					case "2022" : outputYear = 6; break;
					case "2023" : outputYear = 7; break;
					case "2024" : outputYear = 8; break;
				}
				HashMap<String, HashMap<String, ArrayList<String>>> waves = masterHash.get(region);
				int j;
				for(j = 0; j < waveOrder.size(); j++) {
					String wave = waveOrder.get(j);
					if(waves.keySet().contains(wave)){
						HashMap<String, ArrayList<String>> sites = waves.get(wave);
						startPercent += perOfYear;
						if(startPercent >= 1.0) {
							startPercent -= 1.0;
							outputYear++;//we assume all of the saving comes at the end
							if(sustainmentIndex < 4) {
								sustainmentIndex++;
							}
						}
						for(String site : sites.keySet()) {
							// since going through waves in order, i can remove waves and only add the site when:
							// it's not contained, or the array list has only one thing left in it
							boolean addSite = false;
							if(!sitesInMultipleWavesHash.containsKey(site)) {
								addSite = true;
							} else {
								Double count = sitesInMultipleWavesHash.get(site);
								if(count == 1) {
									addSite = true;
									sitesInMultipleWavesHash.remove(site);
								} else {
									Double newCount = count--;
									sitesInMultipleWavesHash.put(site, newCount);
								}
							}
							if(addSite) {
								ArrayList<String> systems = sites.get(site);
								double [] yearlySavings = new double[numColumns];
								
								int counter = 0;
								for(int index = outputYear; index < yearlySavings.length; index++) {
									double savings = 0.0;
									for(String system : systems){
										Double [] costs = sysSustainmentInfoHash.get(system);
										// assume cost for a specific site is total cost / num sites
										double numSites = numSitesForSysHash.get(system);
										if(costs != null){
											if(costs[sustainmentIndex + counter].equals(null)) {
												savings += 0;
											} else {
												savings += costs[sustainmentIndex + counter] / numSites;
											}
										}
									}
									if(sustainmentIndex+counter < 4) {
										counter++;
									}
									yearlySavings[index] = savings * inflationArr[index];
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
			}
		}
		
		list = new ArrayList<Object[]>();
		int numCols = 0;
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
		double[] totalCol = null; 
		for(String site : savingsData.keySet()) {
			double[] values = savingsData.get(site);
			
			if(list.isEmpty()) {
				numCols = values.length+2;
				totalCol = new double[numCols-2];
				names = new String[numCols];
				//TODO: pass start FY
				int fy = 2017;
				int index;
				for(index = 0; index < numCols - 1; index++) {
					if(index == 0) {
						names[0] = "HostSite";
					}
					String fyString = "" + fy;
					fyString = "FY" + fyString.substring(2,4);
					names[index+1] = fyString;
					fy++;				
				}
				names[index] = "Total";
			}
			
			Object[] row = new Object[numCols];
			double totalRow = 0;
			row[0] = site;
			int index;
			for(index = 0; index < numCols - 2; index++) {
				double value = values[index];
				totalRow+=value;
				row[index + 1] = formatter.format(values[index]);
				totalCol[index] += value;
			}
			row[index+1] = formatter.format(totalRow);
			list.add(row);
		}
		
		//add column totals
		Object[] row = new Object[numCols];
		row[0] = "Total";
		double combinedTotal = 0;
		int index;
		for(index = 0; index < numCols - 2; index++) {
			double value = totalCol[index];
			row[index + 1] = formatter.format(value);
			combinedTotal += value;
		}
		row[index+1] = formatter.format(combinedTotal);
		list.add(row);
	}

	public void runSupportQueries() {
		this.tapPortfolio = (IEngine) DIHelper.getInstance().getLocalProp(TAP_PORTFOLIO);
		this.tapSite = (IEngine) DIHelper.getInstance().getLocalProp(TAP_SITE);
		
		if(sysSustainmentInfoHash == null){
			sysSustainmentInfoHash = DHMSMDeploymentHelper.getSysSustainmentBudget(tapPortfolio);
		}
		if(numSitesForSysHash == null) {
			numSitesForSysHash = DHMSMDeploymentHelper.getNumSitesSysDeployedAt(tapSite);
		}
		
		if(regionStartDateHash == null) {
			regionStartDateHash = DHMSMDeploymentHelper.getRegionStartDate(tapSite);
		}
		if(sitesInMultipleWavesHash == null) {
			sitesInMultipleWavesHash = DHMSMDeploymentHelper.getSiteAndMultipleWaveCount(tapSite);
		}
		if(regionOrder == null) {
			regionOrder = DHMSMDeploymentHelper.getRegionOrder(tapSite);
		}
		
		if(waveOrder == null) {
			waveOrder = DHMSMDeploymentHelper.getWaveOrder(tapSite);
		}
	}

	public void runMainQuery(String sysURI) {
		String query = masterQuery;
		if(!sysURI.isEmpty()) {
			query = masterQueryForSingleSystem.replace("@SYSTEM@", sysURI);
		}
		
		LOGGER.info(query);
		
		if(masterHash == null) {
			masterHash = new HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>>();
			//Use Dual Engine Grid to process the dual query that gets cost info
			dualQueries.setQuery(query);
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
				if(masterHash.containsKey(region)) {
					HashMap<String, HashMap<String, ArrayList<String>>> regionInfo = masterHash.get(region);
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
					// put from region info
					ArrayList<String> systemList = new ArrayList<String>();
					systemList.add(system);
					HashMap<String, ArrayList<String>> newWaveInfo = new HashMap<String, ArrayList<String>>();
					newWaveInfo.put(site, systemList);
					HashMap<String, HashMap<String, ArrayList<String>>> newRegionInfo = new HashMap<String, HashMap<String, ArrayList<String>>>();
					newRegionInfo.put(wave, newWaveInfo);
					masterHash.put(region, newRegionInfo);
				}
			}
		}
		
	}

	
}