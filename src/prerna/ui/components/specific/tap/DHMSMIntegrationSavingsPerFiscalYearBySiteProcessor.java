package prerna.ui.components.specific.tap;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.util.DIHelper;

public class DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor {
	

	private final String masterQuery = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Region ?Wave ?DCSite ?System WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} }&SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	private String masterQueryForSingleSystem = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Region ?Wave ?DCSite ?System WHERE { BIND(@SYSTEM@ AS ?System) {?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} }&SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	
	private final String TAP_PORTFOLIO = "TAP_Portfolio";
	private final String TAP_SITE = "TAP_Site_Data";
	private IEngine tapPortfolio;
	private IEngine tapSite;
	private DualEngineGridPlaySheet dualQueries = new DualEngineGridPlaySheet();	
	private HashMap<String, Double[]> sysSustainmentInfoHash= new HashMap<String, Double[]>();
	private HashMap<String, Double> numSitesForSysHash = new HashMap<String, Double>();
	private HashMap<String, String> regionStartDateHash = new HashMap<String, String>();
	private HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>> query5Data = new HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>>();
	private HashMap<String, double[]> savingsData = new HashMap<String, double[]>();
	
	private String firstRegion;
	private String firstWave;
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
		// store sites since we assume we decommission sites after we visit them once
		Set<String> siteList = new HashSet<String>();
		
		int minYear = 3000;//arbitrarily large year
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
		int numColumns = maxYear - minYear + 1;
		
		int i;
		for(i = 0; i < regionOrder.size(); i++) 
		{
			String region = regionOrder.get(i);
			String startDate = regionStartDateHash.get(region);
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
					case "2017" : sustainmentIndex = 2; break;
					case "2018" : sustainmentIndex = 3; break;
					default: sustainmentIndex = 4;
				}
				int outputYear = 0;
				switch(year) {
					case "2017" : outputYear = 0; break;
					case "2018" : outputYear = 1; break;
					case "2019" : outputYear = 2; break;
					case "2020" : outputYear = 3; break;
					case "2021" : outputYear = 4; break;
					case "2022" : outputYear = 5; break;
					case "2023" : outputYear = 6; break;
				}
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
							if(!siteList.contains(site)) {
								siteList.add(site);
								ArrayList<String> systems = sites.get(site);
								double savings = 0.0;
								double [] yearlySavings = new double[numColumns];
								for(String system : systems){
									Double [] costs = sysSustainmentInfoHash.get(system);
									// assume cost for a specific site is total cost / num sites
									double numSites = numSitesForSysHash.get(system);
									if(costs != null){
										if(costs[sustainmentIndex].equals(null)){
											savings += 0;
										} else {
											// for debugging
//											System.out.println(system);
//											System.out.println("\t" + costs[sustainmentIndex] );
//											System.out.println("\t" + numSites);
//											System.out.println("\t" + costs[sustainmentIndex] / numSites);
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
		}
		
		list = new ArrayList<Object[]>();
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
		for(String site : savingsData.keySet()) {
			double[] values = savingsData.get(site);
			if(list.isEmpty()) {
				names = new String[values.length + 1];
				names[0] = "";
				//TODO: pass start FY
				int fy = 2017;
				for(int index = 0; index < values.length; index++) {
					if(index == 0) {
						names[0] = "Site";
					}
					String fyString = "FY" + fy;
					names[index+1] = fyString;
					fy++;				
				}
			}
			
			Object[] row = new Object[values.length + 1];
			row[0] = site;
			for(int index = 0; index < values.length; index++) {
				row[index + 1] = formatter.format(values[index]);
			}
			list.add(row);
		}
	}

	public void runSupportQueries() {
		this.tapPortfolio = (IEngine) DIHelper.getInstance().getLocalProp(TAP_PORTFOLIO);
		this.tapSite = (IEngine) DIHelper.getInstance().getLocalProp(TAP_SITE);
		
		if(sysSustainmentInfoHash.isEmpty()){
			sysSustainmentInfoHash = DHMSMDeploymentHelper.getSysSustainmentBudget(tapPortfolio);
		}
		if(numSitesForSysHash.isEmpty()) {
			numSitesForSysHash = DHMSMDeploymentHelper.getNumSitesSysDeployedAt(tapSite);
		}
		
		if(regionStartDateHash.isEmpty()) {
			regionStartDateHash = DHMSMDeploymentHelper.getRegionStartDate(tapSite);
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
		
		if(query5Data.isEmpty()) {
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
		
	}

	
}