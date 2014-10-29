package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

public final class DHMSMDeploymentHelper {

	public static final String SYS_AT_SITE_IN_DEPLOYMENT_PLAN_QUERY = "SELECT DISTINCT ?HostSite ?System WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}{?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } ORDER BY ?HostSite ?System";
	public static final String CENTRALLY_DEPLOYED_SYS_QUERY = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> 'Y'} }";
	
	public static final String SYS_SUSTIANMENT_BUDGET_QUERY = "SELECT DISTINCT ?System (SUM(?Cost) AS ?Sustainment) ?FY WHERE { { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/OldPrositeCost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?OMTag <http://semoss.org/ontologies/Relation/Includes> ?GLTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/OldPrositeCost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?SystemServiceBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudget} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/Contains/SelfReportCost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?SystemServiceBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudget} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?OMTag <http://semoss.org/ontologies/Relation/Includes> ?GLTag} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/Contains/SelfReportCost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } } GROUP BY ?System ?FY ORDER BY ?System";
	
	public static final String SYS_COUNT_AT_SITES = "SELECT ?System (COUNT(?HostSite) AS ?NumSites) WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} } GROUP BY ?System";
	
//	public static final String REGION_START_DATE = "SELECT DISTINCT ?Region ?Date WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>}{?Date <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Region <http://semoss.org/ontologies/Relation/BeginsOn> ?Date} }";
	public static final String WAVE_ORDER_QUERY = "SELECT DISTINCT ?Wave1 ?Wave2 WHERE {{?Wave1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave1 <http://semoss.org/ontologies/Relation/Precedes> ?Wave2}} ";
//	public static final String REGION_ORDER_QUERY = "SELECT DISTINCT ?Region1 ?Region2 WHERE {{?Region1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region1 <http://semoss.org/ontologies/Relation/Preceeds> ?Region2}}";
	
//	public static final String FIRST_REGION_QUERY = "SELECT DISTINCT ?Region1 WHERE { {?Region1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} MINUS{ {?Region2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Region2 <http://semoss.org/ontologies/Relation/Preceeds> ?Region1}}  }";
	public static final String FIRST_WAVE_QUERY = "SELECT DISTINCT ?Wave1 WHERE { {?Wave1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} MINUS{ {?Wave2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave2 <http://semoss.org/ontologies/Relation/Precedes> ?Wave1}} }";
	
	public static final String SYS_DEPLOYED_AT_SITE_QUERY = "SELECT DISTINCT ?HostSite ?System WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } ORDER BY ?HostSite ?System";
	public static final String SITE_IN_MULTIPLE_WAVES_QUERY = "SELECT DISTINCT ?HostSite ?Wave WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} FILTER( ?WaveNum > 1) { SELECT DISTINCT ?HostSite (COUNT(DISTINCT ?Wave) AS ?WaveNum) WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} } GROUP BY ?HostSite } } ORDER BY ?HostSite";
	public static final String SITE_IN_MULTIPLE_WAVES_COUNT_QUERY = "SELECT DISTINCT ?HostSite (COUNT(DISTINCT ?Wave) AS ?WaveCount) WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} FILTER( ?WaveNum > 1) { SELECT DISTINCT ?HostSite (COUNT(DISTINCT ?Wave) AS ?WaveNum) WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} } GROUP BY ?HostSite } } GROUP BY ?HostSite ORDER BY ?HostSite";
	
	public static final String WAVE_START_END_DATE = "SELECT DISTINCT ?Wave ?StartDate ?EndDate WHERE { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?StartDate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Wave <http://semoss.org/ontologies/Relation/BeginsOn> ?StartDate} {?EndDate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?EndDate} }";
	
	private DHMSMDeploymentHelper() {
		
	}
	
	public static HashMap<String, String[]> getWaveStartAndEndDate(IEngine engine) {
		HashMap<String, String[]> retHash = new HashMap<String, String[]>();
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, WAVE_START_END_DATE);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
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
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, SITE_IN_MULTIPLE_WAVES_COUNT_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String site = sjss.getVar(names[0]).toString();
			Double count = (Double) sjss.getVar(names[1]);
			retHash.put(site, count);
		}
		
		return retHash;
	}
	
	public static HashMap<String, ArrayList<String>> getSitesAndMultipleWaves(IEngine engine) {
		HashMap<String, ArrayList<String>> retHash = new HashMap<String, ArrayList<String>>();
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, SITE_IN_MULTIPLE_WAVES_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String site = sjss.getVar(names[0]).toString();
			String wave = sjss.getVar(names[1]).toString();
			ArrayList<String> waveList;
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
	
	
//	public static ArrayList<String> getRegionOrder(IEngine engine) {
//		//get first region and wave
//		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, FIRST_REGION_QUERY);
//		String[] names = sjsw.getVariables();
//		String firstRegion = sjsw.next().getVar(names[0]).toString();
//		
//		sjsw = Utility.processQuery(engine, REGION_ORDER_QUERY);
//		names = sjsw.getVariables();
//		HashMap<String, String> regionHash = new HashMap<String, String>();
//		int counter = 0;
//		while(sjsw.hasNext()) {
//			SesameJenaSelectStatement sjss = sjsw.next();
//			String region1 = sjss.getVar(names[0]).toString();
//			String region2 = sjss.getVar(names[1]).toString();
//			regionHash.put(region1, region2);
//			counter++;
//		}
//		String nextRegion = firstRegion;
//		ArrayList<String> regionOrder = new ArrayList<String>();
//		regionOrder.add(nextRegion);
//		for(int i = 0; i < counter; i++) {
//			nextRegion = regionHash.get(nextRegion);
//			regionOrder.add(nextRegion);
//		}
//		
//		return regionOrder;
//	}
	
	public static ArrayList<String> getWaveOrder(IEngine engine) {
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, FIRST_WAVE_QUERY);
		String[] names = sjsw.getVariables();
		String firstWave = sjsw.next().getVar(names[0]).toString();
	
	
		sjsw = Utility.processQuery(engine, WAVE_ORDER_QUERY);
		names = sjsw.getVariables();
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
		ArrayList<String> waveOrder = new ArrayList<String>();
		waveOrder.add(nextWave);
		for(int i = 0; i < counter; i++) {
			nextWave = regionHash.get(nextWave);
			waveOrder.add(nextWave);
		}
		
		return waveOrder;
	}
	
	
//	public static HashMap<String, String> getRegionStartDate(IEngine engine) {
//		HashMap<String, String> retHash = new HashMap<String, String>();
//		
//		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, REGION_START_DATE);
//		String[] names = sjsw.getVariables();
//		while(sjsw.hasNext()) {
//			SesameJenaSelectStatement sjss = sjsw.next();
//			String regionName = sjss.getVar(names[0]).toString();
//			String regionStartDate = sjss.getVar(names[1]).toString();
//			retHash.put(regionName, regionStartDate);
//		}
//		
//		return retHash;
//	}
	
	public static HashMap<String, Double> getNumSitesSysDeployedAt(IEngine engine) {
		HashMap<String, Double> retHash = new HashMap<String, Double>();
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, SYS_COUNT_AT_SITES);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String sysName = sjss.getVar(names[0]).toString();
			Double siteCount = (Double) sjss.getVar(names[1]);
			retHash.put(sysName, siteCount);
		}
		
		return retHash;
	}
	
	public static HashMap<String, Double[]> getSysSustainmentBudget(IEngine engine) {
		HashMap<String, Double[]> retHash = new HashMap<String, Double[]>();
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, SYS_SUSTIANMENT_BUDGET_QUERY);
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
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, SYS_AT_SITE_IN_DEPLOYMENT_PLAN_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
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
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, CENTRALLY_DEPLOYED_SYS_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString();
			retSet.add(sys);
		}
		return retSet;
	}
}
