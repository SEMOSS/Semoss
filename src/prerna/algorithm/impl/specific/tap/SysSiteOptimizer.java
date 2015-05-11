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
package prerna.algorithm.impl.specific.tap;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.apache.commons.math3.exception.TooManyEvaluationsException;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.OptimizationData;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.MultiStartUnivariateOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariateOptimizer;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well1024a;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.math.StatisticsUtilityMethods;
import prerna.ui.components.playsheets.ColumnChartPlaySheet;
import prerna.ui.components.playsheets.OCONUSMapPlaySheet;
import prerna.ui.components.specific.tap.HealthGridSheet;

/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class SysSiteOptimizer extends UnivariateOpt {
	
	private static final Logger LOGGER = LogManager.getLogger(SysSiteOptimizer.class.getName());
	
	private IEngine systemEngine, costEngine, siteEngine;
	
	private ArrayList<String> sysList, dataList, bluList, siteList, capList;
	private ArrayList<String> centralSysList;
	
	//user must select these
	private double budgetForYear;
	private int years;
	private Boolean useDHMSMFunctionality;//true if data/blu list made from dhmsm capabilities. False if from the systems
	private Boolean isOptimizeBudget;
	private String type;

	//user can change these as advanced settings
	private double infRate, disRate, trainingPerc;
	private int noOfPts;
	
	//user should not change these
	private double centralDeploymentPer = 0.80;
	private double deploymentFactor = 5;
	
	//generated data stores based off of the users selections
	private Integer[] modArr, decomArr;
	private int[][] systemDataMatrix, systemBLUMatrix;
	private double[][] systemSiteMatrix;
	private int[] systemTheater, systemGarrison;
	private double[] interfaceCostArr;
	private double[] maintenaceCosts, siteMaintenaceCosts, siteDeploymentCosts;
	
	private Integer[] centralModArr, centralDecomArr;
	private int[][] centralSystemDataMatrix, centralSystemBLUMatrix;
	private double[][] centralSystemSiteMatrix;
	private double[] centralSystemNumSite;
	private int[] centralSystemTheater, centralSystemGarrison;
	private double[] centralInterfaceCostArr;
	private double[] centralSystemMaintenaceCosts;

	private int[][] systemCapMatrix, centralSystemCapMatrix;
	
	private int[][] capBLUMatrix, capDataMatrix;
	
	//results of the algorithm
	private SysSiteOptFunction optFunc;
		
	private double[] sysKeptArr, centralSysKeptArr;
	private double[][] systemSiteResultMatrix;
	
	private String[][] systemSiteRecMatrix;
	
	private String sysKeptQueryString = "";
	private int numSysKept, numCentralSysKept;
	
	private double[] siteLat, siteLon;
	private double yearsToComplete;
	private double currSustainmentCost, futureSustainmentCost;
	private double adjustedDeploymentCost, adjustedTotalSavings;
	
	private double[] deployCostPerYear, currCostPerYear, futureCostPerYear, costAvoidedPerYear;

	@Override
	public void execute() {
		executeWeb();
		display();
	}
	
	public void executeWeb() {
	
		long startTime;
		long endTime;
		
		startTime = System.currentTimeMillis();
		createSiteDataBLULists();
		getData();
		endTime = System.currentTimeMillis();
		printMessage("Time to query data " + (endTime - startTime) / 1000 + " seconds");
		
		startTime = System.currentTimeMillis();			
		optimizeSystemsAtSites(isOptimizeBudget, systemSiteMatrix, currSustainmentCost, budgetForYear, years);
		endTime = System.currentTimeMillis();
		printMessage("Time to run Optimization " + (endTime - startTime) / 1000 + " seconds");
		
	}
	
	public void setEngines(IEngine systemEngine, IEngine costEngine, IEngine siteEngine) {
		this.systemEngine = systemEngine;
		this.costEngine = costEngine;
		this.siteEngine = siteEngine;
	}
	
	public void setUseDHMSMFunctionality(Boolean useDHMSMFunctionality) {
		this.useDHMSMFunctionality = useDHMSMFunctionality;
	}
	
	public void setVariables(int budgetForYear, int years, double infRate, double disRate, double trainingPerc, int noOfPts) {
		this.budgetForYear = budgetForYear;
		this.years = years;
		this.infRate = infRate;
		this.disRate = disRate;
		this.trainingPerc = trainingPerc;
		this.noOfPts = noOfPts;
	}
	
	public void setSysList(ArrayList<String> sysList) {
		
		String centralSysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> 'Y'}{?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0)} ORDER BY ?System LIMIT 50";
		ArrayList<String> unfilteredCentralSys = SysOptUtilityMethods.runListQuery(systemEngine, centralSysQuery);
		this.centralSysList = new ArrayList<String>();
		
		int i;
		int numCentral = unfilteredCentralSys.size();
		for(i=0; i<numCentral; i++) {
			String sys = unfilteredCentralSys.get(i);
			if(sysList.contains(sys)) {
				sysList.remove(sys);
				centralSysList.add(sys);
			}
		}
		
		this.sysList = sysList;
		
		printMessage("Not Central Systems are..." + SysOptUtilityMethods.createPrintString(sysList));
		printMessage("Central Systems are..." + SysOptUtilityMethods.createPrintString(centralSysList));
	}
	
	public void setMustModDecomList(ArrayList<String> modList, ArrayList<String> decomList) {

		int i;
		int index;
		int numMod = modList.size();
		int numDecom = decomList.size();
		
		modArr = new Integer[sysList.size()];
		centralModArr = new Integer[centralSysList.size()];
		
		decomArr = new Integer[sysList.size()];
		centralDecomArr = new Integer[centralSysList.size()];
		
		NEXT: for(i=0; i<numMod; i++) {
			String mod = modList.get(i);
			index = sysList.indexOf(mod);
			if(index > -1) {
				modArr[index] = 1;
				continue NEXT;
			}
			
			index = centralSysList.indexOf(mod);
			if(index > -1) {
				centralModArr[index] = 1;
			}	
		}
		
		NEXT: for(i=0; i<numDecom; i++) {
			String decom = decomList.get(i);
			index = sysList.indexOf(decom);
			if(index > -1) {
				decomArr[index] = 1;
				continue NEXT;
			}
			
			index = centralSysList.indexOf(decom);
			if(index > -1) {
				centralDecomArr[index] = 1;
			}	
		}
		
	}
	
	public void setSysHashList(ArrayList<Hashtable<String, String>> sysHashList) {
		sysList = new ArrayList<String>();
		centralSysList = new ArrayList<String>();
		
		String centralSysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> 'Y'}{?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0)} ORDER BY ?System LIMIT 50";
		ArrayList<String> allCentralSys = SysOptUtilityMethods.runListQuery(systemEngine, centralSysQuery);
		
		int numCentral = 0;
		int i;
		int numSysHash = sysHashList.size();
		for(i = 0; i < numSysHash; i++) {
			Hashtable<String, String> sysHash = sysHashList.get(i);
			String name = sysHash.get("name");
			if(allCentralSys.contains(name))
				numCentral++;
		}

		int numNonCentral = numSysHash - numCentral;
		
		modArr = new Integer[numNonCentral];
		centralModArr = new Integer[numCentral];
		
		decomArr = new Integer[numNonCentral];
		centralDecomArr = new Integer[numCentral];		
		
		int centralIndex = 0;
		int nonCentralIndex = 0;
		for(i = 0; i < numSysHash; i++) {
			Hashtable<String, String> sysHash = sysHashList.get(i);
			String sys = sysHash.get("name");
			String status = sysHash.get("ind");
			
			if(allCentralSys.contains(sys)) {
				
				centralSysList.add(sys);
				
				if(status.equals("Modernize"))
					centralModArr[centralIndex] = 1;
				else 
					centralModArr[centralIndex] = 0;

				if(status.equals("Decommission"))
					centralDecomArr[centralIndex] = 1;
				else
					centralDecomArr[centralIndex] = 0;
				centralIndex++;
			
			}else {
				
				sysList.add(sys);
				
				if(status.equals("Modernize"))
					modArr[nonCentralIndex] = 1;
				else
					modArr[nonCentralIndex] = 0;
				
				if(status.equals("Decommission"))
					decomArr[nonCentralIndex] = 1;
				else
					decomArr[nonCentralIndex] = 0;
				nonCentralIndex++;
			}
		}
		
		printMessage("Not Central Systems are..." + SysOptUtilityMethods.createPrintString(sysList));
		printMessage("Central Systems are..." + SysOptUtilityMethods.createPrintString(centralSysList));

	}
	
	
	public void setOptimizationType(String type) {
		this.type = type;
	}
	
	public void setIsOptimizeBudget(Boolean isOptimizeBudget) {
		this.isOptimizeBudget = isOptimizeBudget;
	}
	
	public void setCapList(ArrayList<String> capList) {
		this.capList = capList;
	}
	
	private void createSiteDataBLULists() {

		String sysBindings = "{" + SysOptUtilityMethods.makeBindingString("System",sysList) + SysOptUtilityMethods.makeBindingString("System",centralSysList) + "}";
		
		String siteQuery = "SELECT DISTINCT ?Site WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;}{?Site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?Site;}{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite;} } ORDER BY ?Site BINDINGS ?System " + sysBindings;

		//any data and any blu is being selected
		String dataQuery, bluQuery;
		if(useDHMSMFunctionality) {
			dataQuery = "SELECT DISTINCT ?Data WHERE {BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?DHMSM){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability}{ ?Capability <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess.}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?Data.}}";
			bluQuery = "SELECT DISTINCT ?BLU WHERE {BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?DHMSM){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability}{ ?Capability <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess.}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?BLU.}}";
		}else {
			dataQuery = "SELECT DISTINCT ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data}} ORDER BY ?Data BINDINGS ?System " + sysBindings;
			bluQuery = "SELECT DISTINCT ?BLU WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU}} ORDER BY ?BLU BINDINGS ?System " + sysBindings;
		}
		
		String capQuery = "SELECT DISTINCT ?Capability WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/Supports> ?Capability}} ORDER BY ?Capability BINDINGS ?System " + sysBindings;

		siteList = SysOptUtilityMethods.runListQuery(siteEngine, siteQuery);
		dataList = SysOptUtilityMethods.runListQuery(systemEngine, dataQuery);
		bluList = SysOptUtilityMethods.runListQuery(systemEngine, bluQuery);
		if(capList == null || capList.isEmpty())
			capList = SysOptUtilityMethods.runListQuery(systemEngine, capQuery);
		
		printMessage("Sites are..." + SysOptUtilityMethods.createPrintString(siteList));
		printMessage("Data are..." + SysOptUtilityMethods.createPrintString(dataList));
		printMessage("BLU are..." + SysOptUtilityMethods.createPrintString(bluList));
		printMessage("Capabilities are..." + SysOptUtilityMethods.createPrintString(capList));
	}
	
	private void getData() {

		ResidualSystemOptFillData resFunc = new ResidualSystemOptFillData();
		resFunc.setSysSiteLists(sysList, dataList, bluList, siteList, capList);
		resFunc.setEngines(systemEngine, costEngine, siteEngine);
		resFunc.fillSysSiteOptDataStores(true);
		
		capBLUMatrix = resFunc.capabilityBLUMatrix;
		capDataMatrix = resFunc.capabilityDataMatrix;
		
		systemDataMatrix = resFunc.systemDataMatrix;
		systemBLUMatrix = resFunc.systemBLUMatrix;
		systemSiteMatrix = resFunc.systemSiteMatrix;
		systemCapMatrix = resFunc.systemCapabilityMatrix;
		
		systemTheater = resFunc.systemTheater;
		systemGarrison = resFunc.systemGarrison;

		double[] systemSustainmentBudget = resFunc.systemCostOfMaintenance;
		double[] systemNumOfSites = resFunc.systemNumOfSites;

		int i;
		int sysLength = sysList.size();
		maintenaceCosts = new double[sysLength];
		siteMaintenaceCosts = new double[sysLength];
		siteDeploymentCosts = new double[sysLength];
		
		for(i=0; i<sysLength; i++) {
			
			double sysBudget = systemSustainmentBudget[i];
			double numSites;

			if(systemNumOfSites[i]==0) {
				numSites = 1;
			}else {
				numSites = systemNumOfSites[i];
			}
				
			maintenaceCosts[i] = centralDeploymentPer * sysBudget;
			double siteMaintenance = (1 - centralDeploymentPer) * sysBudget / numSites;
			siteMaintenaceCosts[i] = siteMaintenance;
			siteDeploymentCosts[i] = siteMaintenance * deploymentFactor;
		}
		
		
		double[][] systemCostConsumeDataMatrix = resFunc.systemCostOfDataMatrix;
		
		resFunc.setSysSiteLists(centralSysList,dataList,bluList,siteList, capList);
		resFunc.fillSysSiteOptDataStores(false);
		
		centralSystemDataMatrix = resFunc.systemDataMatrix;
		centralSystemBLUMatrix = resFunc.systemBLUMatrix;
		centralSystemSiteMatrix = resFunc.centralSystemSiteMatrix;
		centralSystemNumSite = resFunc.centralSystemNumSite;

		centralSystemCapMatrix = resFunc.systemCapabilityMatrix;
		centralSystemTheater = resFunc.systemTheater;
		centralSystemGarrison = resFunc.systemGarrison;

		centralSystemMaintenaceCosts = resFunc.systemCostOfMaintenance;

		currSustainmentCost = calculateCurrentSustainmentCost(maintenaceCosts, centralSystemMaintenaceCosts);
		
		double[][] centralSystemCostConsumeDataMatrix = resFunc.systemCostOfDataMatrix;
		
		interfaceCostArr = calculateInterfaceCost(interfaceCostArr, systemCostConsumeDataMatrix, systemDataMatrix, systemTheater, systemGarrison);
		centralInterfaceCostArr = calculateInterfaceCost(centralInterfaceCostArr, centralSystemCostConsumeDataMatrix, centralSystemDataMatrix, centralSystemTheater, centralSystemGarrison);
		
		resFunc.fillSiteLatLon();
		siteLat = resFunc.siteLat;
		siteLon = resFunc.siteLon;
	}
	
	//TODO do we need costs for the central systems too?
	private double[] calculateInterfaceCost(double[] interfaceCostArr, double[][] costConsumeDataMatrix, int[][] sysDataMatrix, int[] sysTheater, int[] sysGarrison) {
		
		int i;
		int j;
		int numTheaterProviders;
		int numGarrisonProviders;
		double probInferfaceStaying;
		double probInferfaceStayingTheater;
		double probInferfaceStayingGarrison;
		int numData = sysDataMatrix[0].length;
		int numSystems = sysDataMatrix.length;
		
		interfaceCostArr = SysOptUtilityMethods.createEmptyVector(interfaceCostArr, numSystems);
		
		for(i=0; i<numData; i++) {
			numTheaterProviders = SysOptUtilityMethods.sumColIf(systemDataMatrix, i, systemTheater) + SysOptUtilityMethods.sumColIf(centralSystemDataMatrix, i, centralSystemTheater);
			numGarrisonProviders = SysOptUtilityMethods.sumColIf(systemDataMatrix, i, systemGarrison) + SysOptUtilityMethods.sumColIf(centralSystemDataMatrix, i, centralSystemGarrison);
			
			for(j=0; j<numSystems; j++) {
				probInferfaceStayingTheater = 1.0;
				probInferfaceStayingGarrison = 1.0;
				//in perfect solution only one provider should stay. probability that that one is the one currently receiving from is 1/total number
				if(sysTheater[j] == 1 && numTheaterProviders != 0)
					probInferfaceStayingTheater = 1.0/numTheaterProviders;
				if(sysGarrison[j] == 1 && numGarrisonProviders != 0)
					probInferfaceStayingGarrison = 1.0/numGarrisonProviders;
				probInferfaceStaying = Math.min(probInferfaceStayingTheater, probInferfaceStayingGarrison);
				//if the sys does not consume the data object, the costConsumeDataMatrix is already 0 so cost is 0
				interfaceCostArr[j] += (1.0-probInferfaceStaying) * costConsumeDataMatrix[j][i];
			}
		}
		
		return interfaceCostArr;
		
	}
	
	
	private double calculateCurrentSustainmentCost(double[] maintenaceCosts, double[] centralSysMaintenaceCosts) {
		double currSustainmentCost = 0.0;

		int i;
		int length = maintenaceCosts.length;
		for(i=0; i<length; i++) {
			currSustainmentCost += maintenaceCosts[i] / centralDeploymentPer;
		}
		
		length = centralSysMaintenaceCosts.length;
		for(i=0; i<length; i++) {
			currSustainmentCost += centralSysMaintenaceCosts[i];
		}
		
		return currSustainmentCost;
			
	}
	
	private double[] calculateSiteSustainCost(double[][] sysSiteMatrix, double[] sysCost, int[][] includeSystem, int includeSysCol) {
		int i;
		int j;
		int numSys = sysList.size();
		int numSite = siteList.size();
		double[] siteCostArr = new double[numSite];
		
		for(i=0; i<numSite; i++) {
			
			double siteCost = 0.0;
			for(j=0; j<numSys; j++) {
				
				if(sysSiteMatrix[j][i] == 1 && (includeSystem == null || includeSystem[j][includeSysCol] == 1))
					siteCost += sysCost[j];
					
			}
			siteCostArr[i] = siteCost;
		}
		
		return siteCostArr;
	}
	
	private double[] calculateHostSiteSustainCost(double[][] sysSiteMatrix, double[] sysCost, double[] numSiteMatrix, double[] sysKept, int[][] includeSystem2, int includeSysCol) {
		int i;
		int j;
		int numSys = centralSysList.size();
		int numSite = siteList.size();
		double[] siteCostArr = new double[numSite];
		
		for(i=0; i<numSite; i++) {
			
			double siteCost = 0.0;
			for(j=0; j<numSys; j++) {
				
				if(sysSiteMatrix[j][i] == 1 && numSiteMatrix[j] != 0 && (sysKept == null || sysKept[j] == 1)&& (includeSystem2 == null || includeSystem2[j][includeSysCol] == 1))
					siteCost += sysCost[j] / numSiteMatrix[j];
					
			}
			siteCostArr[i] = siteCost;
		}
		
		return siteCostArr;
	}
	
	private void optimizeSystemsAtSites(Boolean isOptimizeBudget, double[][] systemSiteMatrix, double currSustainmentCost, double budgetForYear, int years) {
		
		if(type.equals("Savings"))
			optFunc = new SysSiteSavingsOptFunction();
		else if(type.equals("ROI"))
			optFunc = new SysSiteROIOptFunction();
		else if(type.equals("IRR"))
			optFunc = new SysSiteIRROptFunction();
		else {
			printMessage("OPTIMIZATION TYPE DOES NOT EXIST");
			return;
		}
		optFunc.setPlaySheet(playSheet);
		optFunc.setVariables(systemDataMatrix, systemBLUMatrix, systemSiteMatrix, systemTheater, systemGarrison, modArr, decomArr, maintenaceCosts, siteMaintenaceCosts, siteDeploymentCosts, interfaceCostArr, centralInterfaceCostArr, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemTheater, centralSystemGarrison, centralModArr, centralDecomArr, centralSystemMaintenaceCosts, budgetForYear, years, currSustainmentCost, infRate, disRate, trainingPerc);

		optFunc.value(budgetForYear * years);
		futureSustainmentCost = optFunc.getFutureSustainmentCost();
		if(isOptimizeBudget && futureSustainmentCost!=currSustainmentCost) {
			UnivariateOptimizer optimizer = new BrentOptimizer(.05, 10000);
	
			RandomGenerator rand = new Well1024a(500);
			MultiStartUnivariateOptimizer multiOpt = new MultiStartUnivariateOptimizer(optimizer, noOfPts, rand);
			UnivariateObjectiveFunction objF = new UnivariateObjectiveFunction(optFunc);
			SearchInterval search = new SearchInterval(0, budgetForYear * years, budgetForYear * years);
			MaxEval eval = new MaxEval(10);
			
			OptimizationData[] data = new OptimizationData[] { search, objF, GoalType.MAXIMIZE, eval};
			try {
				UnivariatePointValuePair pair = multiOpt.optimize(data);
				optFunc.value(pair.getPoint());
				
			} catch (TooManyEvaluationsException fee) {
				printMessage("Too many evalutions");
			}
		}
	
		sysKeptArr = optFunc.getSysKeptArr();
		centralSysKeptArr = optFunc.getCentralSysKeptArr();
		systemSiteResultMatrix = optFunc.getSystemSiteResultMatrix();
		
		numSysKept = optFunc.getNumSysKept();
		numCentralSysKept = optFunc.getNumCentralSysKept();
		
		futureSustainmentCost = optFunc.getFutureSustainmentCost();
		adjustedTotalSavings = optFunc.getAdjustedTotalSavings();
		adjustedDeploymentCost = optFunc.getAdjustedDeploymentCost();

		yearsToComplete = optFunc.getYearsToComplete();
	
		systemSiteRecMatrix = calculateSiteRecMatrix(this.systemSiteMatrix, systemSiteResultMatrix);
		
		double mu = (1 + infRate / 100) / (1 + disRate / 100);
		
		deployCostPerYear = SysOptUtilityMethods.calculateAdjustedDeploymentCostArr(mu, yearsToComplete, years, -1 * budgetForYear);
		currCostPerYear = SysOptUtilityMethods.calculateAdjustedDeploymentCostArr(mu, Math.ceil(yearsToComplete), years, currSustainmentCost);
		futureCostPerYear =  SysOptUtilityMethods.calculateAdjustedSavingsArr(mu, yearsToComplete, years, futureSustainmentCost);
		costAvoidedPerYear =  SysOptUtilityMethods.calculateAdjustedSavingsArr(mu, yearsToComplete, years, currSustainmentCost - futureSustainmentCost);

	}
	
	private String[][] calculateSiteRecMatrix(double[][] oldMatrix, double[][] newMatrix) {
		int i;
		int j;

		int numRows = oldMatrix.length;
		int numCols = oldMatrix[0].length;
		
		String[][] siteRecMatrix = new String[numRows][numCols];
		
		for(i=0; i<numRows; i++) {
			for(j=0; j<numCols; j++) {
				if(oldMatrix[i][j] == 1 && newMatrix[i][j] == 1) {
					siteRecMatrix[i][j] = "SUSTAINED";
				} else if(oldMatrix[i][j] == 1 && newMatrix[i][j] == 0) {
					siteRecMatrix[i][j] = "CONSOLIDATED";
				} else if(oldMatrix[i][j] == 0 && newMatrix[i][j] == 1) {
					siteRecMatrix[i][j] = "DEPLOYED";
				}
			}
		}
	
		return siteRecMatrix;
	}

	
	public void display() {
				
		createSiteGrid(systemSiteMatrix, sysList, siteList,"Current NonCentral Systems at Sites");
		createSiteGrid(systemSiteResultMatrix, sysList, siteList,"Future NonCentral Systems at Sites");
		createSiteGrid(systemSiteRecMatrix, sysList, siteList,"Changes for NonCentral Systems at Sites");
		
		createOverallGrid(centralSysKeptArr, centralSysList, "Central System", "Future Central Systems (Was central system kept or decommissioned?)");
		createOverallGrid(sysKeptArr, sysList, "NonCentral Systems", "Future NonCentral Systems (Was noncentral system kept or decommissioned?)");
		
		printMessage("**Curr Sustainment Cost " + currSustainmentCost);
		printMessage("**Future Sustainment Cost " + futureSustainmentCost);
		printMessage("**Adjusted Deployment Cost " + adjustedDeploymentCost);
		
		int i;
		printMessage("**Year ....... Investment ....... CostAvoided: ");
		for(i = 0; i<years; i++) {
			printMessage((i + 1) + "     ....... " + (-1*deployCostPerYear[i]) + " ..... " + costAvoidedPerYear[i]);
		}
		
		if(optFunc instanceof SysSiteSavingsOptFunction)
			printMessage("**Adjusted Total Savings: " + adjustedTotalSavings);
		else if(optFunc instanceof SysSiteROIOptFunction)
			printMessage("**ROI: " + optFunc.getROI()+"%");
		else if(optFunc instanceof SysSiteIRROptFunction)
			printMessage("**IRR: " + optFunc.getIRR()+"%");
		
		createCostGrid();
	}
	
	

	private void createCostGrid() {
		
		String[] headers = new String[5];
		headers[0] = "System";
		headers[1] = "Sustain Cost";
		headers[2] = "Interface Cost";
		headers[3] = "Site Maintain Cost";
		headers[4] = "Site Deploy Cost";
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		int i=0;
		int rowLength = sysList.size();
		
		for(i = 0; i<rowLength; i++) {
			Object[] row = new Object[5];
			row[0] = sysList.get(i);
			row[1] = maintenaceCosts[i];
			row[2] = interfaceCostArr[i];
			row[3] = siteMaintenaceCosts[i];
			row[4] = siteDeploymentCosts[i];
			list.add(row);
		}
		createTabAndDisplayList(headers,list,"NonCentral System Costs",false);
	}
	
	private void createOverallGrid(double[] matrix, ArrayList<String> rowLabels, String systemType, String title) {
		int i;

		int rowLength = rowLabels.size();
		
		String[] headers = new String[2];
		headers[0] = systemType;
		headers[1] = "1 if kept";
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		for(i=0; i<rowLength; i++) {

			Object[] row = new Object[2];
			row[0] = rowLabels.get(i);
			row[1] = matrix[i];
			list.add(row);
		}

		createTabAndDisplayList(headers,list,title,false);
	}
	
	private void createSiteGrid(double[][] matrix, ArrayList<String> rowLabels, ArrayList<String> colLabels, String title) {
		int i;
		int j;

		int rowLength = rowLabels.size();
		int colLength = colLabels.size();
		
		String[] headers = new String[colLength + 1];
		headers[0] = "NonCentral System";
		for(i=0; i<colLength; i++)
			headers[i + 1] = colLabels.get(i);
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		for(i=0; i<rowLength; i++) {

			Object[] row = new Object[colLength + 1];
			row[0] = rowLabels.get(i);
			for(j=0; j<colLength; j++) {
				row[j + 1] = matrix[i][j];
			}

			list.add(row);
		}

		createTabAndDisplayList(headers,list,title,true);
	}
	
	private void createSiteGrid(String[][] matrix, ArrayList<String> rowLabels, ArrayList<String> colLabels, String title) {
		int i;
		int j;

		int rowLength = rowLabels.size();
		int colLength = colLabels.size();
		
		String[] headers = new String[colLength + 1];
		headers[0] = "NonCentral System";
		for(i=0; i<colLength; i++)
			headers[i + 1] = colLabels.get(i);
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		for(i=0; i<rowLength; i++) {

			Object[] row = new Object[colLength + 1];
			row[0] = rowLabels.get(i);
			for(j=0; j<colLength; j++) {
				row[j + 1] = matrix[i][j];
			}

			list.add(row);
		}

		createTabAndDisplayList(headers,list,title,true);
	}

	public Hashtable<String,Object> getSysCapHash() {
		Hashtable<String,Object> sysCapHash = new Hashtable<String,Object>();
		ArrayList<Hashtable<String,String>> sysHashList = new ArrayList<Hashtable<String,String>>();
		ArrayList<Hashtable<String,String>> capHashList = new ArrayList<Hashtable<String,String>>();
		
		int i;
		int numSys = sysList.size();
		for(i = 0; i < numSys; i++) {
			Hashtable<String, String> sysHash = new Hashtable<String, String>();
			sysHash.put("name",sysList.get(i));
			if(sysKeptArr[i] == 0)
				sysHash.put("ind", "Decommission");
			else
				sysHash.put("ind", "Modernize");
			sysHashList.add(sysHash);
		}
		
		numSys = centralSysList.size();
		for(i = 0; i < numSys; i++) {
			Hashtable<String, String> sysHash = new Hashtable<String, String>();
			sysHash.put("name",centralSysList.get(i));
			if(centralSysKeptArr[i] == 0)
				sysHash.put("ind", "Decommission");
			else
				sysHash.put("ind", "Modernize");
			sysHashList.add(sysHash);
		}
		
		int numCap = capList.size();
		for(i = 0; i < numCap; i++) {
			Hashtable<String, String> capHash = new Hashtable<String, String>();
			capHash.put("name", capList.get(i));
			capHash.put("ind", "Capability");
			capHashList.add(capHash);
		}
		
		sysCapHash.put("systemList",sysHashList);
		sysCapHash.put("capList",capHashList);
		
		return sysCapHash;
	}
	
	public Hashtable<String,Object> getOverviewInfoData() {

		Hashtable<String,Object> overviewInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> systemInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> budgetInfoHash = new Hashtable<String,Object>();
		
		int totalSys = sysList.size() + centralSysList.size();
		int totalKept = numSysKept + numCentralSysKept;
		systemInfoHash.put("beforeCount", totalSys);
		systemInfoHash.put("decommissionedCount", totalSys - totalKept);
		systemInfoHash.put("afterCount", totalKept);
		systemInfoHash.put("yearsToTransition",(double) Math.round(yearsToComplete * 1000)/1000);
		
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);

		budgetInfoHash.put("formattedCurrentSustVal", formatter.format(currSustainmentCost));
		budgetInfoHash.put("formattedFutureSustVal", formatter.format(futureSustainmentCost));
		budgetInfoHash.put("formattedCostVal", formatter.format(adjustedDeploymentCost));
		
		overviewInfoHash.put("systemInfo",systemInfoHash);
		overviewInfoHash.put("budgetInfo",budgetInfoHash);
		
		return overviewInfoHash;
	}
	
	public Hashtable<String,Object> getOverviewCostData() {
		String[] names = new String[]{"Year", "Build Cost","Cost Avoided"};
		ArrayList<Object []> list = new ArrayList<Object []>();
		int i;
		int startYear = 2017;
		for(i=0; i<years; i++) {
			Object[] row = new Object[3];
			row[0] = startYear + i;
			row[1] = deployCostPerYear[i];
			row[2] = costAvoidedPerYear[i];
			list.add(row);
		}
		
		ColumnChartPlaySheet ps = new ColumnChartPlaySheet();
		ps.setNames(names);
		ps.setList(list);
		ps.setDataHash(ps.processQueryData());
		return (Hashtable<String,Object>)ps.getData();
	}
	
	/**
	 * Gets the health grid for all systems selected OR all systems selected that support the given capability
	 * Includes whether those systems were sustained or or consolidated.
	 * @param capability String that is null if all systems for the overview page or a capability to filter the systems
	 * @return
	 */
	public Hashtable<String,Object> getHealthGrid(String capability) {
		
		if(sysKeptQueryString.isEmpty())
			makeSysKeptQueryString();
		
		String capabilityBindings;
		if(capability == null || capability.isEmpty())
			capabilityBindings = "";
		else {
			capabilityBindings = "BIND(<http://health.mil/ontologies/Concept/Capability/"+capability+"> AS ?Capability){?System <http://semoss.org/ontologies/Relation/Supports> ?Capability}";
		}
			
		String sysBindings = SysOptUtilityMethods.makeBindingString("System",sysList) + SysOptUtilityMethods.makeBindingString("System",centralSysList);
		
		String query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) (COALESCE(?SustainmentBud,0.0) AS ?SustainmentBudget) (IF(?System in (" + sysKeptQueryString + "), \"Sustained\", \"Consolidated\") AS ?Status) WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}" + capabilityBindings + "OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud}}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} } BINDINGS ?System {" + sysBindings + "}";

		HealthGridSheet ps = new HealthGridSheet();
		ps.setQuery(query);
		ps.setRDFEngine(systemEngine);
		ps.createData();
		return (Hashtable<String,Object>)ps.getData();
	}
	

	public Hashtable<String,Object> getOverviewSiteMapData(String capability) {
		double[] currSiteSustainCost;
		double[] futureSiteSustainCost;

		if(capability.equals("")) {
			currSiteSustainCost = calculateSiteSustainCost(systemSiteMatrix, maintenaceCosts, null, -1);
			currSiteSustainCost = SysOptUtilityMethods.addArrays(currSiteSustainCost, calculateHostSiteSustainCost(centralSystemSiteMatrix, centralSystemMaintenaceCosts, centralSystemNumSite, null, null, -1));

			futureSiteSustainCost = calculateSiteSustainCost(systemSiteResultMatrix, maintenaceCosts, null, -1); 
			futureSiteSustainCost = SysOptUtilityMethods.addArrays(futureSiteSustainCost, calculateHostSiteSustainCost(centralSystemSiteMatrix, centralSystemMaintenaceCosts, centralSystemNumSite, centralSysKeptArr, null, -1));
		}else {
			int capIndex = capList.indexOf(capability);
			
			currSiteSustainCost = calculateSiteSustainCost(systemSiteMatrix, maintenaceCosts, systemCapMatrix, capIndex);
			currSiteSustainCost = SysOptUtilityMethods.addArrays(currSiteSustainCost, calculateHostSiteSustainCost(centralSystemSiteMatrix, centralSystemMaintenaceCosts, centralSystemNumSite, null, centralSystemCapMatrix, capIndex));
		
			futureSiteSustainCost = calculateSiteSustainCost(systemSiteResultMatrix, maintenaceCosts, systemCapMatrix, capIndex);
			futureSiteSustainCost = SysOptUtilityMethods.addArrays(futureSiteSustainCost, calculateHostSiteSustainCost(centralSystemSiteMatrix, centralSystemMaintenaceCosts, centralSystemNumSite, centralSysKeptArr, centralSystemCapMatrix, capIndex));
		}

		double[] percentDiff = StatisticsUtilityMethods.calculatePercentDiff(currSiteSustainCost,futureSiteSustainCost);
		
		String[] names = new String[]{"DCSite", "lat", "lon", "Future Sustainment Cost", "Change in Sustainment Cost %"};
		ArrayList<Object []> list = new ArrayList<Object []>();
		int i;
		int numSites = siteList.size();
		for(i=0; i<numSites; i++) {
			Object[] row = new Object[5];
			row[0] = siteList.get(i);
			row[1] = siteLat[i];
			row[2] = siteLon[i];
			row[3] = futureSiteSustainCost[i];
			row[4] = (double) Math.round(percentDiff[i] * 1000)/1000;//positive number means it decreased in cost, negative means increased cost
			list.add(row);
		}
		
		OCONUSMapPlaySheet ps = new OCONUSMapPlaySheet();
		ps.setNames(names);
		ps.setList(list);
		ps.setDataHash(ps.processQueryData());
		return (Hashtable<String,Object>)ps.getData();
	}

	public Hashtable<String,Object> getSystemInfoData(String system, Boolean isModernizedPage) {
	
		Hashtable<String,Object> systemInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> dataBLUInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> descriptionInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> budgetInfoHash = new Hashtable<String,Object>();

		int sysIndex;
		int bluCount;
		int dataCount;
		String hosting;
		double sysCurrSustainCost;
		double sysFutureSustainCost;
		double sysDeployCost;
		int sysNumSites;
		
		if(sysList.contains(system)) {//if noncentral system
			sysIndex = sysList.indexOf(system);

			bluCount = SysOptUtilityMethods.sumRow(systemBLUMatrix[sysIndex]);
			dataCount = SysOptUtilityMethods.sumRow(systemDataMatrix[sysIndex]);
			hosting = "Local";
			
			sysCurrSustainCost = maintenaceCosts[sysIndex] / centralDeploymentPer;
			if(isModernizedPage) {
				sysNumSites = (int)SysOptUtilityMethods.sumRow(systemSiteResultMatrix[sysIndex]);
				sysFutureSustainCost = maintenaceCosts[sysIndex] + sysNumSites * siteMaintenaceCosts[sysIndex];
				sysDeployCost = 0.0;
				int i;
				int numSites = siteList.size();
				for(i=0; i<numSites; i++) {
					sysDeployCost += systemSiteResultMatrix[sysIndex][i] * ((1 - systemSiteMatrix[sysIndex][i]) * siteDeploymentCosts[sysIndex] + (1+trainingPerc) * interfaceCostArr[sysIndex]);
				}
			}else {
				sysNumSites = (int)SysOptUtilityMethods.sumRow(systemSiteMatrix[sysIndex]);
				sysFutureSustainCost = 0;
				sysDeployCost = 0;
			}
			
		}else {//if central system
			sysIndex = centralSysList.indexOf(system);

			bluCount = SysOptUtilityMethods.sumRow(centralSystemBLUMatrix[sysIndex]);
			dataCount = SysOptUtilityMethods.sumRow(centralSystemDataMatrix[sysIndex]);
			hosting = "Central";
			
			sysCurrSustainCost = centralSystemMaintenaceCosts[sysIndex];
			
			sysNumSites = siteList.size();
			if(isModernizedPage) {
				sysFutureSustainCost = sysCurrSustainCost;
				sysDeployCost = (1+trainingPerc)*centralInterfaceCostArr[sysIndex];
			}else {
				sysFutureSustainCost = 0;
				sysDeployCost = 0;
			}
		}
		
		String downstreamQuery = "SELECT DISTINCT (COUNT(DISTINCT(?DownstreamSys)) AS ?NumDownstreamSys) WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + system + "> AS ?System){?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;}} GROUP BY ?System";
		String upstreamQuery = "SELECT DISTINCT (COUNT(DISTINCT(?UpstreamSys)) AS ?NumUpstreamSys) WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + system + "> AS ?System){?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;}} GROUP BY ?System";
		
		Object numDownstream = SysOptUtilityMethods.runSingleResultQuery(systemEngine, downstreamQuery);
		Object numUpstream = SysOptUtilityMethods.runSingleResultQuery(systemEngine, upstreamQuery);
		if(numDownstream == null)
			numDownstream = 0;
		if(numUpstream == null)
			numUpstream = 0;
		
		String atoQuery = "SELECT DISTINCT ?ATO WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + system + "> AS ?System){?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ATO}}";

		Object ato = SysOptUtilityMethods.runSingleResultQuery(systemEngine, atoQuery);
		if(ato == null)
			ato = "TBD";
		else if(((String)ato).endsWith("00.000Z"))
				ato = ((String)ato).substring(0,((String)ato).indexOf("T"));
		
		String descriptionQuery = "SELECT DISTINCT ?Description WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + system + "> AS ?System){?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}}";

		Object description = SysOptUtilityMethods.runSingleResultQuery(systemEngine, descriptionQuery);
		if(description == null)
			description = "TBD";
		else
			description = ((String) description).replaceAll("_"," ");

		dataBLUInfoHash.put("bluCount", bluCount);
		dataBLUInfoHash.put("dataCount", dataCount);
		dataBLUInfoHash.put("upstreamCount", numUpstream);
		dataBLUInfoHash.put("downstreamCount", numDownstream);
		dataBLUInfoHash.put("hosting", hosting);
		
		descriptionInfoHash.put("atoDate", ato);
		descriptionInfoHash.put("decomCount", sysNumSites);
		descriptionInfoHash.put("systemDesc", description);
		
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);

		budgetInfoHash.put("formattedCurrentSustVal", formatter.format(sysCurrSustainCost));
		budgetInfoHash.put("formattedFutureSustVal", formatter.format(sysFutureSustainCost));
		budgetInfoHash.put("formattedCostVal", formatter.format(sysDeployCost));
		
		systemInfoHash.put("dataBluInfo",dataBLUInfoHash);
		systemInfoHash.put("decommissionInfo",descriptionInfoHash);
		systemInfoHash.put("budgetInfo",budgetInfoHash);

		return systemInfoHash;
	}
	
	public Hashtable<String,Object> getKeptSystemSiteMapData(String system) {
		String[] names = new String[]{"DCSite", "lat", "lon", "Status"};
		ArrayList<Object []> list = new ArrayList<Object []>();

		int i;
		int numSites = siteList.size();

		int sysIndex;

		if(sysList.contains(system)) {//if noncentral system

			sysIndex = sysList.indexOf(system);
			for(i=0; i<numSites; i++) {
				if(systemSiteRecMatrix[sysIndex][i]!=null) {
					Object[] row = new Object[5];
					row[0] = siteList.get(i);
					row[1] = siteLat[i];
					row[2] = siteLon[i];
					row[3] = systemSiteRecMatrix[sysIndex][i];
					list.add(row);
				}
			}

		}else {//if central system
			sysIndex = centralSysList.indexOf(system);
			
			for(i=0; i<numSites; i++) {
					Object[] row = new Object[5];
					row[0] = siteList.get(i);
					row[1] = siteLat[i];
					row[2] = siteLon[i];
					if(centralSystemSiteMatrix[sysIndex][i]==1) {
						row[3] = "HOSTED";
					}else {
						row[3] = "ACCESSIBLE";
					}
					list.add(row);
				}
		}

		OCONUSMapPlaySheet ps = new OCONUSMapPlaySheet();
		ps.setNames(names);
		ps.setList(list);
		ps.setDataHash(ps.processQueryData());
		return (Hashtable<String,Object>)ps.getData();
		
	}

	public Hashtable<String,Object> getDecomSystemSiteMapData(String system) {
		String[] names = new String[]{"DCSite", "lat", "lon", "Status","Systems at Site"};
		ArrayList<Object []> list = new ArrayList<Object []>();

		int i;
		int numSites = siteList.size();

		int sysIndex;

		if(sysList.contains(system)) {//if noncentral system

			sysIndex = sysList.indexOf(system);
			for(i=0; i<numSites; i++) {
				if(systemSiteMatrix[sysIndex][i] == 1) {
					Object[] row = new Object[5];
					row[0] = siteList.get(i);
					row[1] = siteLat[i];
					row[2] = siteLon[i];
					row[3] = "CONSOLIDATED";
					row[4] = getSystemsAtSiteList(i);
					list.add(row);
				}
			}

		}else {//if central system
			sysIndex = centralSysList.indexOf(system);
			for(i=0; i<numSites; i++) {
				Object[] row = new Object[5];
				row[0] = siteList.get(i);
				row[1] = siteLat[i];
				row[2] = siteLon[i];
				if(centralSystemSiteMatrix[sysIndex][i]==1) {
					row[3] = "HOSTED";
				}else {
					row[3] = "ACCESSIBLE";
				}
				row[4] = getSystemsAtSiteList(i);
				list.add(row);
			}

		}

		OCONUSMapPlaySheet ps = new OCONUSMapPlaySheet();
		ps.setNames(names);
		ps.setList(list);
		ps.setDataHash(ps.processQueryData());
		return (Hashtable<String,Object>)ps.getData();
		
	}
	
	private ArrayList<String> getSystemsAtSiteList(int siteIndex) {

		ArrayList<String> sysAtSiteList = new ArrayList<String>();
		int i;
		int numSys = sysList.size();
		for(i=0; i<numSys; i++) {
			if(systemSiteResultMatrix[i][siteIndex] == 1)
				sysAtSiteList.add(sysList.get(i));
		}
		
		numSys = centralSysList.size();
		for(i=0; i<numSys; i++) {
			if(centralSysKeptArr[i] == 1)
				sysAtSiteList.add("*"+centralSysList.get(i));
		}
		
		return sysAtSiteList;
	}
	
	public Hashtable<String,Object> getDecomSystemCoverageData(String system) {

		Hashtable<String,Object> systemCoverageHash = new Hashtable<String,Object>();
		Hashtable<String, Hashtable<String,Set<String>>> dataHash = new Hashtable<String, Hashtable<String,Set<String>>> ();
		Set<String> dataProvided;
		Set<String> bluProvided; 
		int sysIndex;
		
		if(sysList.contains(system)) {//if noncentral system
			sysIndex = sysList.indexOf(system);
			
			dataProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(dataList, systemDataMatrix[sysIndex]));
			bluProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(bluList, systemBLUMatrix[sysIndex]));

			dataProvided = processCoverage(dataHash, dataProvided, systemDataMatrix[sysIndex], systemDataMatrix, sysKeptArr, sysList, dataList, sysIndex, "Data");
			dataProvided = processCoverage(dataHash, dataProvided, systemDataMatrix[sysIndex], centralSystemDataMatrix, centralSysKeptArr, centralSysList, dataList, -1, "Data");
			bluProvided = processCoverage(dataHash, bluProvided, systemBLUMatrix[sysIndex], systemBLUMatrix, sysKeptArr, sysList, bluList, sysIndex, "BLU");
			bluProvided = processCoverage(dataHash, bluProvided, systemBLUMatrix[sysIndex], centralSystemBLUMatrix, centralSysKeptArr, centralSysList, bluList, -1, "BLU");
			
		}else {//if central system
			sysIndex = centralSysList.indexOf(system);
			
			dataProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(dataList, centralSystemDataMatrix[sysIndex]));
			bluProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(bluList, centralSystemBLUMatrix[sysIndex]));
			
			dataProvided = processCoverage(dataHash, dataProvided, centralSystemDataMatrix[sysIndex], systemDataMatrix, sysKeptArr, sysList, dataList, -1, "Data");
			dataProvided = processCoverage(dataHash, dataProvided, centralSystemDataMatrix[sysIndex], centralSystemDataMatrix, centralSysKeptArr, centralSysList, dataList, sysIndex, "Data");
			bluProvided = processCoverage(dataHash, bluProvided, centralSystemBLUMatrix[sysIndex], systemBLUMatrix, sysKeptArr, sysList, bluList, -1, "BLU");
			bluProvided = processCoverage(dataHash, bluProvided, centralSystemBLUMatrix[sysIndex], centralSystemBLUMatrix, centralSysKeptArr, centralSysList, bluList, sysIndex, "BLU");
		}
		
		Hashtable<String,Set<String>> uncoveredFuncHash;
		uncoveredFuncHash = new Hashtable<String,Set<String>>();
		uncoveredFuncHash.put("Data", dataProvided);
		uncoveredFuncHash.put("BLU", bluProvided);
		dataHash.put("Uncovered", uncoveredFuncHash);
		
		systemCoverageHash.put("data",dataHash);

		return systemCoverageHash;
	}
	
	
	public Hashtable<String,Object> getKeptSystemCoverageData(String system) {

		Hashtable<String,Object> systemCoverageHash = new Hashtable<String,Object>();
		Hashtable<String, Hashtable<String,Set<String>>> dataHash = new Hashtable<String, Hashtable<String,Set<String>>> ();
		Set<String> dataProvided;
		Set<String> bluProvided; 
		int sysIndex;
		
		if(sysList.contains(system)) {//if noncentral system
			sysIndex = sysList.indexOf(system);
			
			dataProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(dataList, systemDataMatrix[sysIndex]));
			bluProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(bluList, systemBLUMatrix[sysIndex]));

			dataProvided = processCoverage(dataHash, dataProvided, systemDataMatrix[sysIndex], capDataMatrix, null, capList, dataList, -1, "Data");
			bluProvided = processCoverage(dataHash, bluProvided, systemBLUMatrix[sysIndex], capBLUMatrix, null, capList, bluList, -1, "BLU");

		}else {//if central system
			sysIndex = centralSysList.indexOf(system);
			
			dataProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(dataList, centralSystemDataMatrix[sysIndex]));
			bluProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(bluList, centralSystemBLUMatrix[sysIndex]));
			
			dataProvided = processCoverage(dataHash, dataProvided, centralSystemDataMatrix[sysIndex], capDataMatrix, null, capList, dataList, -1, "Data");
			bluProvided = processCoverage(dataHash, bluProvided, centralSystemBLUMatrix[sysIndex], capBLUMatrix, null, capList, bluList, -1, "BLU");
		}
		
		Hashtable<String,Set<String>> uncoveredFuncHash;
		uncoveredFuncHash = new Hashtable<String,Set<String>>();
		uncoveredFuncHash.put("Data", dataProvided);
		uncoveredFuncHash.put("BLU", bluProvided);
		dataHash.put("Uncovered", uncoveredFuncHash);
	
		
		systemCoverageHash.put("data",dataHash);

		return systemCoverageHash;
	}

	private Set<String> processCoverage(Hashtable<String, Hashtable<String,Set<String>>> hashtable,Set<String> uncoveredSet, int[] desiredFunc, int[][] systemFuncMatrix, double[] sysKeptArr, ArrayList<String> sysList, ArrayList<String> funcList, int ignoreIndex, String dataOrBLU) {
		
		int i;
		int j;
		int numFunc = funcList.size();
		int numSys = sysList.size();
		String func;
		String system;

		for(i=0; i<numFunc; i++) {
			if(desiredFunc[i] == 1) {//system provides the functionality
				func = funcList.get(i);
				
				for(j=0; j<numSys; j++) {
					
					if(j!=ignoreIndex && systemFuncMatrix[j][i] == 1 && (sysKeptArr == null || sysKeptArr[j] == 1)) {//another system provides the same functionality and is kept
						system = sysList.get(j);
						Hashtable<String,Set<String>> systemFuncHash;
						if(hashtable.containsKey(system)) {
							systemFuncHash = hashtable.get(system);
						}else {
							systemFuncHash = new Hashtable<String,Set<String>>();
							systemFuncHash.put("Data", new HashSet<String>());
							systemFuncHash.put("BLU", new HashSet<String>());
							hashtable.put(system, systemFuncHash);
						}
						systemFuncHash.get(dataOrBLU).add(func);
						uncoveredSet.remove(func);
					}
				}

			}
			
		}
		return uncoveredSet;
	}	
	
	public Hashtable<String,Object> getCapabilityInfoData(String capability) {

		Hashtable<String,Object> capInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> dataBLUInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> budgetInfoHash = new Hashtable<String,Object>();
		
		int capIndex = capList.indexOf(capability);
		
		int bluCount = SysOptUtilityMethods.sumRow(capBLUMatrix[capIndex]);
		int dataCount = SysOptUtilityMethods.sumRow(capDataMatrix[capIndex]);
		
		int i;
		int numSystems = sysList.size();
		int numCentralSystems = centralSysList.size();
		int numSysBefore = 0;
		int numSysAfter = 0;
		for(i=0; i<numSystems; i++) {
			numSysBefore += systemCapMatrix[i][capIndex];
			numSysAfter += systemCapMatrix[i][capIndex] * sysKeptArr[i];
		}
		
		for(i=0; i<numCentralSystems; i++) {
			numSysBefore += centralSystemCapMatrix[i][capIndex];
			numSysAfter += centralSystemCapMatrix[i][capIndex] * centralSysKeptArr[i];
		}

		double capCurrSustainCost = 0.0;
		double capFutureSustainCost = 0.0;
		int sysNumSites;
		
		for(i=0; i<numSystems; i++) {
			if(systemCapMatrix[i][capIndex] == 1) {
				capCurrSustainCost += maintenaceCosts[i] / centralDeploymentPer;
				sysNumSites = (int)SysOptUtilityMethods.sumRow(systemSiteResultMatrix[i]);
				capFutureSustainCost += sysKeptArr[i] * (maintenaceCosts[i] + sysNumSites * siteMaintenaceCosts[i]);
			}
		}
		
		for(i=0; i<numCentralSystems; i++) {

			if(centralSystemCapMatrix[i][capIndex] == 1) {
				capCurrSustainCost += centralSystemMaintenaceCosts[i];
				capFutureSustainCost += centralSysKeptArr[i] * centralSystemMaintenaceCosts[i];
			}
		}

		String descriptionQuery = "SELECT DISTINCT ?Description WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/" + capability + "> AS ?Capability){?Capability <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}}";

		Object description = SysOptUtilityMethods.runSingleResultQuery(systemEngine, descriptionQuery);
		if(description == null)
			description = "TBD";
		else
			description = ((String) description).replaceAll("_"," ");

		dataBLUInfoHash.put("bluCount", bluCount);
		dataBLUInfoHash.put("dataCount", dataCount);
		dataBLUInfoHash.put("beforeCount", numSysBefore);
		dataBLUInfoHash.put("afterCount", numSysAfter);
		
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);

		budgetInfoHash.put("formattedCurrentSustVal", formatter.format(capCurrSustainCost));
		budgetInfoHash.put("formattedFutureSustVal", formatter.format(capFutureSustainCost));
		
		capInfoHash.put("dataBluInfo",dataBLUInfoHash);
		capInfoHash.put("capabilityDesc",description);
		capInfoHash.put("budgetInfo",budgetInfoHash);

		return capInfoHash;
	}
	
	public Hashtable<String,Object> getCapabilityCoverageData(String capability) {

		Hashtable<String,Object> capabilityCoverageHash = new Hashtable<String,Object>();
		Hashtable<String, Hashtable<String,Set<String>>>  dataHash = new Hashtable<String, Hashtable<String,Set<String>>> ();

		int capIndex = capList.indexOf(capability);
		
		Set<String> dataProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(dataList, capDataMatrix[capIndex]));
		Set<String> bluProvided = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(bluList, capBLUMatrix[capIndex]));

		dataProvided = processCoverage(dataHash, dataProvided, capDataMatrix[capIndex], systemDataMatrix, sysKeptArr, sysList, dataList, -1, "Data");
		dataProvided = processCoverage(dataHash, dataProvided, capDataMatrix[capIndex], centralSystemDataMatrix, centralSysKeptArr, centralSysList, dataList, -1, "Data");
		bluProvided = processCoverage(dataHash, bluProvided, capBLUMatrix[capIndex], systemBLUMatrix, sysKeptArr, sysList, bluList, -1, "BLU");
		bluProvided = processCoverage(dataHash, bluProvided, capBLUMatrix[capIndex], centralSystemBLUMatrix, centralSysKeptArr, centralSysList, bluList, -1, "BLU");

		Hashtable<String,Set<String>> uncoveredFuncHash;
		uncoveredFuncHash = new Hashtable<String,Set<String>>();
		uncoveredFuncHash.put("Data", dataProvided);
		uncoveredFuncHash.put("BLU", bluProvided);
		dataHash.put("Uncovered", uncoveredFuncHash);
		
		capabilityCoverageHash.put("data",dataHash);

		return capabilityCoverageHash;
	}
	
	private void makeSysKeptQueryString() {
		int i;
		int numSys = sysList.size();
		for(i = 0; i<numSys; i++)
			if(sysKeptArr[i] == 1)
				sysKeptQueryString+= "<http://health.mil/ontologies/Concept/System/"+sysList.get(i)+">,";

		numSys = centralSysList.size();
		for(i = 0; i<numSys; i++)
			if(centralSysKeptArr[i] == 1)
				sysKeptQueryString+= "<http://health.mil/ontologies/Concept/System/"+centralSysList.get(i)+">,";

		if(sysKeptQueryString.length() > 0)
			sysKeptQueryString = sysKeptQueryString.substring(0,sysKeptQueryString.length() - 1);
	}
	
	private void printMessage(String message) {
		if(playSheet == null)
			System.out.println(message);
		else
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n" + message);
	}

	@Override
	public String[] getVariables() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAlgoName() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
