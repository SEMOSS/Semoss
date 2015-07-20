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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
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

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.engine.api.IEngine;
import prerna.ui.components.playsheets.OCONUSMapPlaySheet;
import prerna.ui.components.specific.tap.HealthGridSheet;
import prerna.ui.components.specific.tap.SysSiteOptGraphFunctions;
import prerna.ui.components.specific.tap.SysSiteOptPlaySheet;
import prerna.util.Utility;

/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class SysSiteOptimizer extends UnivariateOpt {
	
	private static final Logger LOGGER = LogManager.getLogger(SysSiteOptimizer.class.getName());
	
	private IEngine systemEngine, costEngine, siteEngine;
	
	private ArrayList<String> localSysList, centralSysList, dataList, bluList, siteList, capList;
	
	//user must select these
	private double budgetForYear;
	private Boolean useDHMSMFunctionality;//true if data/blu list made from dhmsm capabilities. False if from the systems
	private Boolean isOptimizeBudget;
	private String type;

	//user can change these as advanced settings //TODO make absConvergence a perc of num years and budget 
	private double centralPercOfBudget, trainingPerc, relConvergence, absConvergence;
	
	//user should not change these
	private double deploymentFactor = 5;
	private double interfacePercOfDeployment = 0.075;
	
	//generated data stores based off of the users selections
	//local systems
	private int[][] localSystemDataMatrix, localSystemBLUMatrix;
	private int[] localSystemIsTheaterArr, localSystemIsGarrisonArr;
	private Integer[] localSystemIsModArr, localSystemIsDecomArr;
	private double[] localSystemMaintenanceCostArr, localSystemSiteMaintenaceCostArr, localSystemSiteDeploymentCostArr, localSystemSiteInterfaceCostArr;
	private double[][] localSystemSiteMatrix;
	
	//central systems
	private int[][] centralSystemDataMatrix, centralSystemBLUMatrix;
	private int[] centralSystemIsTheaterArr, centralSystemIsGarrisonArr;
	private Integer[] centralSystemIsModArr, centralSystemIsDecomArr;
	private double[] centralSystemMaintenanceCostArr, centralSystemInterfaceCostArr;
	private double[] centralSystemNumSiteArr;
	private double[][] centralSystemSiteMatrix;

	//capability
//	private int[][] localSystemCapMatrix, centralSystemCapMatrix;
//	private int[][] capBLUMatrix, capDataMatrix;
	
	//results of the algorithm
	private SysSiteOptFunction optFunc;
	
	private double[] localSysKeptArr, centralSysKeptArr;
	private double[][] localSystemSiteResultMatrix;
	private String[][] localSystemSiteRecMatrix;
	
	private double[] siteLat, siteLon;
	private double yearsToComplete;
	private double currentSustainmentCost, futureSustainmentCost;
	private double adjustedDeploymentCost, adjustedTotalSavings, roi, irr;
	
	private double[] costAvoidedPerYearArr;
	public double[] deployCostPerYearArr, futureCostPerYearArr, currCostPerYearArr, cummDeployCostArr, cummCostAvoidedArr;
	public double[][] balanceArr;
	
	private final String RECOMMENDED_SUSTAIN = "Recommended Sustain";
	private final String RECOMMENDED_CONSOLIDATION = "Recommended Consolidation";
	private final String SUSTAINED_SYSTEMS = "Sustained Systems";
	private final String CONSOLIDATED_SYSTEMS = "Consolidated Systems";

	private final String SUSTAINED_HOST_SITE = "Sustained Host Site";
	private final String PREVIOUSLY_HOSTED_SITE = "Previoulsy Hosted Site";
	private final String SUSTAINED_ACCESSIBLE_SITE = "Sustained Accessible Site";
	private final String DEPLOYED_ACCESSIBLE_SITE = "Deployed Accessible Site";
	private final String PREVIOUSLY_ACCESSIBLE_SITE = "Previoulsy Accessible Site";
	
	public int startYear = 2015;
	
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
		optimizeSystemsAtSites(isOptimizeBudget, localSystemSiteMatrix, currentSustainmentCost, budgetForYear, maxYears);
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
	
	/**
	 * 
	 * @param budgetForYear
	 * @param years
	 * @param infRate, disRate in decimals (NOT percents)
	 * @param trainingPerc in decimal (NOT percent)
	 * @param hourlyRate
	 * @param noOfPts
	 * @param relConvergence in decimal (NOT percent)
	 * @param absConvergence
	 */
	public void setVariables(double budgetForYear, int years, double infRate, double disRate, double centralPercOfBudget, double trainingPerc,double hourlyCost, int noOfPts, double relConvergence, double absConvergence) {
		this.budgetForYear = budgetForYear;
		this.maxYears = years;
		this.infRate = infRate;
		this.disRate = disRate;
		this.centralPercOfBudget = centralPercOfBudget;
		this.trainingPerc = trainingPerc;
		this.hourlyCost = hourlyCost;
		this.noOfPts = noOfPts;
		this.relConvergence = relConvergence;
		this.absConvergence = absConvergence;
	}
	
	public void setSysList(ArrayList<String> sysList, ArrayList<String> modList, ArrayList<String> decomList) {
		
		String centralSysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> 'Y'}{?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0)} ORDER BY ?System";
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
		
		this.localSysList = sysList;
		
		int numLocalSys = localSysList.size();
		int numCentralSys = centralSysList.size();
		
		localSystemIsModArr = new Integer[numLocalSys];
		centralSystemIsModArr = new Integer[numCentralSys];
		
		localSystemIsDecomArr = new Integer[numLocalSys];
		centralSystemIsDecomArr = new Integer[numCentralSys];

		for(i=0; i<numLocalSys; i++) {
			if(modList.contains(localSysList.get(i))) {
				localSystemIsModArr[i] = 1;
				localSystemIsDecomArr[i] = 0;
			}else if(decomList.contains(localSysList.get(i))) {
				localSystemIsModArr[i] = 0;
				localSystemIsDecomArr[i] = 1;				
			}else {
				localSystemIsModArr[i] = 0;
				localSystemIsDecomArr[i] = 0;				
			}
		}
		
		for(i=0; i<numCentralSys; i++) {
			if(modList.contains(centralSysList.get(i))) {
				centralSystemIsModArr[i] = 1;
				centralSystemIsDecomArr[i] = 0;
			}else if(decomList.contains(centralSysList.get(i))) {
				centralSystemIsModArr[i] = 0;
				centralSystemIsDecomArr[i] = 1;				
			}else {
				centralSystemIsModArr[i] = 0;
				centralSystemIsDecomArr[i] = 0;				
			}
		}

		printMessage("Not Central Systems are..." + SysOptUtilityMethods.createPrintString(localSysList));
		printMessage("Central Systems are..." + SysOptUtilityMethods.createPrintString(centralSysList));
	}
	
	public void setSysHashList(ArrayList<Hashtable<String, String>> sysHashList) {
		localSysList = new ArrayList<String>();
		centralSysList = new ArrayList<String>();
		
		String centralSysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> 'Y'}{?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0)} ORDER BY ?System";
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
		
		localSystemIsModArr = new Integer[numNonCentral];
		centralSystemIsModArr = new Integer[numCentral];
		
		localSystemIsDecomArr = new Integer[numNonCentral];
		centralSystemIsDecomArr = new Integer[numCentral];		
		
		int centralIndex = 0;
		int nonCentralIndex = 0;
		for(i = 0; i < numSysHash; i++) {
			Hashtable<String, String> sysHash = sysHashList.get(i);
			String sys = sysHash.get("name");
			String status = sysHash.get("ind");
			
			if(allCentralSys.contains(sys)) {
				
				centralSysList.add(sys);
				
				if(status.equals("Modernize")) {
					centralSystemIsModArr[centralIndex] = 1;
					centralSystemIsDecomArr[centralIndex] = 0;
				} else if(status.equals("Decommission")) {
					centralSystemIsModArr[centralIndex] = 0;
					centralSystemIsDecomArr[centralIndex] = 1;
				} else {
					centralSystemIsModArr[centralIndex] = 0;
					centralSystemIsDecomArr[centralIndex] = 0;
				}
				
				centralIndex++;
			
			}else {
				
				localSysList.add(sys);
				
				if(status.equals("Modernize")) {
					localSystemIsModArr[nonCentralIndex] = 1;
					localSystemIsDecomArr[nonCentralIndex] = 0;	
				} else if(status.equals("Decommission")) {
					localSystemIsModArr[nonCentralIndex] = 0;
					localSystemIsDecomArr[nonCentralIndex] = 1;
				} else {
					localSystemIsModArr[nonCentralIndex] = 0;
					localSystemIsDecomArr[nonCentralIndex] = 0;
				}
				
				nonCentralIndex++;
			}
		}
		
		printMessage("Not Central Systems are..." + SysOptUtilityMethods.createPrintString(localSysList));
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

		String sysBindings = "{" + SysOptUtilityMethods.makeBindingString("System",localSysList) + SysOptUtilityMethods.makeBindingString("System",centralSysList) + "}";
		
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
		resFunc.setHourlyRate(hourlyCost);
		resFunc.setSysSiteLists(localSysList, dataList, bluList, siteList, capList);
		resFunc.setEngines(systemEngine, costEngine, siteEngine);
		resFunc.fillSysSiteOptDataStores(true);
		
//		capBLUMatrix = resFunc.capabilityBLUMatrix;
//		capDataMatrix = resFunc.capabilityDataMatrix;
		
		localSystemDataMatrix = resFunc.systemDataMatrix;
		localSystemBLUMatrix = resFunc.systemBLUMatrix;
		localSystemSiteMatrix = resFunc.systemSiteMatrix;
//		localSystemCapMatrix = resFunc.systemCapabilityMatrix;
		
		localSystemIsTheaterArr = resFunc.systemTheater;
		localSystemIsGarrisonArr = resFunc.systemGarrison;

		double[] systemSustainmentBudget = resFunc.systemCostOfMaintenance;
		double[] systemNumOfSites = resFunc.systemNumOfSites;

		int i;
		int sysLength = localSysList.size();
		localSystemMaintenanceCostArr = new double[sysLength];
		localSystemSiteMaintenaceCostArr = new double[sysLength];
		localSystemSiteDeploymentCostArr = new double[sysLength];
		currentSustainmentCost = 0.0;
		for(i=0; i<sysLength; i++) {
			
			double sysBudget = systemSustainmentBudget[i];
			double siteMaintenance;
			
			if(systemNumOfSites[i]==0) {
				localSystemMaintenanceCostArr[i] = sysBudget;
				siteMaintenance = (1 - centralPercOfBudget) * sysBudget;
			}else {
				localSystemMaintenanceCostArr[i] = centralPercOfBudget * sysBudget;
				siteMaintenance = (1 - centralPercOfBudget) * sysBudget / systemNumOfSites[i];
			}
			
			localSystemSiteMaintenaceCostArr[i] = siteMaintenance;
			localSystemSiteDeploymentCostArr[i] = siteMaintenance * deploymentFactor;

			currentSustainmentCost+=sysBudget;
		}
		
		
		localSystemSiteInterfaceCostArr = resFunc.systemCostOfDataConsumeArr;
		
		resFunc.setSysSiteLists(centralSysList,dataList,bluList,siteList, capList);
		resFunc.fillSysSiteOptDataStores(false);
		
		centralSystemDataMatrix = resFunc.systemDataMatrix;
		centralSystemBLUMatrix = resFunc.systemBLUMatrix;
		centralSystemSiteMatrix = resFunc.centralSystemSiteMatrix;
		centralSystemNumSiteArr = resFunc.centralSystemNumSite;

//		centralSystemCapMatrix = resFunc.systemCapabilityMatrix;
		centralSystemIsTheaterArr = resFunc.systemTheater;
		centralSystemIsGarrisonArr = resFunc.systemGarrison;

		centralSystemMaintenanceCostArr = resFunc.systemCostOfMaintenance;
		
		sysLength = centralSysList.size();
		for(i=0; i<sysLength; i++) {
			currentSustainmentCost += centralSystemMaintenanceCostArr[i];
		}
		
		centralSystemInterfaceCostArr = resFunc.systemCostOfDataConsumeArr;

		sysLength = localSysList.size();
		for(i=0; i<sysLength; i++)
			if(localSystemSiteInterfaceCostArr[i] != 0)
				localSystemSiteInterfaceCostArr[i] = interfacePercOfDeployment*localSystemSiteDeploymentCostArr[i];
		
		sysLength = centralSysList.size();
		for(i=0; i<sysLength; i++) 
			if(centralSystemInterfaceCostArr[i] != 0)
				centralSystemInterfaceCostArr[i] = centralSystemMaintenanceCostArr[i] * deploymentFactor * interfacePercOfDeployment;
		
		resFunc.fillSiteLatLon();
		siteLat = resFunc.siteLat;
		siteLon = resFunc.siteLon;
	}
	
	private void optimizeSystemsAtSites(Boolean isOptimizeBudget, double[][] systemSiteMatrix, double currSustainmentCost, double budgetForYear, int years) {
		
		if(!isOptimizeBudget) {
			optFunc = new SysSiteIRROptFunction();
		} else if(type.equals("Savings")) {
			optFunc = new SysSiteSavingsOptFunction();
		} else if(type.equals("ROI")) {
			optFunc = new SysSiteROIOptFunction();
		} else if(type.equals("IRR")) {
			optFunc = new SysSiteIRROptFunction();
		} else {
			printMessage("OPTIMIZATION TYPE DOES NOT EXIST");
			return;
		}
		optFunc.setPlaySheet(playSheet);
		optFunc.setVariables(localSystemDataMatrix, localSystemBLUMatrix, localSystemIsTheaterArr, localSystemIsGarrisonArr, localSystemIsModArr, localSystemIsDecomArr, localSystemMaintenanceCostArr, localSystemSiteMaintenaceCostArr, localSystemSiteDeploymentCostArr, localSystemSiteInterfaceCostArr, localSystemSiteMatrix, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemIsTheaterArr, centralSystemIsGarrisonArr, centralSystemIsModArr, centralSystemIsDecomArr, centralSystemMaintenanceCostArr, centralSystemInterfaceCostArr, trainingPerc, currentSustainmentCost, budgetForYear, years, infRate, disRate);

		optFunc.value(budgetForYear * years);
		futureSustainmentCost = optFunc.getFutureSustainmentCost();
		if(isOptimizeBudget && futureSustainmentCost!=currSustainmentCost) {
			UnivariateOptimizer optimizer = new BrentOptimizer(relConvergence, budgetForYear * years * absConvergence);
	
			RandomGenerator rand = new Well1024a(500);
			MultiStartUnivariateOptimizer multiOpt = new MultiStartUnivariateOptimizer(optimizer, noOfPts, rand);
			UnivariateObjectiveFunction objF = new UnivariateObjectiveFunction(optFunc);
			SearchInterval search = new SearchInterval(0, budgetForYear * years, budgetForYear * years);
			MaxEval eval = new MaxEval(10);
			
			OptimizationData[] data = new OptimizationData[] { search, objF, GoalType.MAXIMIZE, eval};
			try {
				UnivariatePointValuePair pair = multiOpt.optimize(data);
				optFunc = new SysSiteIRROptFunction();
				optFunc.setPlaySheet(playSheet);
				optFunc.setVariables(localSystemDataMatrix, localSystemBLUMatrix, localSystemIsTheaterArr, localSystemIsGarrisonArr, localSystemIsModArr, localSystemIsDecomArr, localSystemMaintenanceCostArr, localSystemSiteMaintenaceCostArr, localSystemSiteDeploymentCostArr, localSystemSiteInterfaceCostArr, localSystemSiteMatrix, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemIsTheaterArr, centralSystemIsGarrisonArr, centralSystemIsModArr, centralSystemIsDecomArr, centralSystemMaintenanceCostArr, centralSystemInterfaceCostArr, trainingPerc, currentSustainmentCost, budgetForYear, years, infRate, disRate);
				optFunc.value(pair.getPoint());
				
			} catch (TooManyEvaluationsException fee) {
				printMessage("Too many evalutions");
			}
		}
	
		localSysKeptArr = optFunc.getSysKeptArr();
		centralSysKeptArr = optFunc.getCentralSysKeptArr();
		localSystemSiteResultMatrix = optFunc.getSystemSiteResultMatrix();
		
		futureSustainmentCost = optFunc.getFutureSustainmentCost();
		adjustedTotalSavings = optFunc.getAdjustedTotalSavings();
		adjustedDeploymentCost = optFunc.getAdjustedDeploymentCost();
		roi = optFunc.getROI();
		irr = optFunc.getIRR();
		
		yearsToComplete = optFunc.getYearsToComplete();
	
		localSystemSiteRecMatrix = calculateSiteRecMatrix(this.localSystemSiteMatrix, localSystemSiteResultMatrix);
		
		double mu = (1 + infRate / 100) / (1 + disRate / 100);
		
		deployCostPerYearArr = SysOptUtilityMethods.calculateAdjustedDeploymentCostArr(mu, yearsToComplete, false, years, budgetForYear);
		currCostPerYearArr = SysOptUtilityMethods.calculateAdjustedDeploymentCostArr(mu,yearsToComplete, true, years, currSustainmentCost);
		futureCostPerYearArr =  SysOptUtilityMethods.calculateAdjustedSavingsArr(mu, yearsToComplete, years, futureSustainmentCost);
		costAvoidedPerYearArr =  SysOptUtilityMethods.calculateAdjustedSavingsArr(mu, yearsToComplete, years, currSustainmentCost - futureSustainmentCost);
		cummDeployCostArr= SysOptUtilityMethods.calculateCummulativeArr(deployCostPerYearArr);
		cummCostAvoidedArr = SysOptUtilityMethods.calculateCummulativeArr(costAvoidedPerYearArr);
		
		balanceArr  = new double[maxYears+1][2];
		balanceArr[0][0]=startYear;
		balanceArr[0][1]=0;
		for (int i=0; i<maxYears;i++)
		{	
			balanceArr[i+1][0]=startYear+i+1;
			balanceArr[i+1][1]=cummCostAvoidedArr[i] - cummDeployCostArr[i];
		}
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
					siteRecMatrix[i][j] =  SUSTAINED_ACCESSIBLE_SITE;
				} else if(oldMatrix[i][j] == 1 && newMatrix[i][j] == 0) {
					siteRecMatrix[i][j] = PREVIOUSLY_ACCESSIBLE_SITE;
				} else if(oldMatrix[i][j] == 0 && newMatrix[i][j] == 1) {
					siteRecMatrix[i][j] = DEPLOYED_ACCESSIBLE_SITE;
				}
			}
		}
		
		return siteRecMatrix;
	}

	
	/**
	 * Desktop display
	 */
	public void display() {
		
		//output results to the console
		printMessage("**Curr Sustainment Cost " + currentSustainmentCost);
		printMessage("**Future Sustainment Cost " + futureSustainmentCost);
		printMessage("**Adjusted Deployment Cost " + adjustedDeploymentCost);
		
		int i;
		printMessage("**Year ....... Investment ....... CostAvoided: ");
		for(i = 0; i<maxYears; i++) {
			printMessage((i + 1) + "     ....... " + deployCostPerYearArr[i] + " ..... " + costAvoidedPerYearArr[i]);
		}
		
		if(type.equals("Savings")) {
			printMessage("**Adjusted Total Savings: " + adjustedTotalSavings);
		} else if(type.equals("ROI")) {
			printMessage("**ROI: " + (optFunc.getROI()*100) + "%");
		} else if(type.equals("IRR")) {
			if(optFunc.getIRR() == -1E-40) {
				printMessage("**IRR: " + "does not exist since no savings");
			} else if(optFunc.getIRR() == -1E-30 || optFunc.getIRR() == -1E-31) {
				printMessage("**IRR: " + "has problem with calculation");
			} else {
				printMessage("**IRR: " + (optFunc.getIRR()*100) + "%");
			}
		}
		if(currentSustainmentCost == futureSustainmentCost) {
			clearPlaysheet();
		}else {
			String netSavingsString = Utility.sciToDollar(adjustedTotalSavings);
			((SysSiteOptPlaySheet)playSheet).savingLbl.setText(netSavingsString);
			double roiRounded =Utility.round(roi * 100, 2);
			((SysSiteOptPlaySheet)playSheet).roiLbl.setText(Double.toString(roiRounded)+ "%");
			double irrRounded =Utility.round(irr * 100, 2);
			((SysSiteOptPlaySheet)playSheet).irrLbl.setText(Double.toString(irrRounded) + "%");
			double yearsToCompleteRounded = Utility.round(yearsToComplete, 2);
			((SysSiteOptPlaySheet)playSheet).timeTransitionLbl.setText(Double.toString(yearsToCompleteRounded) + " Years");
			String deployCostString = Utility.sciToDollar(adjustedDeploymentCost);
			((SysSiteOptPlaySheet)playSheet).costLbl.setText(deployCostString);
			
			int negBalanceYear = 0;
			for(i = 0; i < maxYears+1; i++) {
				if (balanceArr[i][1] < 0)
					negBalanceYear = i;
			}
			if(negBalanceYear>=maxYears) {
				((SysSiteOptPlaySheet)playSheet).bkevenLbl.setText("Beyond Max Time");
			} else if (negBalanceYear == 0) {
				((SysSiteOptPlaySheet)playSheet).bkevenLbl.setText("1 Year");
			} else {
				double amountInLastYear = balanceArr[negBalanceYear+1][1] - balanceArr[negBalanceYear][1];
				double fraction = (-balanceArr[negBalanceYear][1]) / amountInLastYear;
				double breakEven = Utility.round(negBalanceYear + fraction, 2);
				((SysSiteOptPlaySheet)playSheet).bkevenLbl.setText(Double.toString(breakEven) + " Years");
			}
	
			SysSiteOptGraphFunctions graphF = new SysSiteOptGraphFunctions();
			graphF.setOptimzer(this);
			Hashtable chartHash3 = graphF.createCostChart();
			Hashtable chartHash4 = graphF.createCumulativeSavings();
			Hashtable chartHash5 = graphF.createBreakevenGraph();
			
			((SysSiteOptPlaySheet)playSheet).tab3.callIt(chartHash3);
			((SysSiteOptPlaySheet)playSheet).tab4.callIt(chartHash4);
			((SysSiteOptPlaySheet)playSheet).tab5.callIt(chartHash5);
			playSheet.setGraphsVisible(true);
			
			//create additional tabs
			createSiteGrid(localSystemSiteMatrix, localSysList, siteList,"Current NonCentral Systems at Sites");
			createSiteGrid(localSystemSiteResultMatrix, localSysList, siteList,"Future NonCentral Systems at Sites");
			createSiteGrid(localSystemSiteRecMatrix, localSysList, siteList,"Changes for NonCentral Systems at Sites");
			
			createOverallGrid(centralSysKeptArr, centralSysList, "Central System", "Future Central Systems (Was central system sustained or consolidated?)");
			createOverallGrid(localSysKeptArr, localSysList, "NonCentral Systems", "Future NonCentral Systems (Was noncentral system sustained or consolidated?)");
			
			createCostGrid();
			createCentralCostGrid();
		}
	}
	
	/*
	 * Part of desktop display
	 */
	private void createCostGrid() {
		
		String[] headers = new String[6];
		headers[0] = "System";
		headers[1] = "Sustain Cost";
		headers[2] = "Interface Cost";
		headers[3] = "Site Maintain Cost";
		headers[4] = "Site Deploy Cost";
		headers[5] = RECOMMENDED_SUSTAIN + " or " + RECOMMENDED_CONSOLIDATION;
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		int i=0;
		int rowLength = localSysList.size();
		
		for(i = 0; i<rowLength; i++) {
			Object[] row = new Object[6];
			row[0] = localSysList.get(i);
			row[1] = localSystemMaintenanceCostArr[i];
			row[2] = localSystemSiteInterfaceCostArr[i];
			row[3] = localSystemSiteMaintenaceCostArr[i];
			row[4] = localSystemSiteDeploymentCostArr[i];
			if(localSysKeptArr[i] == 1)
				row[5] = RECOMMENDED_SUSTAIN;
			else 
				row[5] = RECOMMENDED_CONSOLIDATION;
			list.add(row);
		}
		createTabAndDisplayList(headers,list,"NonCentral System Costs",false);
	}
	
	/*
	 * Part of desktop display
	 */
	private void createCentralCostGrid() {
		
		String[] headers = new String[4];
		headers[0] = "Central System";
		headers[1] = "Sustain Cost";
		headers[2] = "Interface Cost";
		headers[3] = RECOMMENDED_SUSTAIN + " or " + RECOMMENDED_CONSOLIDATION;
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		int i=0;
		int rowLength = centralSysList.size();
		
		for(i = 0; i<rowLength; i++) {
			Object[] row = new Object[4];
			row[0] = centralSysList.get(i);
			row[1] = centralSystemMaintenanceCostArr[i];
			row[2] = centralSystemInterfaceCostArr[i];
			if(centralSysKeptArr[i] == 1)
				row[3] = RECOMMENDED_SUSTAIN;
			else 
				row[3] = RECOMMENDED_CONSOLIDATION;
			list.add(row);
		}
		createTabAndDisplayList(headers,list,"Central System Costs",false);
	}
	
	/*
	 * Part of desktop display
	 */
	private void createOverallGrid(double[] matrix, ArrayList<String> rowLabels, String systemType, String title) {
		int i;

		int rowLength = rowLabels.size();
		
		String[] headers = new String[2];
		headers[0] = systemType;
		headers[1] = RECOMMENDED_SUSTAIN + " or " + RECOMMENDED_CONSOLIDATION;
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		for(i=0; i<rowLength; i++) {

			Object[] row = new Object[2];
			row[0] = rowLabels.get(i);
			if(matrix[i] == 1)
				row[1] = RECOMMENDED_SUSTAIN;
			else
				row[1] = RECOMMENDED_CONSOLIDATION;
			list.add(row);
		}

		createTabAndDisplayList(headers,list,title,false);
	}
	
	/*
	 * Part of desktop display
	 */
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
				if(matrix[i][j] == 1)
					row[j + 1] = "X";
				else 
					row[j + 1] = "";
			}

			list.add(row);
		}

		createTabAndDisplayList(headers,list,title,true);
	}
	
	/*
	 * Part of desktop display
	 */
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

	/**
	 *
	 * @return
	 */
	public ArrayList<Hashtable<String,String>> getSysResultList() {
		ArrayList<Hashtable<String,String>> sysResultList = new ArrayList<Hashtable<String,String>>();
		
		int i;
		int numSys = localSysList.size();
		for(i = 0; i < numSys; i++) {
			Hashtable<String, String> sysHash = new Hashtable<String, String>();
			sysHash.put("name",localSysList.get(i));
			if(localSysKeptArr[i] == 0)
				sysHash.put("ind", RECOMMENDED_CONSOLIDATION);
			else
				sysHash.put("ind", RECOMMENDED_SUSTAIN);
			sysResultList.add(sysHash);
		}
		
		numSys = centralSysList.size();
		for(i = 0; i < numSys; i++) {
			Hashtable<String, String> sysHash = new Hashtable<String, String>();
			sysHash.put("name",centralSysList.get(i));
			if(centralSysKeptArr[i] == 0)
				sysHash.put("ind", RECOMMENDED_CONSOLIDATION);
			else
				sysHash.put("ind", RECOMMENDED_SUSTAIN);
			sysResultList.add(sysHash);
		}
		return sysResultList;
	}
	
	public Hashtable<String,Object> getOverviewInfoData() {

		Hashtable<String,Object> overviewInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> systemInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> budgetInfoHash = new Hashtable<String,Object>();
		
		int totalSys = localSysList.size() + centralSysList.size();
		double totalKept = SysOptUtilityMethods.sumRow(localSysKeptArr) + SysOptUtilityMethods.sumRow(centralSysKeptArr);
		
		int i;
		int j;
		int numSystems = localSysList.size();
		int numSites = siteList.size();
		int numAdditionalDeployments = 0;
		for(i=0; i<numSystems; i++) {
			for(j=0; j<numSites; j++) {
				if(localSystemSiteMatrix[i][j] == 0 && localSystemSiteResultMatrix[i][j] == 1) {
					numAdditionalDeployments ++;
				}
			}
		}
		
		systemInfoHash.put("decommissionedCount", totalSys - totalKept);
		systemInfoHash.put("beforeCount", totalSys);
		systemInfoHash.put("afterCount", totalKept);
		systemInfoHash.put("additionalDeployment", numAdditionalDeployments);
		
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);

		budgetInfoHash.put("formattedCurrentSustVal", formatter.format(currentSustainmentCost));
		budgetInfoHash.put("formattedFutureSustVal", formatter.format(futureSustainmentCost));
		budgetInfoHash.put("formattedCostVal", formatter.format(adjustedDeploymentCost));
		
		overviewInfoHash.put("systemInfo",systemInfoHash);
		overviewInfoHash.put("budgetInfo",budgetInfoHash);
		
		return overviewInfoHash;
	}
	
	public Hashtable<String,Object> getOverviewSiteMapData() {
		double[] currSiteSustainCost;
		double[] futureSiteSustainCost;

		currSiteSustainCost = calculateSiteCostsForLocalSystems(localSystemSiteMatrix, localSystemSiteMaintenaceCostArr);

		futureSiteSustainCost = calculateSiteCostsForLocalSystems(localSystemSiteResultMatrix, localSystemSiteMaintenaceCostArr);
		
		String[] names = new String[]{"DCSite", "lat", "lon", "Site Savings", SUSTAINED_SYSTEMS, CONSOLIDATED_SYSTEMS};
		ITableDataFrame data = new BTreeDataFrame(names);
		int i;
		int numSites = siteList.size();
		for(i=0; i<numSites; i++) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put(names[0], siteList.get(i));
			row.put(names[1], siteLat[i]);
			row.put(names[2], siteLon[i]);
			row.put(names[3], futureSiteSustainCost[i] - currSiteSustainCost[i]);
			row.put(names[4], getSustainedSystemsAtSiteList(i));
			row.put(names[5], getConsolidatedSystemsAtSiteList(i));
			data.addRow(row, row);
		}
		
		OCONUSMapPlaySheet ps = new OCONUSMapPlaySheet();
		ps.setDataFrame(data);
		ps.processQueryData();
		return (Hashtable<String,Object>)ps.getData();
	}
	
	/**
	 * Gets the health grid for all systems
	 * Includes whether those systems were sustained or or consolidated.
	 * @return
	 */
	public Hashtable<String,Object> getHealthGrid() {
		
		String sysKeptQueryString = makeSysKeptQueryString();
			
		String sysBindings = SysOptUtilityMethods.makeBindingString("System",localSysList) + SysOptUtilityMethods.makeBindingString("System",centralSysList);
		
		String query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) (COALESCE(?SustainmentBud,0.0) AS ?SustainmentBudget) (IF(?System in (" + sysKeptQueryString + "), \""+RECOMMENDED_SUSTAIN+"\", \""+RECOMMENDED_CONSOLIDATION+"\") AS ?Recommendation) WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud}}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} } BINDINGS ?System {" + sysBindings + "}";

		HealthGridSheet ps = new HealthGridSheet();
		ps.setQuery(query);
		ps.setRDFEngine(systemEngine);
		ps.createData();
		return (Hashtable<String,Object>)ps.getData();
	}

	public Hashtable<String,Object> getSystemInfoData(String system, Boolean isModernized) {
	
		Hashtable<String,Object> systemInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> dataBLUInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> descriptionInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> budgetInfoHash = new Hashtable<String,Object>();

		ArrayList<String> sysBLUList;
		ArrayList<String> sysDataList;
		String hosting;
		double sysCurrSustainCost;
		double sysFutureSustainCost;
		double sysDeployCost;
		String recommendation;
		
		if(localSysList.contains(system)) {//if noncentral system
			int sysIndex = localSysList.indexOf(system);

			sysBLUList = SysOptUtilityMethods.createNonZeroList(bluList, localSystemBLUMatrix[sysIndex]);
			sysDataList = SysOptUtilityMethods.createNonZeroList(dataList, localSystemDataMatrix[sysIndex]);
			hosting = "Local";
			
			sysCurrSustainCost = localSystemMaintenanceCostArr[sysIndex] / centralPercOfBudget;
			if(isModernized) {
				int i;
				int numSites = siteList.size();
				int numSustainSites = 0;
				for(i=0; i<numSites; i++) {
					if(localSystemSiteMatrix[sysIndex][i] == 1 && localSystemSiteResultMatrix[sysIndex][i] == 1)
						numSustainSites ++;
				}
				
				recommendation = "Recommend sustainment of " + numSustainSites + " accessible site(s)";
				int numAllFutureSites = (int)SysOptUtilityMethods.sumRow(localSystemSiteResultMatrix[sysIndex]);
				sysFutureSustainCost = localSystemMaintenanceCostArr[sysIndex] + numAllFutureSites * localSystemSiteMaintenaceCostArr[sysIndex];
				sysDeployCost = 0.0;

				for(i=0; i<numSites; i++) {
					sysDeployCost += localSystemSiteResultMatrix[sysIndex][i] * ((1 - localSystemSiteMatrix[sysIndex][i]) * localSystemSiteDeploymentCostArr[sysIndex] + (1+trainingPerc) * localSystemSiteInterfaceCostArr[sysIndex]);
				}
			}else {
				int numConsolidatedSites = (int)SysOptUtilityMethods.sumRow(localSystemSiteMatrix[sysIndex]);
				recommendation = "Recommend consolidation of " + numConsolidatedSites + " accessible site(s)";
				sysFutureSustainCost = 0;
				sysDeployCost = 0;
			}
			
		}else {//if central system
			int sysIndex = centralSysList.indexOf(system);
			
			sysBLUList = SysOptUtilityMethods.createNonZeroList(bluList, centralSystemBLUMatrix[sysIndex]);
			sysDataList = SysOptUtilityMethods.createNonZeroList(dataList, centralSystemDataMatrix[sysIndex]);
			
			double numHostedSites = centralSystemNumSiteArr[sysIndex];
			double numAccessibleSites = siteList.size() - numHostedSites;
			
			if(numHostedSites == 1) {
				hosting = "Central";
			}else if(numHostedSites > 1 && numHostedSites < 5 ) {
				hosting = "Regional";
			}else {
				hosting = "TBD";
			}
			
			sysCurrSustainCost = centralSystemMaintenanceCostArr[sysIndex];

			if(isModernized) {
				recommendation = "Recommend sustainment of " + numHostedSites + " host sites(s) and " + numAccessibleSites + " accessible site(s)";
				sysFutureSustainCost = sysCurrSustainCost;
				sysDeployCost = (1+trainingPerc)*centralSystemInterfaceCostArr[sysIndex];
			}else {
				recommendation = "Recommend consolidation of " + numHostedSites + " host sites(s) and " + numAccessibleSites + " accessible site(s)";
				sysFutureSustainCost = 0;
				sysDeployCost = 0;
			}
		}
		
		String downstreamQuery = "SELECT DISTINCT ?DownstreamSys WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + system + "> AS ?System){?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;}}";
		String upstreamQuery = "SELECT DISTINCT ?UpstreamSys WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + system + "> AS ?System){?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;}}";
		
		ArrayList<String> downstreamSysList = SysOptUtilityMethods.runListQuery(systemEngine, downstreamQuery);
		ArrayList<String> upsteramSysList = SysOptUtilityMethods.runListQuery(systemEngine, upstreamQuery);

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

		//creating what is needed for the system specs section
		dataBLUInfoHash.put("bluCount", sysBLUList);
		dataBLUInfoHash.put("dataCount", sysDataList);
		dataBLUInfoHash.put("upstreamSystems", upsteramSysList);
		dataBLUInfoHash.put("downstreamSystems", downstreamSysList);
		dataBLUInfoHash.put("hosting", hosting);
		
		//creating what is needed for the additional info hover over
		descriptionInfoHash.put("atoDate", ato);
		descriptionInfoHash.put("systemDesc", description);
		descriptionInfoHash.put("recommendation", recommendation);
		
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);

		//create what is needed for budget panel
		budgetInfoHash.put("formattedCurrentSustVal", formatter.format(sysCurrSustainCost));
		budgetInfoHash.put("formattedFutureSustVal", formatter.format(sysFutureSustainCost));
		budgetInfoHash.put("formattedCostVal", formatter.format(sysDeployCost));
		
		systemInfoHash.put("dataBluInfo",dataBLUInfoHash);
		systemInfoHash.put("decommissionInfo",descriptionInfoHash);
		systemInfoHash.put("budgetInfo",budgetInfoHash);

		return systemInfoHash;
	}
	
	public Hashtable<String,Object> getSystemSiteMapData(String system) {
		String[] names = new String[]{"DCSite", "lat", "lon", "Recommendation"};
		ITableDataFrame data = new BTreeDataFrame(names);
		
		int i;
		int numSites = siteList.size();
		int sysIndex;

		if(localSysList.contains(system)) {//if noncentral system
			sysIndex = localSysList.indexOf(system);
			for(i=0; i<numSites; i++) {
				if(localSystemSiteRecMatrix[sysIndex][i]!=null) {
					Map<String, Object> row = new HashMap<String, Object>();
					row.put(names[0], siteList.get(i));
					row.put(names[1], siteLat[i]);
					row.put(names[2], siteLon[i]);
					row.put(names[3], localSystemSiteRecMatrix[sysIndex][i]);
					data.addRow(row, row);
				}
			}

		}else {
			
			//if central system
			sysIndex = centralSysList.indexOf(system);
			
			if(centralSysKeptArr[sysIndex] == 1) {
				//if central system was kept
				for(i=0; i<numSites; i++) {
					Map<String, Object> row = new HashMap<String, Object>();
					row.put(names[0], siteList.get(i));
					row.put(names[1], siteLat[i]);
					row.put(names[2], siteLon[i]);
					if(centralSystemSiteMatrix[sysIndex][i]==1) {
						row.put(names[3], SUSTAINED_HOST_SITE);
					}else {
						row.put(names[3], SUSTAINED_ACCESSIBLE_SITE);
					}
					data.addRow(row, row);
				}
			}else {
				//if central system was decommissioned
				for(i=0; i<numSites; i++) {
					Map<String, Object> row = new HashMap<String, Object>();
					row.put(names[0], siteList.get(i));
					row.put(names[1], siteLat[i]);
					row.put(names[2], siteLon[i]);
					if(centralSystemSiteMatrix[sysIndex][i]==1) {
						row.put(names[3], PREVIOUSLY_HOSTED_SITE);
					}else {
						row.put(names[3], PREVIOUSLY_ACCESSIBLE_SITE);
					}
					data.addRow(row, row);
				}
			}
			
		}

		OCONUSMapPlaySheet ps = new OCONUSMapPlaySheet();
		ps.setDataFrame(data);
		ps.processQueryData();
		return (Hashtable<String,Object>)ps.getData();
		
	}
	
	public Hashtable<String,Object> getSystemCoverageData(String system) {

		Hashtable<String,Object> systemCoverageHash = new Hashtable<String,Object>();
		Hashtable<String, Hashtable<String,Object>> dataHash = new Hashtable<String, Hashtable<String,Object>> ();
		Set<String> uncoveredDataSet = new HashSet<String>();
		Set<String> uncoveredBLUSet = new HashSet<String>();

		
		if(system.equals(null) || system.isEmpty()) {

			//find the coverage hash for all systems included in the analysis
			uncoveredDataSet.addAll(dataList);
			uncoveredBLUSet.addAll(bluList);

			uncoveredDataSet = processCoverage(dataHash, uncoveredDataSet, localSystemDataMatrix, localSysList, dataList, localSysKeptArr, "Data");
			uncoveredDataSet = processCoverage(dataHash, uncoveredDataSet, centralSystemDataMatrix, centralSysList, dataList, centralSysKeptArr, "Data");
			
			uncoveredBLUSet = processCoverage(dataHash, uncoveredBLUSet, localSystemBLUMatrix, localSysList, bluList, localSysKeptArr, "BLU");
			uncoveredBLUSet = processCoverage(dataHash, uncoveredBLUSet, centralSystemBLUMatrix, centralSysList, bluList, centralSysKeptArr, "BLU");
			
		}else {
			//find the coverage hash for a specific system. Which means filtering the data and blu lists to be JUST those for that system
			int sysIndex;
			
			//get the correct list of data and blu
			if(localSysList.contains(system)) {
				sysIndex = localSysList.indexOf(system);
				
				uncoveredDataSet = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(dataList, localSystemDataMatrix[sysIndex]));
				uncoveredBLUSet = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(bluList, localSystemBLUMatrix[sysIndex]));
				
				uncoveredDataSet = processCoverage(dataHash, uncoveredDataSet, localSystemDataMatrix[sysIndex], localSystemDataMatrix, localSysKeptArr, localSysList, dataList, sysIndex, "Data");
				uncoveredDataSet = processCoverage(dataHash, uncoveredDataSet, localSystemDataMatrix[sysIndex], centralSystemDataMatrix, centralSysKeptArr, centralSysList, dataList, -1, "Data");
				uncoveredBLUSet = processCoverage(dataHash, uncoveredBLUSet, localSystemBLUMatrix[sysIndex], localSystemBLUMatrix, localSysKeptArr, localSysList, bluList, sysIndex, "BLU");
				uncoveredBLUSet = processCoverage(dataHash, uncoveredBLUSet, localSystemBLUMatrix[sysIndex], centralSystemBLUMatrix, centralSysKeptArr, centralSysList, bluList, -1, "BLU");			

			}else {
				sysIndex = centralSysList.indexOf(system);
				
				uncoveredDataSet = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(dataList, centralSystemDataMatrix[sysIndex]));
				uncoveredBLUSet = new HashSet<String>(SysOptUtilityMethods.createNonZeroList(bluList, centralSystemBLUMatrix[sysIndex]));
				
				uncoveredDataSet = processCoverage(dataHash, uncoveredDataSet, centralSystemDataMatrix[sysIndex], localSystemDataMatrix, localSysKeptArr, localSysList, dataList, -1, "Data");
				uncoveredDataSet = processCoverage(dataHash, uncoveredDataSet, centralSystemDataMatrix[sysIndex], centralSystemDataMatrix, centralSysKeptArr, centralSysList, dataList, sysIndex, "Data");
				uncoveredBLUSet = processCoverage(dataHash, uncoveredBLUSet, centralSystemBLUMatrix[sysIndex], localSystemBLUMatrix, localSysKeptArr, localSysList, bluList, -1, "BLU");
				uncoveredBLUSet = processCoverage(dataHash, uncoveredBLUSet, centralSystemBLUMatrix[sysIndex], centralSystemBLUMatrix, centralSysKeptArr, centralSysList, bluList, sysIndex, "BLU");
			}
		}
			

		
		Hashtable<String,Object> uncoveredFuncHash;
		uncoveredFuncHash = new Hashtable<String,Object>();
		uncoveredFuncHash.put("Data", uncoveredDataSet);
		uncoveredFuncHash.put("BLU", uncoveredBLUSet);
		dataHash.put("Uncovered", uncoveredFuncHash);
	
		systemCoverageHash.put("data",dataHash);

		return systemCoverageHash;
	}

	private Set<String> processCoverage(Hashtable<String, Hashtable<String,Object>> coverageHash,Set<String> uncoveredFuncSet, int[][] systemFuncMatrix, ArrayList<String> sysList, ArrayList<String> funcList, double[] sysKeptArr, String dataOrBLU) {
		
		int i;
		int j;
		int numFunc = funcList.size();
		int numSys = sysList.size();
		String func;
		String system;

		for(i=0; i<numFunc; i++) {
			func = funcList.get(i);
			
			for(j=0; j<numSys; j++) {
				
				if(systemFuncMatrix[j][i] == 1) {//another system provides the same functionality and is kept
					system = sysList.get(j);
					Hashtable<String,Object> systemHash;
					if(coverageHash.containsKey(system)) {
						systemHash = coverageHash.get(system);
					}else {
						String rec;
						if(sysKeptArr[j] == 0) {
							rec = RECOMMENDED_CONSOLIDATION;
						}else {
							rec = RECOMMENDED_SUSTAIN;
						}						
						systemHash = new Hashtable<String,Object>();
						systemHash.put("Data", new HashSet<String>());
						systemHash.put("BLU", new HashSet<String>());
						systemHash.put("Recommendation",rec);
						coverageHash.put(system, systemHash);
					}
					Set<String> funcSet = (Set<String>)systemHash.get(dataOrBLU);
					funcSet.add(func);
					uncoveredFuncSet.remove(func);
				}
			}
			
		}
		return uncoveredFuncSet;
	}	

	/**
	 * Coverage for a single system, filtered out data and and system
	 * @param coverageHash
	 * @param uncoveredFuncSet
	 * @param desiredFunc
	 * @param systemFuncMatrix
	 * @param sysKeptArr
	 * @param sysList
	 * @param funcList
	 * @param ignoreIndex
	 * @return
	 */
	private Set<String> processCoverage(Hashtable<String, Hashtable<String,Object>> coverageHash,Set<String> uncoveredFuncSet, int[] desiredFunc, int[][] systemFuncMatrix, double[] sysKeptArr, ArrayList<String> sysList, ArrayList<String> funcList, int ignoreIndex, String dataOrBLU) {
		
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
						Hashtable<String,Object> systemHash;
						if(coverageHash.containsKey(system)) {
							systemHash = coverageHash.get(system);
						}else {
							String rec;
							if(sysKeptArr[j] == 0) {
								rec = RECOMMENDED_CONSOLIDATION;
							}else {
								rec = RECOMMENDED_SUSTAIN;
							}						
							systemHash = new Hashtable<String,Object>();
							systemHash.put("Data", new HashSet<String>());
							systemHash.put("BLU", new HashSet<String>());
							systemHash.put("Recommendation",rec);
							coverageHash.put(system, systemHash);
						}
						Set<String> funcSet = (Set<String>)systemHash.get(dataOrBLU);
						funcSet.add(func);
						uncoveredFuncSet.remove(func);
					}
				}

			}
			
		}
		return uncoveredFuncSet;
	}
	
	private ArrayList<String> getSustainedSystemsAtSiteList(int siteIndex) {

		ArrayList<String> sysAtSiteList = new ArrayList<String>();
		int i;
		int numSys = localSysList.size();
		for(i=0; i<numSys; i++) {
			if(localSystemSiteResultMatrix[i][siteIndex] == 1)
				sysAtSiteList.add(localSysList.get(i));
		}
		
		numSys = centralSysList.size();
		for(i=0; i<numSys; i++) {
			if(centralSysKeptArr[i] == 1)
				sysAtSiteList.add("*"+centralSysList.get(i));
		}
		
		return sysAtSiteList;
	}
	
	private ArrayList<String> getConsolidatedSystemsAtSiteList(int siteIndex) {

		ArrayList<String> sysAtSiteList = new ArrayList<String>();
		int i;
		int numSys = localSysList.size();
		for(i=0; i<numSys; i++) {
			if(localSystemSiteMatrix[i][siteIndex] == 1 && localSystemSiteResultMatrix[i][siteIndex] == 0)
				sysAtSiteList.add(localSysList.get(i));
		}
		
		numSys = centralSysList.size();
		for(i=0; i<numSys; i++) {
			if(centralSysKeptArr[i] == 0)
				sysAtSiteList.add("*"+centralSysList.get(i));
		}
		
		return sysAtSiteList;
	}
	
	private double[] calculateSiteCostsForLocalSystems(double[][] sysSiteMatrix, double[] sysSiteCost) {
		int i;
		int j;
		int numSys = sysSiteMatrix.length;
		int numSite = sysSiteMatrix[0].length;
		double[] siteCostArr = new double[numSite];
		
		for(i=0; i<numSite; i++) {
			
			double siteCost = 0.0;
			for(j=0; j<numSys; j++) {
				
					siteCost += sysSiteMatrix[j][i] * sysSiteCost[j];
					
			}
			siteCostArr[i] = siteCost;
		}
		
		return siteCostArr;
	}
	
	private String makeSysKeptQueryString() {
		String query = "";
		int i;
		int numSys = localSysList.size();
		for(i = 0; i<numSys; i++)
			if(localSysKeptArr[i] == 1)
				query+= "<http://health.mil/ontologies/Concept/System/"+localSysList.get(i)+">,";

		numSys = centralSysList.size();
		for(i = 0; i<numSys; i++)
			if(centralSysKeptArr[i] == 1)
				query+= "<http://health.mil/ontologies/Concept/System/"+centralSysList.get(i)+">,";

		if(query.length() > 0)
			query = query.substring(0,query.length() - 1);
		
		return query;
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
