/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.ui.components.playsheets.OCONUSMapPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.specific.tap.HealthGridSheet;
import prerna.ui.components.specific.tap.SysSiteOptGraphFunctions;
import prerna.ui.components.specific.tap.SysSiteOptPlaySheet;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class SysSiteOptimizer extends UnivariateOpt {
	
	private static final Logger LOGGER = LogManager.getLogger(SysSiteOptimizer.class.getName());
	
	private IEngine systemEngine, siteEngine;
	
	private ArrayList<String> localSysList, centralSysList, dataList, bluList, siteList;
	
	private String capOrBPURI;
	
	//user must select these
	private double budgetForYear;
	private Boolean useDHMSMFunctionality;//true if data/blu list made from dhmsm capabilities. False if from the systems
	private String optType;

	//user can change these as advanced settings
	private double centralPercOfBudget, trainingPerc;
	
	//forced modernize/decomission
	private SysSiteOptDataStore localSysData, centralSysData;

	//results of the algorithm
	private SysSiteOptFunction optFunc;
	
	private int[] sustainedLocalSysIndiciesArr, sustainedCentralSysIndiciesArr;
	private int[][] localSystemSiteResultMatrix;
	private String[][] localSystemSiteRecMatrix;
	
	//cost to develop interfaces
	private double[] localSysInterfaceCostArr, centralSysInterfaceCostArr;
	
	//system replacement results
	private ArrayList<Object[]> sysReplacementList;
	private String[] sysReplacementHeaders;
	
	private double[] siteLat, siteLon;
	private double yearsToComplete;
	private double currentSustainmentCost, futureSustainmentCost;
	private double adjustedDeploymentCost, adjustedTotalSavings, roi, irr;
	
	private double[] costAvoidedPerYearArr;
	public double[] deployCostPerYearArr, futureCostPerYearArr, currCostPerYearArr, cummDeployCostArr, cummCostAvoidedArr;
	public double[][] balanceArr;
	
	private final String RECOMMENDED_SUSTAIN = "Recommended_Sustain";
	private final String RECOMMENDED_CONSOLIDATION = "Recommended_Consolidation";
	private final String SUSTAINED_AND_DEPLOYED_SYSTEMS = "Sustained_and_Deployed_Systems";
	private final String CONSOLIDATED_SYSTEMS = "Consolidated_Systems";

	private final String SUSTAINED_HOST_SITE = "Sustained_Host_Site";
	private final String PREVIOUSLY_HOSTED_SITE = "Previously_Hosted_Site";
	private final String SUSTAINED_ACCESSIBLE_SITE = "Sustained_Accessible_Site";
	private final String DEPLOYED_ACCESSIBLE_SITE = "Deployed_Accessible_Site";
	private final String PREVIOUSLY_ACCESSIBLE_SITE = "Previously_Accessible_Site";
	
	public int startYear = 2015;
	
	public SysSiteOptimizer() {
		localSysData = new SysSiteOptDataStore();
		centralSysData = new SysSiteOptDataStore();
		
	}
	

	/**
	 * Executes the desktop optimization and displays the results.
	 * Includes optimization of sustained and consolidated systems
	 * AND optimization of system replacements
	 */
	@Override
	public void execute() {		
		executeWeb();

		optimizeSystemReplacements();

		display();
	}
	
	/**
	 * Executes the web optimization of sustained and consolidated systems
	 */
	public void executeWeb() {
	
		createDataBLUSiteLists();
		
		getData();
		
		printMissingData();
		
		optimizeSystemsAtSites();
		
		calculateConsolidatedSysInterfaceCosts();
		
		//for web display TODO move?
		SysSiteOptDataStore siteData = new SysSiteOptDataStore();
		siteData.fillSiteLatLon(siteList, siteEngine);
		siteLat = siteData.siteLat;
		siteLon = siteData.siteLon;
		
	}

	/**
	 * Sets variables for calculation
	 * @param budgetForYear	Annual budget of years to run analysis over
	 * @param years			Number of years to run analysis over
	 * @param infRate		Inflation rate as a decimal 
	 * @param disRate		Discount rate as a decimal 
	 * @param centralPercOfBudget	Decimal between 0 and 1 representing portion of budget that is a central cost
	 * @param trainingPerc	Decimal between 0 and 1 representing portion of budget that is a training cost
	 * @param hourlyCost	Dollar cost for 1 hour of work
	 */
	public void setVariables(double budgetForYear, int years, double infRate, double disRate, double centralPercOfBudget, double trainingPerc,double hourlyCost) {
		this.budgetForYear = budgetForYear;
		this.maxYears = years;
		this.infRate = infRate;
		this.disRate = disRate;
		this.centralPercOfBudget = centralPercOfBudget;
		this.trainingPerc = trainingPerc;
		this.hourlyCost = hourlyCost;
	}
	
	/**
	 * 	Divide systems between local and central
	 *	iterate through systems and update mod/decommission lists appropriately
	 * @param sysList
	 * @param modList
	 * @param decomList
	 */
	public void setSysList(ArrayList<String> sysList, ArrayList<String> modList, ArrayList<String> decomList) {
		divideCentralAndLocalSystems(sysList);

		int numLocalSys = localSysList.size();
		int numCentralSys = centralSysList.size();
		
		int[] localSystemIsModArr = new int[numLocalSys];
		int[] centralSystemIsModArr = new int[numCentralSys];
		
		int[] localSystemIsDecomArr = new int[numLocalSys];
		int[] centralSystemIsDecomArr = new int[numCentralSys];
		
		int i;
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
		
		localSysData.setForceModAndDecomArr(localSystemIsModArr, localSystemIsDecomArr);
		centralSysData.setForceModAndDecomArr(centralSystemIsModArr, centralSystemIsDecomArr);

	}
	
	public void setSysHashList(List<Map<String, String>> sysHashList) {
		
		ArrayList<String> sysList = new ArrayList<String>();
		for(Map<String, String> sysHash : sysHashList) {
			sysList.add(sysHash.get("name"));
		}
		
		divideCentralAndLocalSystems(sysList);
		
		int numLocalSys = localSysList.size();
		int numCentralSys = centralSysList.size();
		
		int[] localSystemIsModArr = new int[numLocalSys];
		int[] centralSystemIsModArr = new int[numCentralSys];
		
		int[] localSystemIsDecomArr = new int[numLocalSys];
		int[] centralSystemIsDecomArr = new int[numCentralSys];	
		
		int index;
		for(Map<String, String> hash : sysHashList) {
			String sys = hash.get("name");
			String status = hash.get("ind");
			
			if(localSysList.contains(sys)) {
				index = localSysList.indexOf(sys);
				if(status.equals("Sustain")) {
					localSystemIsModArr[index] = 1;
					localSystemIsDecomArr[index] = 0;	
				} else if(status.equals("Consolidate")) {
					localSystemIsModArr[index] = 0;
					localSystemIsDecomArr[index] = 1;
				} else {
					localSystemIsModArr[index] = 0;
					localSystemIsDecomArr[index] = 0;
				}
				
			}else {
				index = centralSysList.indexOf(sys);
				if(status.equals("Sustain")) {
					centralSystemIsModArr[index] = 1;
					centralSystemIsDecomArr[index] = 0;
				} else if(status.equals("Consolidate")) {
					centralSystemIsModArr[index] = 0;
					centralSystemIsDecomArr[index] = 1;
				} else {
					centralSystemIsModArr[index] = 0;
					centralSystemIsDecomArr[index] = 0;
				}

			}
		}
		localSysData.setForceModAndDecomArr(localSystemIsModArr, localSystemIsDecomArr);
		centralSysData.setForceModAndDecomArr(centralSystemIsModArr, centralSystemIsDecomArr);
	}
	
	/**
	 * Separates a list of systems into those that are local and central for analysis
	 * @param sysList the systems to identify whether local or central
	 */
	private void divideCentralAndLocalSystems(ArrayList<String> sysList) {
		localSysList = new ArrayList<String>();
		centralSysList = new ArrayList<String>();
		
		String sysBindings = "{" + SysOptUtilityMethods.makeBindingString("System",sysList) + "}";

		String sysDeploymentQuery = "SELECT DISTINCT ?System ?CentralDeploy WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> ?CentralDeploy}} ORDER BY ?System BINDINGS ?System "+sysBindings;
		ArrayList<Object []> sysDeploymentList = SysOptUtilityMethods.runQuery(systemEngine,sysDeploymentQuery);

		int i;
		for(i = 0; i<sysDeploymentList.size(); i++) {
			if(sysDeploymentList.get(i)[1].toString().toUpperCase().contains("Y")) {
				centralSysList.add(sysDeploymentList.get(i)[0].toString());
			}else {
				localSysList.add(sysDeploymentList.get(i)[0].toString());
			}
		}
		printMessage("Not Central Systems are..." + SysOptUtilityMethods.convertToString(localSysList));
		printMessage("Central Systems are..." + SysOptUtilityMethods.convertToString(centralSysList));
	}
	

	
	/**
	 * Creates data, blu, and site lists depending on users selections.
	 * Options for data/blu include:
	 * 1) Filtering data/blu to those required for a certain capability or bp
	 * 2) Filtering data/blu to those provided by DHMSM
	 * 3) Using any/all data/blu the systems provide or consume
	 * Site list is filtered to only include the locations of local/centrally deployed systems
	 */
	private void createDataBLUSiteLists() {
//TODO is sysbindings required anywhere else?
		String sysBindings = "{" + SysOptUtilityMethods.makeBindingString("System",localSysList) + SysOptUtilityMethods.makeBindingString("System",centralSysList) + "}";
		
		//Select the proper data/blu depending on query/user input
		String dataQuery, bluQuery;
		if(capOrBPURI != null && !capOrBPURI.isEmpty()) { 
			if(capOrBPURI.contains("Capability")){
				dataQuery = "SELECT DISTINCT ?Data WHERE {BIND(" + capOrBPURI + " as ?Capability){?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{ ?Capability <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess.}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?Data.}}";
				bluQuery = "SELECT DISTINCT ?BLU WHERE {BIND(" + capOrBPURI + " as ?Capability){?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{ ?Capability <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess.}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?BLU.}}";
			}else {
				dataQuery = "SELECT DISTINCT ?Data WHERE {BIND(" + capOrBPURI + " as ?BusinessProcess){?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?Data.}}";
				bluQuery = "SELECT DISTINCT ?BLU WHERE {BIND(" + capOrBPURI + " as ?BusinessProcess){?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?BLU.}}";
			}
		}else if(useDHMSMFunctionality) {
			dataQuery = "SELECT DISTINCT ?Data WHERE {BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> as ?DHMSM){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability}{ ?Capability <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess.}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?Data.}}";
			bluQuery = "SELECT DISTINCT ?BLU WHERE {BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> as ?DHMSM){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability}{ ?Capability <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess.}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?BLU.}}";
		}else {
			dataQuery = "SELECT DISTINCT ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data}} ORDER BY ?Data BINDINGS ?System " + sysBindings;
			bluQuery = "SELECT DISTINCT ?BLU WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU}} ORDER BY ?BLU BINDINGS ?System " + sysBindings;
		}

		String siteQuery = "SELECT DISTINCT ?Site WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;}{?Site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?Site;}{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite;} } ORDER BY ?Site BINDINGS ?System " + sysBindings;

		siteList = SysOptUtilityMethods.runListQuery(siteEngine, siteQuery);
		dataList = SysOptUtilityMethods.runListQuery(systemEngine, dataQuery);
		bluList = SysOptUtilityMethods.runListQuery(systemEngine, bluQuery);
		
		printMessage("Sites are..." + SysOptUtilityMethods.convertToString(siteList));
		printMessage("Data are..." + SysOptUtilityMethods.convertToString(dataList));
		printMessage("BLU are..." + SysOptUtilityMethods.convertToString(bluList));
	}
	
	private void getData() {
		//local

		ArrayList<String> allSysList = SysOptUtilityMethods.deepCopy(localSysList);
		allSysList.addAll(centralSysList);
		
		localSysData.fillSysSiteOptDataStores(localSysList, allSysList, dataList, bluList, siteList, systemEngine, siteEngine, centralPercOfBudget, trainingPerc, true);

		//central

		centralSysData.fillSysSiteOptDataStores(centralSysList, allSysList, dataList, bluList, siteList, systemEngine, siteEngine, centralPercOfBudget, trainingPerc, false);

		currentSustainmentCost = localSysData.currentSustainmentCost + centralSysData.currentSustainmentCost;

	}
	
	private void printMissingData() {
		int i;
		int size = localSysList.size();
		for(i=0; i<size; i++) {
			if(SysOptUtilityMethods.sumRow(localSysData.systemDataMatrix[i]) + SysOptUtilityMethods.sumRow(localSysData.systemBLUMatrix[i]) == 0)
				printMessage(localSysList.get(i) + " provides no data or BLU.");

			if(SysOptUtilityMethods.sumRow(localSysData.systemSiteMatrix[i]) == 0)
				printMessage(localSysList.get(i) + " has no site information.");
			
			if(localSysData.systemCentralMaintenanceCostArr[i] == 0)
				printMessage(localSysList.get(i) + " has no budget information.");
		}
		
		size = centralSysList.size();
		for(i=0; i<size; i++) {
			if(SysOptUtilityMethods.sumRow(centralSysData.systemDataMatrix[i]) + SysOptUtilityMethods.sumRow(centralSysData.systemBLUMatrix[i]) == 0)
				printMessage(centralSysList.get(i) + " provides no data or BLU.");
			
			if(centralSysData.systemCentralMaintenanceCostArr[i] == 0)
				printMessage(centralSysList.get(i) + " has no budget information.");
		}
	
	}
	
	private void optimizeSystemsAtSites() {
		
		if(optType.equals("Savings")) {
			optFunc = new SysSiteSavingsOptFunction();
		} else if(optType.equals("ROI")) {
			optFunc = new SysSiteROIOptFunction();
		} else if(optType.equals("IRR")) {
			optFunc = new SysSiteIRROptFunction();
		} else { //TODO
			printMessage("OPTIMIZATION TYPE WAS NOT SET");
			return;
		}
		optFunc.setPlaySheet(playSheet);
		optFunc.setVariables(localSysData, centralSysData, currentSustainmentCost, budgetForYear, maxYears, infRate, disRate);
		optFunc.value(budgetForYear * maxYears);
	
		sustainedLocalSysIndiciesArr = optFunc.getLocalSysSustainedArr();
		sustainedCentralSysIndiciesArr = optFunc.getCentralSysSustainedArr();
		localSystemSiteResultMatrix = optFunc.getSystemSiteResultMatrix();
		
		futureSustainmentCost = optFunc.getFutureSustainmentCost();
		adjustedTotalSavings = optFunc.getAdjustedTotalSavings();
		adjustedDeploymentCost = optFunc.getAdjustedDeploymentCost();
		roi = optFunc.getROI();
		irr = optFunc.getIRR();
		
		yearsToComplete = optFunc.getYearsToComplete();
	
		localSystemSiteRecMatrix = calculateSiteRecMatrix(localSysData.systemSiteMatrix, localSystemSiteResultMatrix);
		
		double mu = (1 + infRate / 100) / (1 + disRate / 100);
		
		deployCostPerYearArr = SysOptUtilityMethods.calculateAdjustedDeploymentCostArr(mu, yearsToComplete, false, maxYears, budgetForYear);
		currCostPerYearArr = SysOptUtilityMethods.calculateAdjustedDeploymentCostArr(mu,yearsToComplete, true, maxYears, currentSustainmentCost);
		futureCostPerYearArr =  SysOptUtilityMethods.calculateAdjustedSavingsArr(mu, yearsToComplete, maxYears, futureSustainmentCost);
		costAvoidedPerYearArr =  SysOptUtilityMethods.calculateAdjustedSavingsArr(mu, yearsToComplete, maxYears, currentSustainmentCost - futureSustainmentCost);
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
	
	private void calculateConsolidatedSysInterfaceCosts() {
		//iterate through all the sustained systems
		//run consolidated interface cost processor
		//add results to interface cost store.

		ConsolidatedInterfaceCostProcessor proc = new ConsolidatedInterfaceCostProcessor(localSysList.size(), centralSysList.size());
		
		for(int index : sustainedLocalSysIndiciesArr) {
			if(localSysData.systemHasUpstreamInterfaceArr[index] == 1) {
				proc.setVariables(systemEngine, localSysList.get(index), localSysList, centralSysList, dataList, localSysData.systemSiteMatrix[index], localSysData.systemSiteMatrix, localSystemSiteResultMatrix, sustainedCentralSysIndiciesArr, localSysData.systemSingleSiteInterfaceCostArr[index]);
				proc.execute();
				printMessage("Interface cost for " + localSysList.get(index)+ ": allocated " + proc.getAmountAllocated() + ", distributed to consolidated systems " + proc.getAmountDistributed());
			}else {
				printMessage("Interface cost for " + localSysList.get(index)+ ": allocated 0");
			}
		}
		
		int[] centralSiteDeployment = new int[siteList.size()];
		Arrays.fill(centralSiteDeployment, 1);
		
		for(int index : sustainedCentralSysIndiciesArr) {
			if(centralSysData.systemHasUpstreamInterfaceArr[index] == 1) {
				proc.setVariables(systemEngine, centralSysList.get(index), localSysList, centralSysList, dataList, centralSiteDeployment, localSysData.systemSiteMatrix, localSystemSiteResultMatrix, sustainedCentralSysIndiciesArr, centralSysData.systemSingleSiteInterfaceCostArr[index]);
				proc.execute();
				printMessage("Interface cost for " + centralSysList.get(index)+ ": allocated " + proc.getAmountAllocated() + ", distributed to consolidated systems " + proc.getAmountDistributed());
			}else {
				printMessage("Interface cost for " + centralSysList.get(index)+ ": allocated 0");
			}
		}
		localSysInterfaceCostArr = proc.getLocalSysInterfaceCost();
		centralSysInterfaceCostArr = proc.getCentralSysInterfaceCost();
	}
	
	private void optimizeSystemReplacements() {

		double[] localSystemFutureTotalMaintenanceCostArr = new double[localSysData.systemCentralMaintenanceCostArr.length];
		for(int i=0; i<localSysData.systemCentralMaintenanceCostArr.length; i++){
			int numFutureSystems = SysOptUtilityMethods.sumRow(localSystemSiteResultMatrix[i]);
			localSystemFutureTotalMaintenanceCostArr[i] = localSysData.systemCentralMaintenanceCostArr[i] + numFutureSystems * localSysData.systemSingleSiteMaintenanceCostArr[i];
			System.out.println(localSysList.get(i)+","+localSystemFutureTotalMaintenanceCostArr[i]);
		}
		
		SysReplacementProcessor proc = new SysReplacementProcessor(localSysList, centralSysList, dataList, bluList, sustainedLocalSysIndiciesArr, sustainedCentralSysIndiciesArr, localSysData.systemDataMatrix, localSysData.systemBLUMatrix, localSystemSiteResultMatrix, localSysData.systemTheaterArr, localSysData.systemGarrisonArr, localSystemFutureTotalMaintenanceCostArr, centralSysData.systemDataMatrix, centralSysData.systemBLUMatrix, centralSysData.systemTheaterArr, centralSysData.systemGarrisonArr, centralSysData.systemCentralMaintenanceCostArr);
	
		//TODO: catch exceptions?
		int numLocalSystems = localSysList.size();
		for(int i=0; i<numLocalSystems; i++) {
			if(!ArrayUtilityMethods.arrayContainsValue(sustainedLocalSysIndiciesArr, i)) {
				proc.optimizeSysReplacement(localSysList.get(i), localSysData.systemDataMatrix[i], localSysData.systemBLUMatrix[i], localSysData.systemSiteMatrix[i], localSysData.systemTheaterArr[i], localSysData.systemGarrisonArr[i]);

			}
		}

		int numCentralSystems = centralSysList.size();
		int[] centralSiteMatrix = new int[siteList.size()];
		Arrays.fill(centralSiteMatrix, 1);
		for(int i=0; i<numCentralSystems; i++) {
			if(!ArrayUtilityMethods.arrayContainsValue(sustainedCentralSysIndiciesArr, i)) {
				proc.optimizeSysReplacement(centralSysList.get(i),centralSysData.systemDataMatrix[i], centralSysData.systemBLUMatrix[i], centralSiteMatrix, centralSysData.systemTheaterArr[i], centralSysData.systemGarrisonArr[i]);
				
			}
		}
		
		sysReplacementHeaders = proc.getHeaders();
		sysReplacementList = proc.getSysReplacementList();
	}

	
	private String[][] calculateSiteRecMatrix(int[][] oldMatrix, int[][] newMatrix) {
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
		
		if(optType.equals("Savings")) {
			printMessage("**Adjusted Total Savings: " + adjustedTotalSavings);
		} else if(optType.equals("ROI")) {
			printMessage("**ROI: " + (optFunc.getROI()*100) + "%");
		} else if(optType.equals("IRR")) {
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
			createSiteGrid(localSysData.systemSiteMatrix, localSysList, siteList,"Current NonCentral Systems at Sites");
			createSiteGrid(localSystemSiteResultMatrix, localSysList, siteList,"Future NonCentral Systems at Sites");
			createSiteGrid(localSystemSiteRecMatrix, localSysList, siteList,"Changes for NonCentral Systems at Sites");

			createCostGrid();
			createCentralCostGrid();
			createTabAndDisplayList(sysReplacementHeaders,sysReplacementList,"Consolidated System Replacements",true);
		}
	}
	
	/*
	 * Part of desktop display
	 */
	private void createCostGrid() {
		
		String[] headers = new String[10];
		headers[0] = "System";
		headers[1] = "# of Current Sites";
		headers[2] = "# of New Site Deployments";
		headers[3] = "# of Consolidated Sites";
		headers[4] = "Central Sustainment Cost";
		headers[5] = "All Sites Sustainment Cost";
		headers[6] = "Interface Development Cost";
		headers[7] = "New Sites Deployment Cost";
		headers[8] = "User Training Costs";
		headers[9] = RECOMMENDED_SUSTAIN + " or " + RECOMMENDED_CONSOLIDATION;
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		int i=0;
		int rowLength = localSysList.size();
		
		for(i = 0; i<rowLength; i++) {
			int numOrigSites = SysOptUtilityMethods.sumRow(localSysData.systemSiteMatrix[i]);
			Object[] row = new Object[10];
			row[0] = localSysList.get(i);
			row[1] = numOrigSites;
			if(ArrayUtilityMethods.arrayContainsValue(sustainedLocalSysIndiciesArr, i)) {
				int numSites = siteList.size();
				int numAdditionalDeployments = 0;
				int numDecommissioned = 0;
				for(int j=0; j<numSites; j++) {
					if(localSysData.systemSiteMatrix[i][j] == 0 && localSystemSiteResultMatrix[i][j] == 1) {
						numAdditionalDeployments ++;
					}
					if(localSysData.systemSiteMatrix[i][j] == 1 && localSystemSiteResultMatrix[i][j] == 0) {
						numDecommissioned ++;
					}
				}
				row[2] = numAdditionalDeployments;
				row[3] = numDecommissioned;
				row[4] = localSysData.systemCentralMaintenanceCostArr[i];
				row[5] = localSysData.systemSingleSiteMaintenanceCostArr[i] * (numOrigSites + numAdditionalDeployments - numDecommissioned);
				row[6] = 0;
				row[7] = localSysData.systemSingleSiteDeploymentCostArr[i] * numAdditionalDeployments;
				row[8] = 0;
				row[9] = RECOMMENDED_SUSTAIN;
			} else {
				row[2] = 0;
				row[3] = numOrigSites;
				row[4] = 0;
				row[5] = 0;
				row[6] = localSysInterfaceCostArr[i];
				row[7] = 0;
				row[8] = localSysData.systemSingleSiteUserTrainingCostArr[i] * numOrigSites;
				row[9] = RECOMMENDED_CONSOLIDATION;
			}
			list.add(row);
		}
		createTabAndDisplayList(headers,list,"NonCentral System Costs",false);
	}
	
	/*
	 * Part of desktop display
	 */
	private void createCentralCostGrid() {
		
		String[] headers = new String[5];
		headers[0] = "Central System";
		headers[1] = "Sustainment Cost";
		headers[2] = "Interface Cost";
		headers[3] = "User Training Cost";
		headers[4] = RECOMMENDED_SUSTAIN + " or " + RECOMMENDED_CONSOLIDATION;
		
		ArrayList<Object []> list = new ArrayList<Object []>();
		
		int i=0;
		int rowLength = centralSysList.size();
		int numSites = siteList.size();
		
		for(i = 0; i<rowLength; i++) {
			Object[] row = new Object[5];
			row[0] = centralSysList.get(i);
			if(ArrayUtilityMethods.arrayContainsValue(sustainedCentralSysIndiciesArr, i)){
				row[1] = centralSysData.systemCentralMaintenanceCostArr[i];
				row[2] = 0;
				row[3] = 0;
				row[4] = RECOMMENDED_SUSTAIN;
				
			} else {
				row[1] = 0;
				row[2] = centralSysInterfaceCostArr[i];
				row[3] = centralSysData.systemSingleSiteUserTrainingCostArr[i] * numSites;
				row[4] = RECOMMENDED_CONSOLIDATION;
			}
			list.add(row);
		}
		createTabAndDisplayList(headers,list,"Central System Costs",false);
	}
	
	/*
	 * Part of desktop display
	 */
	private void createSiteGrid(int[][] matrix, ArrayList<String> rowLabels, ArrayList<String> colLabels, String title) {
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

	//TODO move to SysSiteOptPlaySheet
	/**
	 * Returns display information for SEMOSS Web,
	 * specifically whether to Sustain or Consolidate each system
	 * Return is list filled with a hashtable for each system with the system's name and recommendation
	 */
	public List<Map<String,String>> getSysResultList() {
		List<Map<String,String>> sysResultList = new ArrayList<Map<String,String>>();
		
		int i;
		int numSys = localSysList.size();
		for(i = 0; i < numSys; i++) {
			Hashtable<String, String> sysHash = new Hashtable<String, String>();
			sysHash.put("name",localSysList.get(i));
			if(ArrayUtilityMethods.arrayContainsValue(sustainedLocalSysIndiciesArr, i))
				sysHash.put("ind", RECOMMENDED_SUSTAIN);
			else
				sysHash.put("ind", RECOMMENDED_CONSOLIDATION);
			sysResultList.add(sysHash);
		}
		
		numSys = centralSysList.size();
		for(i = 0; i < numSys; i++) {
			Hashtable<String, String> sysHash = new Hashtable<String, String>();
			sysHash.put("name",centralSysList.get(i));
			if(ArrayUtilityMethods.arrayContainsValue(sustainedCentralSysIndiciesArr, i))
				sysHash.put("ind", RECOMMENDED_SUSTAIN);
			else
				sysHash.put("ind", RECOMMENDED_CONSOLIDATION);
			sysResultList.add(sysHash);
		}
		return sysResultList;
	}
	
	public Hashtable<String,Object> getOverviewInfoData() {

		Hashtable<String,Object> overviewInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> systemInfoHash = new Hashtable<String,Object>();
		Hashtable<String,Object> budgetInfoHash = new Hashtable<String,Object>();
		
		int totalSys = localSysList.size() + centralSysList.size();
		double totalKept = sustainedLocalSysIndiciesArr.length + sustainedCentralSysIndiciesArr.length;
		
		int i;
		int j;
		int numSystems = localSysList.size();
		int numSites = siteList.size();
		int numAdditionalDeployments = 0;
		for(i=0; i<numSystems; i++) {
			for(j=0; j<numSites; j++) {
				if(localSysData.systemSiteMatrix[i][j] == 0 && localSystemSiteResultMatrix[i][j] == 1) {
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
		NumberFormat formatter = new DecimalFormat("'$' ###,###", symbols);

		budgetInfoHash.put("formattedCurrentSustVal", formatter.format(currentSustainmentCost));
		budgetInfoHash.put("formattedFutureSustVal", formatter.format(futureSustainmentCost));
		budgetInfoHash.put("formattedCostVal", formatter.format(adjustedDeploymentCost));
		
		overviewInfoHash.put("systemInfo",systemInfoHash);
		overviewInfoHash.put("budgetInfo",budgetInfoHash);
		
		return overviewInfoHash;
	}
	
	public Hashtable<String,Object> getOverviewSiteSavingsMapData() {
		double[] siteSavingsFromLocalSystems;
		double[] siteSavingsFromCentralSystems;

		siteSavingsFromLocalSystems = calculateSiteSavingsForLocalSystems(localSysData.systemSiteMatrix, localSystemSiteResultMatrix, localSysData.systemSingleSiteMaintenanceCostArr);

		siteSavingsFromCentralSystems = calculateSiteSavingsForCentralSystems(centralSysData.systemSiteMatrix, centralSysData.systemCentralMaintenanceCostArr, sustainedCentralSysIndiciesArr);
		Map<String, String> dataTableAlign = new HashMap<String, String>();
		String[] names = new String[]{"DCSite", "lat", "lon", "Site_Savings", SUSTAINED_AND_DEPLOYED_SYSTEMS, CONSOLIDATED_SYSTEMS};
		String[] types = new String[]{"STRING","DOUBLE","DOUBLE","DOUBLE","STRING","STRING"};
		dataTableAlign.put("label", "DCSite");
		dataTableAlign.put("lat", "lat");
		dataTableAlign.put("lon", "lon");
		dataTableAlign.put("size", SUSTAINED_AND_DEPLOYED_SYSTEMS);
		dataTableAlign.put("time", CONSOLIDATED_SYSTEMS);

		H2Frame data = new H2Frame(names, types);
		int i;
		int numSites = siteList.size();
		for(i=0; i<numSites; i++) {
			Object[] row = new Object[6];
			row[0] = siteList.get(i);
			row[1] = siteLat[i];
			row[2] = siteLon[i];
			if(siteSavingsFromCentralSystems == null) {
				row[3] = siteSavingsFromLocalSystems[i];
			}else {
				row[3] = siteSavingsFromLocalSystems[i] + siteSavingsFromCentralSystems[i];
			}
			row[4] = makeString(getSustainedSystemsAtSiteList(i));
			row[5] = makeString(getConsolidatedSystemsAtSiteList(i));
			data.addRow(row, names);
		}
		
		OCONUSMapPlaySheet ps = new OCONUSMapPlaySheet();
		ps.setDataMaker(data);
		ps.setTableDataAlign(dataTableAlign);
		ps.processQueryData();
		Hashtable<String, Object> retMap = (Hashtable<String, Object>) ps.getDataMakerOutput();
		retMap.put("layout", "WorldMap");
		retMap.put("dataTableAlign", ps.getDataTableAlign());

		return retMap;
	}
	
	public Hashtable<String,Object> getOverviewCapBPCoverageMapData() {
		
		//all the data and blu for each of the kept systems
		Hashtable<Integer,Set<String>> doBLUForKeptLocalSystemsHash = new Hashtable<Integer, Set<String>>();

		int index;
		Set<String> doBLUForSys;
		for(int i=0; i< sustainedLocalSysIndiciesArr.length; i++) {
			index = sustainedLocalSysIndiciesArr[i];
			doBLUForSys = new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(dataList,localSysData.systemDataMatrix[index]));
			doBLUForSys.addAll(SysOptUtilityMethods.convertToStringIfNonZero(bluList,localSysData.systemBLUMatrix[index]));
			doBLUForKeptLocalSystemsHash.put(index,doBLUForSys);
		}
		
		Set<String> doBLUForKeptCentralSystemsSet = new HashSet<String>();
		for(int i=0; i< sustainedCentralSysIndiciesArr.length; i++) {
			index = sustainedCentralSysIndiciesArr[i];
			doBLUForKeptCentralSystemsSet.addAll(SysOptUtilityMethods.convertToStringIfNonZero(dataList,centralSysData.systemDataMatrix[index]));
			doBLUForKeptCentralSystemsSet.addAll(SysOptUtilityMethods.convertToStringIfNonZero(bluList,centralSysData.systemBLUMatrix[index]));
		}
		
		String[] names = new String[]{"DCSite", "lat", "lon", "Percent DOs and BLUs covered", SUSTAINED_AND_DEPLOYED_SYSTEMS};
		String[] types = new String[]{"STRING","DOUBLE","DOUBLE","DOUBLE","STRING"};
		ITableDataFrame data = new H2Frame(names, types);
		int i;
		int j;
		int numSites = siteList.size();
		double totalDOBLU = dataList.size() + bluList.size();
		Set<String> doBLUForSiteSet;
		for(i=0; i<numSites; i++) {
			
			//get the data and BLU for each system at the site in future
			doBLUForSiteSet = new HashSet<String>(doBLUForKeptCentralSystemsSet);
			for(j=0; j<localSysList.size(); j++) {
				if(localSystemSiteResultMatrix[j][i] == 1) {
					doBLUForSiteSet.addAll(doBLUForKeptLocalSystemsHash.get(j));
				}
			}
			
			double percent = doBLUForSiteSet.size() / totalDOBLU * 100;
			
			Object[] row = new Object[5];
			row[0] = siteList.get(i);
			row[1] = siteLat[i];
			row[2] = siteLon[i];
			row[3] = percent;
			row[4] = makeString(getSustainedSystemsAtSiteList(i));
			data.addRow(row, names);
		}
		
		OCONUSMapPlaySheet ps = new OCONUSMapPlaySheet();
		ps.setDataMaker(data);
		ps.processQueryData();

		Hashtable<String, Object> retMap = (Hashtable<String, Object>) ps.getDataMakerOutput();
		retMap.put("layout", "WorldMap");
		retMap.put("dataTableAlign", ps.getDataTableAlign());

		return retMap;
	}
	
	/**
	 * Gets the health grid for all systems
	 * Includes whether those systems were sustained or or consolidated.
	 * @return
	 */
	public Hashtable<String,Object> getHealthGrid() {
		
		String sysKeptQueryString = makeSysKeptQueryString();
			
		String sysBindings = SysOptUtilityMethods.makeBindingString("System",localSysList) + SysOptUtilityMethods.makeBindingString("System",centralSysList);
		
		String query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?attm, 0.0) AS ?ArchitecturalComplexity) (COALESCE(?iatm, 0.0) AS ?InformationAssurance) (COALESCE(?nfrtm, 0.0) AS ?NonFunctionalRequirements)  (COALESCE(?SustainmentBud,0.0) AS ?SustainmentBudget) (IF(?System in (" + sysKeptQueryString + "), \""+RECOMMENDED_SUSTAIN+"\", \""+RECOMMENDED_CONSOLIDATION+"\") AS ?Recommendation) WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud}OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}  OPTIONAL { ?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM>  ?estm ;} OPTIONAL { ?System <http://semoss.org/ontologies/Relation/Contains/ArchitecturalComplecxityTM>  ?attm ;}  OPTIONAL { ?System <http://semoss.org/ontologies/Relation/Contains/InformationAssuranceTM>  ?iatm ;} OPTIONAL { ?System <http://semoss.org/ontologies/Relation/Contains/NonFunctionalRequirementsTM>  ?nfrtm ;}  } BINDINGS ?System {" + sysBindings + "}";
		DataMakerComponent dmc = new DataMakerComponent(systemEngine, query);
		HealthGridSheet ps = new HealthGridSheet();
		ps.processDataMakerComponent(dmc);
		ps.processQueryData();
		Hashtable<String, Object> retMap = (Hashtable<String,Object>)ps.getDataMakerOutput();
		retMap.put("dataTableAlign", ps.getDataTableAlign());
		retMap.put("layout", "prerna.ui.components.specific.tap.HealthGridSheetPortRat");
		return retMap;
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

			sysDataList = SysOptUtilityMethods.convertToStringIfNonZero(dataList, localSysData.systemDataMatrix[sysIndex]);
			sysBLUList = SysOptUtilityMethods.convertToStringIfNonZero(bluList, localSysData.systemBLUMatrix[sysIndex]);
			hosting = "Local";
			
			sysCurrSustainCost = localSysData.systemCentralMaintenanceCostArr[sysIndex] / centralPercOfBudget;

			int i;
			int numSites = siteList.size();
			if(isModernized) {
				int numSustainSites = 0;
				for(i=0; i<numSites; i++) {
					if(localSysData.systemSiteMatrix[sysIndex][i] == 1 && localSystemSiteResultMatrix[sysIndex][i] == 1)
						numSustainSites ++;
				}
				
				recommendation = "Recommend sustainment of " + numSustainSites + " accessible site(s)";
				int numAllFutureSites = (int)SysOptUtilityMethods.sumRow(localSystemSiteResultMatrix[sysIndex]);
				sysFutureSustainCost = localSysData.systemCentralMaintenanceCostArr[sysIndex] + numAllFutureSites * localSysData.systemSingleSiteMaintenanceCostArr[sysIndex];
				sysDeployCost = 0.0;
				
				for(i=0; i<numSites; i++) {
					sysDeployCost += localSysData.systemSingleSiteDeploymentCostArr[sysIndex] * (1 - localSysData.systemSiteMatrix[sysIndex][i]) * localSystemSiteResultMatrix[sysIndex][i];
				}
				sysDeployCost += localSysInterfaceCostArr[sysIndex];
				
			}else {
				int numConsolidatedSites = (int)SysOptUtilityMethods.sumRow(localSysData.systemSiteMatrix[sysIndex]);
				recommendation = "Recommend consolidation of " + numConsolidatedSites + " accessible site(s)";
				sysFutureSustainCost = 0;
				sysDeployCost = 0.0;
				for(i=0; i<numSites; i++) {
					sysDeployCost += localSysData.systemSingleSiteUserTrainingCostArr[sysIndex] * localSysData.systemSiteMatrix[sysIndex][i] * (1 - localSystemSiteResultMatrix[sysIndex][i]);
				}
			}
			
		}else {//if central system
			int sysIndex = centralSysList.indexOf(system);
			
			sysDataList = SysOptUtilityMethods.convertToStringIfNonZero(dataList, centralSysData.systemDataMatrix[sysIndex]);
			sysBLUList = SysOptUtilityMethods.convertToStringIfNonZero(bluList, centralSysData.systemBLUMatrix[sysIndex]);
			
			double numHostedSites = SysOptUtilityMethods.sumRow(centralSysData.systemSiteMatrix[sysIndex]);
			double numAccessibleSites = siteList.size() - numHostedSites;
			int numSites = siteList.size();
			if(numHostedSites == 1) {
				hosting = "Central";
			}else if(numHostedSites > 1) {
				hosting = "Regional";
			}else {
				hosting = "TBD";
			}
	        
			sysCurrSustainCost = centralSysData.systemCentralMaintenanceCostArr[sysIndex];

			if(isModernized) {
				recommendation = "Recommend sustainment of " + numHostedSites + " host sites(s) and " + numAccessibleSites + " accessible site(s)";
				sysFutureSustainCost = sysCurrSustainCost;
				sysDeployCost = centralSysInterfaceCostArr[sysIndex];

			}else {
				recommendation = "Recommend consolidation of " + numHostedSites + " host sites(s) and " + numAccessibleSites + " accessible site(s)";
				sysFutureSustainCost = 0;
				sysDeployCost = centralSysData.systemSingleSiteUserTrainingCostArr[sysIndex] * numSites;
			}
		}
		
		String downstreamQuery = "SELECT DISTINCT ?DownstreamSys WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + system + "> AS ?System){?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;}}";
		String upstreamQuery = "SELECT DISTINCT ?UpstreamSys WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + system + "> AS ?System){?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;}}";
		
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
		NumberFormat formatter = new DecimalFormat("'$' ###,###", symbols);

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
		String[] types = new String[]{"STRING","DOUBLE","DOUBLE","STRING"};
		H2Frame data = new H2Frame(names, types);
		
		int i;
		int numSites = siteList.size();
		int sysIndex;

		if(localSysList.contains(system)) {//if noncentral system
			sysIndex = localSysList.indexOf(system);
			for(i=0; i<numSites; i++) {
				if(localSystemSiteRecMatrix[sysIndex][i]!=null) {
					Object[] row = new Object[4];
					row[0] = siteList.get(i);
					row[1] = siteLat[i];
					row[2] = siteLon[i];
					row[3] = localSystemSiteRecMatrix[sysIndex][i];
					data.addRow(row, names);
				}
			}

		}else {
			
			//if central system
			sysIndex = centralSysList.indexOf(system);

			if(ArrayUtilityMethods.arrayContainsValue(sustainedCentralSysIndiciesArr, sysIndex)) {
				//if central system was kept
				for(i=0; i<numSites; i++) {
					Object[] row = new Object[4];
					row[0] = siteList.get(i);
					row[1] = siteLat[i];
					row[2] = siteLon[i];
					if(centralSysData.systemSiteMatrix[sysIndex][i]==1) {
						row[3] = SUSTAINED_HOST_SITE;
					}else {
						row[3] = SUSTAINED_ACCESSIBLE_SITE;
					}
					data.addRow(row, names);
				}
			}else {
				//if central system was decommissioned
				for(i=0; i<numSites; i++) {
					Object[] row = new Object[4];
					row[0] = siteList.get(i);
					row[1] = siteLat[i];
					row[2] = siteLon[i];
					if(centralSysData.systemSiteMatrix[sysIndex][i]==1) {
						row[3] = PREVIOUSLY_HOSTED_SITE;
					}else {
						row[3] = PREVIOUSLY_ACCESSIBLE_SITE;
					}
					data.addRow(row, names);
				}
			}
		}

		OCONUSMapPlaySheet ps = new OCONUSMapPlaySheet();
		ps.setDataMaker(data);
		ps.processQueryData();
		Hashtable<String, Object> retMap = (Hashtable<String, Object>) ps.getDataMakerOutput();
		retMap.put("layout", "WorldMap");
		retMap.put("dataTableAlign", ps.getDataTableAlign());
		return retMap;		
	}
	
	public Hashtable<String,Object> getSystemCoverageData(String system) {

		ArrayList<String> dataList = convertToURIs(this.dataList,"http://health.mil/ontologies/Concept/DataObject/");
		ArrayList<String> bluList = convertToURIs(this.bluList,"http://health.mil/ontologies/Concept/BusinessLogicUnit/");
		Hashtable<String,Object> dataHash = new Hashtable<String,Object>();
		Hashtable<String, Hashtable<String,Object>> coverageHash = new Hashtable<String, Hashtable<String,Object>> ();
		
		//all the data and blu that will be in the coverage visual
		Set<String> dataSet;
		Set<String> bluSet;
		
		//all the data and blu that has been covered by at least one system
		Set<String> coveredDataSet = new HashSet<String>();
		Set<String> coveredBLUSet = new HashSet<String>();
		
		if(system == null || system.isEmpty()) {

			dataSet = new HashSet<String>(dataList);
			bluSet = new HashSet<String>(bluList);
			
			processCoverage(coverageHash, coveredDataSet, coveredBLUSet, localSysList, localSysData.systemDataMatrix, localSysData.systemBLUMatrix, dataList, bluList, sustainedLocalSysIndiciesArr);
			processCoverage(coverageHash, coveredDataSet, coveredBLUSet, centralSysList, centralSysData.systemDataMatrix, centralSysData.systemBLUMatrix, dataList, bluList, sustainedCentralSysIndiciesArr);

		}else if(localSysList.contains(system)) {
			int sysIndex = localSysList.indexOf(system);
			
			dataSet = new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(dataList, localSysData.systemDataMatrix[sysIndex]));
			bluSet = new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(bluList, localSysData.systemBLUMatrix[sysIndex]));
			
			Set<String> dataToIgnoreSet = new HashSet<String>(dataList);
			dataToIgnoreSet.removeAll(dataSet);
			Set<String> bluToIgnoreSet = new HashSet<String>(bluList);
			bluToIgnoreSet.removeAll(bluSet);
			
			processCoverage(coverageHash, coveredDataSet, coveredBLUSet, localSysList, localSysData.systemDataMatrix, localSysData.systemBLUMatrix, dataList, bluList, dataToIgnoreSet, bluToIgnoreSet, sustainedLocalSysIndiciesArr, sysIndex);
			processCoverage(coverageHash, coveredDataSet, coveredBLUSet, centralSysList, centralSysData.systemDataMatrix, centralSysData.systemBLUMatrix, dataList, bluList, dataToIgnoreSet, bluToIgnoreSet, sustainedCentralSysIndiciesArr, -1);


		}else {
			int sysIndex = centralSysList.indexOf(system);
			
			dataSet = new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(dataList, centralSysData.systemDataMatrix[sysIndex]));
			bluSet = new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(bluList, centralSysData.systemBLUMatrix[sysIndex]));
			
			Set<String> dataToIgnoreSet = new HashSet<String>(dataList);
			dataToIgnoreSet.removeAll(dataSet);
			Set<String> bluToIgnoreSet = new HashSet<String>(bluList);
			bluToIgnoreSet.removeAll(bluSet);
			
			processCoverage(coverageHash, coveredDataSet, coveredBLUSet, localSysList, localSysData.systemDataMatrix, localSysData.systemBLUMatrix, dataList, bluList, dataToIgnoreSet, bluToIgnoreSet, sustainedLocalSysIndiciesArr, -1);
			processCoverage(coverageHash, coveredDataSet, coveredBLUSet, centralSysList, centralSysData.systemDataMatrix, centralSysData.systemBLUMatrix, dataList, bluList, dataToIgnoreSet, bluToIgnoreSet, sustainedCentralSysIndiciesArr, sysIndex);
				
		}
		
		//all the data and blu that has not been covered by any system
		dataSet.removeAll(coveredDataSet);
		bluSet.removeAll(coveredBLUSet);
		
		Hashtable<String,Object> uncoveredFuncHash;
		uncoveredFuncHash = new Hashtable<String,Object>();
		uncoveredFuncHash.put("Data", dataSet);
		uncoveredFuncHash.put("BLU", bluSet);
		coverageHash.put("Uncovered", uncoveredFuncHash);
	
		dataHash.put("data",coverageHash);

		return dataHash;
	}

	private void processCoverage(Hashtable<String, Hashtable<String,Object>> coverageHash, Set<String> coveredData, Set<String> coveredBLU, ArrayList<String> sysList, int[][] systemDataMatrix, int[][] systemBLUMatrix, ArrayList<String> dataList, ArrayList<String> bluList, int[] sysKeptArr) {
		
		int i;
		int numSys = sysList.size();
		String system;
		String rec;

		for(i=0; i<numSys; i++) {
			
			system = "http://health.mil/ontologies/Concept/System/"+sysList.get(i);
			//system = sysList.get(i);

			if(ArrayUtilityMethods.arrayContainsValue(sysKeptArr,i)) {
				rec = RECOMMENDED_SUSTAIN;
			}else {
				rec = RECOMMENDED_CONSOLIDATION;
			}
			
			Hashtable<String,Object> systemHash = new Hashtable<String,Object>();
			coveredData.addAll(SysOptUtilityMethods.convertToStringIfNonZero(dataList,systemDataMatrix[i]));
			coveredBLU.addAll(SysOptUtilityMethods.convertToStringIfNonZero(bluList,systemBLUMatrix[i]));
			
			systemHash.put("Data", new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(dataList,systemDataMatrix[i])));
			systemHash.put("BLU", new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(bluList,systemBLUMatrix[i])));
			systemHash.put("Recommendation",rec);

			coverageHash.put(system, systemHash);
		}
		
	}	
	
	private void processCoverage(Hashtable<String, Hashtable<String,Object>> coverageHash, Set<String> coveredDataSet, Set<String> coveredBLUSet, ArrayList<String> sysList, int[][] systemDataMatrix, int[][] systemBLUMatrix, ArrayList<String> dataList, ArrayList<String> bluList, Set<String> dataToIgnoreSet, Set<String> bluToIgnoreSet, int[] sysKeptArr, int sysIndex) {
		
		int i;
		int numSys = sysList.size();
		String system;
		String rec;
		
		for(i=0; i<numSys; i++) {
			if(i != sysIndex) {
				system = "http://health.mil/ontologies/Concept/System/"+sysList.get(i);
				//system = sysList.get(i);
				if(ArrayUtilityMethods.arrayContainsValue(sysKeptArr,i)) {
					rec = RECOMMENDED_SUSTAIN;
				}else {
					rec = RECOMMENDED_CONSOLIDATION;
				}
				
				//filter the sys lists to only those we care about
				Set<String> sysDataSet = new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(dataList,systemDataMatrix[i]));
				sysDataSet.removeAll(dataToIgnoreSet);
				Set<String> sysBLUSet = new HashSet<String>(SysOptUtilityMethods.convertToStringIfNonZero(bluList,systemBLUMatrix[i]));
				sysBLUSet.removeAll(bluToIgnoreSet);
				
				coveredDataSet.addAll(sysDataSet);
				coveredBLUSet.addAll(sysBLUSet);
				
				Hashtable<String,Object> systemHash = new Hashtable<String,Object>();
				
				systemHash.put("Data", sysDataSet);
				systemHash.put("BLU", sysBLUSet);
				systemHash.put("Recommendation",rec);

				coverageHash.put(system, systemHash);
			}
		}
		
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
		for(i=0; i<sustainedCentralSysIndiciesArr.length; i++ ) {
			int index = sustainedCentralSysIndiciesArr[i];
			sysAtSiteList.add("*"+centralSysList.get(index));
		}
		
		return sysAtSiteList;
	}
	
	private ArrayList<String> getConsolidatedSystemsAtSiteList(int siteIndex) {

		ArrayList<String> sysAtSiteList = new ArrayList<String>();
		int i;
		int numSys = localSysList.size();
		for(i=0; i<numSys; i++) {
			if(localSysData.systemSiteMatrix[i][siteIndex] == 1 && localSystemSiteResultMatrix[i][siteIndex] == 0)
				sysAtSiteList.add(localSysList.get(i));
		}
		
		numSys = centralSysList.size();
		for(i=0; i<numSys; i++) {
			if(!ArrayUtilityMethods.arrayContainsValue(sustainedCentralSysIndiciesArr, i))
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
				
					siteCost += sysSiteMatrix[j][i] * sysSiteCost[j] / (1-centralPercOfBudget);
					
			}
			siteCostArr[i] = siteCost;
		}
		
		return siteCostArr;
	}

	//TODO verify
	private double[] calculateSiteSavingsForLocalSystems(int[][] sysSiteMatrix, int[][] sysSiteResultMatrix, double[] sysSiteCost) {
		int i;
		int j;
		int numSys = sysSiteMatrix.length;
		int numSite = sysSiteMatrix[0].length;
		double[] siteSavingsArr = new double[numSite];
		
		for(i=0; i<numSite; i++) {
			
			double siteSavings = 0.0;
			for(j=0; j<numSys; j++) {
				
				//if the system was decommissioned.... savings!!!!
				if(sysSiteMatrix[j][i] == 1 && sysSiteResultMatrix[j][i] == 0) {
					siteSavings += sysSiteCost[j] / (1-centralPercOfBudget);
				}
				else if(sysSiteMatrix[j][i] == 0 && sysSiteResultMatrix[j][i] == 1){
					siteSavings += sysSiteCost[j];
				}
					
			}
			siteSavingsArr[i] = siteSavings;
		}
		
		return siteSavingsArr;
	}
	
	//TODO verify
	private double[] calculateSiteSavingsForCentralSystems(int[][] sysSiteMatrix, double[] sysSiteCostArr, int[] centralSysSustainedArr) {
		int i;
		int j;
		if(sysSiteMatrix.length == 0) {
			return null;
		}
		int numSys = sysSiteMatrix.length;
		int numSite = sysSiteMatrix[0].length;
		double[] siteSavingsArr = new double[numSite];
		
		for(i=0; i<numSite; i++) {
			
			double siteSavings = 0.0;
			for(j=0; j<numSys; j++) {
				//if the system is kept.. there are no savings. cost stays the same
				//if the system is not kept... there are savings!
				double numSites = SysOptUtilityMethods.sumRow(sysSiteMatrix[j]);//TODO clean up this method
				if(numSites == 0.0) {
					numSites = 1.0;
				}
				if(!ArrayUtilityMethods.arrayContainsValue(centralSysSustainedArr,j))
					siteSavings += sysSiteMatrix[j][i] * sysSiteCostArr[j] / numSites;
			}
			siteSavingsArr[i] = siteSavings;
		}
		
		return siteSavingsArr;
	}
	
	private String makeString(ArrayList<String> list) {
		String ret = "";
		for(String ele : list) {
			ret = ret.concat(ele+"\n");
		}
		return ret;
	}
	
	private String makeSysKeptQueryString() {
		String query = "";
		int i;
		int index;
		
		for(i = 0; i<sustainedLocalSysIndiciesArr.length; i++) {
			index = sustainedLocalSysIndiciesArr[i];
			query+= "<http://health.mil/ontologies/Concept/System/"+localSysList.get(index)+">,";
		}

		for(i = 0; i<sustainedCentralSysIndiciesArr.length; i++) {
			index = sustainedCentralSysIndiciesArr[i];
			query+= "<http://health.mil/ontologies/Concept/System/"+centralSysList.get(index)+">,";
		}

		if(query.length() > 0)
			query = query.substring(0,query.length() - 1);
		
		return query;
	}
	
	private ArrayList<String> convertToURIs(ArrayList<String> list, String uri) {
		ArrayList<String> uriList = new ArrayList<String>();
		for(String ele : list) {
			uriList.add(uri+ele);
		}
		return uriList;
	}
	private void printMessage(String message) {
		if(playSheet == null)
			System.out.println(message);
		else
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n" + message);
	}
	
	public void setEngines(IEngine systemEngine, IEngine siteEngine) {
		this.systemEngine = systemEngine;
		this.siteEngine = siteEngine;
	}
	
	public void setUseDHMSMFunctionality(Boolean useDHMSMFunctionality) {
		this.useDHMSMFunctionality = useDHMSMFunctionality;
	}
	
	public boolean setOptimizationType(String optType) {
		if(optType.equals("Savings") || optType.equals("ROI") || optType.equals("IRR")) {
			this.optType = optType;
			return true;
		}
		printMessage("OPTIMIZATION TYPE DOES NOT EXIST");
		return false;
	}
	
	public void setCapOrBPURI(String capOrBPURI) {
		this.capOrBPURI = capOrBPURI;
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
