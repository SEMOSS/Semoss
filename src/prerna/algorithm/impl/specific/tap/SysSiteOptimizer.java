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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import javax.swing.JDesktopPane;

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

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class SysSiteOptimizer implements IAlgorithm {
	
	private static final Logger LOGGER = LogManager.getLogger(SysSiteOptimizer.class.getName());

	private ArrayList<String> sysList, dataList, bluList, siteList;
	Integer[] modArr, centralModArr, decomArr, centralDecomArr;
	
	private IEngine systemEngine;
	private IEngine siteEngine;
	
	//user must select these
	private double budgetForYear;
	private int years;
	private Boolean useDHMSMFunctionality = false;//true if data/blu list made from dhmsm capabilities. False if from the systems
	private Boolean isOptimizeBudget = false;

	//user can change these as advanced settings
	private double infRate = 1.5;
	private double disRate = 2.5;
	private int noOfPts = 1;
	
	//user should not change these
	private double centralDeploymentPer = 0.80;
	private double deploymentFactor = 5;
	
	//generated data stores based off of the users selections
	private int[][] systemDataMatrix, systemBLUMatrix;
	private double[][] systemSiteMatrix;
	private int[] systemTheater, systemGarrison;

	private double[] maintenaceCosts;
	private double[] siteMaintenaceCosts;//assuming that may be different at different sites/
	private double[] siteDeploymentCosts;//assuming that may be different at different sites/
	
	private ArrayList<String> centralSysList;
	private int[][] centralSystemDataMatrix, centralSystemBLUMatrix;
	private int[] centralSystemTheater, centralSystemGarrison;
	
	private double[] centralSystemMaintenaceCosts;

	//results of the algorithm
	private SysSiteOptFunction optFunc = new SysSiteSavingsOptFunction();;
	
	private double[] sysKeptArr;
	private double[] centralSysKeptArr;
	private double[][] systemSiteResultMatrix;
	
	private double currSustainmentCost, futureSustainmentCost;
	private double adjustedDeploymentCost, adjustedTotalSavings;
	
	private double[] budgetSpentPerYear, costAvoidedPerYear;
	
	@Override
	public void execute() {
	
		long startTime;
		long endTime;
		
		startTime = System.currentTimeMillis();			
		getData();
		endTime = System.currentTimeMillis();
		System.out.println("Time to query data " + (endTime - startTime) / 1000 );
		
		startTime = System.currentTimeMillis();			
		optimizeSystemsAtSites();
		endTime = System.currentTimeMillis();
		System.out.println("Time to run LP " + (endTime - startTime) / 1000 );
		
	}
	
	public void setEngines(IEngine systemEngine, IEngine siteEngine) {
		this.systemEngine = systemEngine;
		this.siteEngine = siteEngine;
	}
	
	public void setUseDHMSMFunctionality(Boolean useDHMSMFunctionality) {
		this.useDHMSMFunctionality = useDHMSMFunctionality;
	}
	
	public void setVariables(int budgetForYear, int years) {
		this.budgetForYear = budgetForYear;
		this.years = years;
	}
	
	public void setAdvancedVariables(double infRate, double disRate, int noOfPts) {
		this.infRate = infRate;
		this.disRate = disRate;
		this.noOfPts = noOfPts;
	}
	
	public void setSysList(ArrayList<String> sysList, ArrayList<String> centralSysList) {

		this.sysList = sysList;
		this.centralSysList = centralSysList;
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
		
		System.out.println("Not Central Systems are..." + SysOptUtilityMethods.createPrintString(sysList));
		System.out.println("Central Systems are..." + SysOptUtilityMethods.createPrintString(centralSysList));
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
	
	public void setSysHash(Hashtable<String,String> sysHash) {
		sysList = new ArrayList<String>();
		centralSysList = new ArrayList<String>();
		
		String centralSysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> 'Y'}{?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0)} ORDER BY ?System LIMIT 50";
		ArrayList<String> allCentralSys = SysOptUtilityMethods.runListQuery(systemEngine, centralSysQuery);
		
		int numCentral = 0;
		Iterator<String> sysItr = sysHash.keySet().iterator();
		while(sysItr.hasNext()) {
			String sys = sysItr.next();
			if(allCentralSys.contains(sys)) {
				numCentral++;
			}
		}
		

		int numNonCentral = sysHash.size() - numCentral;
		
		modArr = new Integer[numNonCentral];
		centralModArr = new Integer[numCentral];
		
		decomArr = new Integer[numNonCentral];
		centralDecomArr = new Integer[numCentral];		
		
		int centralIndex = 0;
		int nonCentralIndex = 0;
		sysItr = sysHash.keySet().iterator();
		while(sysItr.hasNext()) {
			
			String sys = sysItr.next();
			String status = sysHash.get(sys);
			
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
		
		System.out.println("Not Central Systems are..." + SysOptUtilityMethods.createPrintString(sysList));
		System.out.println("Central Systems are..." + SysOptUtilityMethods.createPrintString(centralSysList));

	}
	
	
	public void setOptimizationType(String type) {

		if(type.equals("savings"))
			optFunc = new SysSiteSavingsOptFunction();
		else if(type.equals("roi"))
			optFunc = new SysSiteROIOptFunction();
		else if(type.equals("irr"))
			optFunc = new SysSiteIRROptFunction();
		else {
			System.out.println("OPTIMIZATION TYPE DOES NOT EXIST");
		}
	}
	
	public void setIsOptimizeBudget(Boolean isOptimizeBudget) {
		this.isOptimizeBudget = isOptimizeBudget;
	}
	
	private void getData() {

		String sysBindings = "{" + SysOptUtilityMethods.makeBindingString("System",sysList) + SysOptUtilityMethods.makeBindingString("System",centralSysList) + "}";
		
		String siteQuery = "SELECT DISTINCT ?Site WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;}{?Site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?Site;}{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite;} } ORDER BY ?Site BINDINGS ?System @SYSTEM-BINDINGS@";
		siteQuery = siteQuery.replace("@SYSTEM-BINDINGS@",sysBindings);
		siteList = SysOptUtilityMethods.runListQuery(siteEngine, siteQuery);

		//any data and any blu is being selected
		String dataQuery, bluQuery;
		if(useDHMSMFunctionality) {
			dataQuery = "SELECT DISTINCT ?Data WHERE {BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?DHMSM){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability}{ ?Capability <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess.}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?Data.}}";
			bluQuery = "SELECT DISTINCT ?BLU WHERE {BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?DHMSM){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability}{ ?Capability <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess.}{?BusinessProcess <http://semoss.org/ontologies/Relation/Consists> ?Activity.}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?BLU.}}";
		}else {
			dataQuery = "SELECT DISTINCT ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data}} ORDER BY ?Data BINDINGS ?System @SYSTEM-BINDINGS@";
			dataQuery = dataQuery.replace("@SYSTEM-BINDINGS@",sysBindings);
			bluQuery = "SELECT DISTINCT ?BLU WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU}} ORDER BY ?BLU BINDINGS ?System @SYSTEM-BINDINGS@";
			bluQuery = bluQuery.replace("@SYSTEM-BINDINGS@",sysBindings);
		}

		dataList = SysOptUtilityMethods.runListQuery(systemEngine, dataQuery);
		bluList = SysOptUtilityMethods.runListQuery(systemEngine, bluQuery);
		
		System.out.println("Sites are..." + SysOptUtilityMethods.createPrintString(siteList));
		System.out.println("Data are..." + SysOptUtilityMethods.createPrintString(dataList));
		System.out.println("blu are..." + SysOptUtilityMethods.createPrintString(bluList));

		ResidualSystemOptFillData resFunc = new ResidualSystemOptFillData();
		resFunc.setSystemEngine(systemEngine);
		resFunc.setSiteEngine(siteEngine);
		resFunc.setSysSiteLists(sysList,dataList,bluList,siteList);
		resFunc.fillSysDeploymentOptDataStores();
		
		systemDataMatrix = resFunc.systemDataMatrix;
		systemBLUMatrix = resFunc.systemBLUMatrix;
		systemSiteMatrix = resFunc.systemSiteMatrix;
		
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

			currSustainmentCost += systemSustainmentBudget[i];
		}
		
		resFunc.setSysSiteLists(centralSysList,dataList,bluList,siteList);
		resFunc.fillSysDeploymentOptDataStores();
		
		centralSystemDataMatrix = resFunc.systemDataMatrix;
		centralSystemBLUMatrix = resFunc.systemBLUMatrix;

		centralSystemTheater = resFunc.systemTheater;
		centralSystemGarrison = resFunc.systemGarrison;

		centralSystemMaintenaceCosts = resFunc.systemCostOfMaintenance;
		int centralSysLength = centralSysList.size();
		for(i=0; i<centralSysLength; i++) {

			currSustainmentCost += centralSystemMaintenaceCosts[i];
		}

	}
	
	private void optimizeSystemsAtSites() {
		
		optFunc.setVariables(systemDataMatrix, systemBLUMatrix, systemSiteMatrix, systemTheater, systemGarrison, modArr, decomArr, maintenaceCosts, siteMaintenaceCosts, siteDeploymentCosts, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemTheater, centralSystemGarrison, centralModArr, centralDecomArr, centralSystemMaintenaceCosts, budgetForYear, years, currSustainmentCost, infRate, disRate);

		if(isOptimizeBudget) {
			UnivariateOptimizer optimizer = new BrentOptimizer(.001, 10000);
	
			RandomGenerator rand = new Well1024a(500);
			MultiStartUnivariateOptimizer multiOpt = new MultiStartUnivariateOptimizer(optimizer, noOfPts, rand);
			UnivariateObjectiveFunction objF = new UnivariateObjectiveFunction(optFunc);
			SearchInterval search = new SearchInterval(0, budgetForYear * years);
			MaxEval eval = new MaxEval(200);
			
			OptimizationData[] data = new OptimizationData[] { search, objF, GoalType.MAXIMIZE, eval };
			try {
				UnivariatePointValuePair pair = multiOpt.optimize(data);
				optFunc.value(pair.getPoint());
				
			} catch (TooManyEvaluationsException fee) {
				System.out.println("Too many evalutions");
			}
		} else {
			optFunc.value(budgetForYear * years);
		}
	
		sysKeptArr = optFunc.getSysKeptArr();
		centralSysKeptArr = optFunc.getCentralSysKeptArr();
		systemSiteResultMatrix = optFunc.getSystemSiteResultMatrix();
		
		futureSustainmentCost = optFunc.getFutureSustainmentCost();
		adjustedDeploymentCost = optFunc.getAdjustedDeploymentCost();
		adjustedTotalSavings = optFunc.getAdjustedTotalSavings();

		double yearsToComplete = optFunc.getYearsToComplete();
		
		double mu = (1 + infRate / 100) / (1 + disRate / 100);
		
		budgetSpentPerYear = SysOptUtilityMethods.calculateAdjustedDeploymentCostArr(mu, yearsToComplete, years, budgetForYear);
		costAvoidedPerYear =  SysOptUtilityMethods.calculateAdjustedSavingsArr(mu, yearsToComplete, years, currSustainmentCost - futureSustainmentCost);

	}
	
	public void display() {
				
		createSiteGrid(systemSiteMatrix, sysList, siteList,"Current NonCentral Systems at Sites");
		createSiteGrid(systemSiteResultMatrix, sysList, siteList,"Future NonCentral Systems at Sites");
		createSiteChange(systemSiteMatrix, systemSiteResultMatrix, sysList, siteList,"Changes for NonCentral Systems at Sites");
		
		createOverallGrid(centralSysKeptArr, centralSysList, "Central System", "Future Central Systems (Was central system kept or decommissioned?)");
		createOverallGrid(sysKeptArr, sysList, "NonCentral Systems", "Future NonCentral Systems (Was noncentral system kept or decommissioned?)");
		
		System.out.println("**Curr Sustainment Cost " + currSustainmentCost);
		System.out.println("**Future Sustainment Cost " + futureSustainmentCost);
		System.out.println("**Adjusted Deployment Cost " + adjustedDeploymentCost);
		
		int i;
		System.out.println("**Year Investment CostAvoided: ");
		for(i = 0; i<years; i++) {
			System.out.println((i + 1) + " " + budgetSpentPerYear[i] + " " + costAvoidedPerYear[i]);
		}
		
		if(optFunc instanceof SysSiteSavingsOptFunction)
			System.out.println("**Adjusted Total Savings: " + adjustedTotalSavings);
		else if(optFunc instanceof SysSiteROIOptFunction)
			System.out.println("**ROI: " + optFunc.getROI());
		else if(optFunc  instanceof SysSiteIRROptFunction)
			System.out.println("**IRR: " + optFunc.getIRR());
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
		
		createNewGridPlaySheet(headers,list,title);
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
		
		createNewGridPlaySheet(headers,list,title);
	}
	
	private void createSiteChange(double[][] oldMatrix, double[][] newMatrix, ArrayList<String> rowLabels, ArrayList<String> colLabels, String title) {
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
				if(oldMatrix[i][j] == 0 && newMatrix[i][j] == 0) {
					row[j + 1] = "NOT AT";
				}else if(oldMatrix[i][j] == 1 && newMatrix[i][j] == 1) {
					row[j + 1] = "STAY";
				} else if(oldMatrix[i][j] == 1 && newMatrix[i][j] == 0) {
					row[j + 1] = "DECOMMISSIONED";
				} else if(oldMatrix[i][j] == 0 && newMatrix[i][j] == 1) {
					row[j + 1] = "DEPLOYED";
				}else {
					row[j + 1] = "PROBLEM";
				}
			}

			list.add(row);
		}
	
		createNewGridPlaySheet(headers,list,title);

	}
	
	private void createNewGridPlaySheet(String[] headers, ArrayList<Object []> list, String title) {
		GridPlaySheet ps = new GridPlaySheet();
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		ps.setJDesktopPane(pane);
		ps.setList(list);
		ps.setNames(headers);
		ps.setTitle(title);
		ps.setHorizontalScrollBar(true);
		ps.createView();
//		
//		PlaysheetCreateRunner runner = new PlaysheetCreateRunner(ps);
//		Thread playThread = new Thread(runner);
//		playThread.start();
	}
	
	
	private void printMatrixDifference(double[][] oldMatrix,double[][] newMatrix, ArrayList<String> rowLabels, ArrayList<String> colLabels) {
		int i;
		int j;

		int rowLength = rowLabels.size();
		int colLength = colLabels.size();
		
		for(i=0; i<rowLength; i++) {
			for(j=0; j<colLength; j++) {
				if(oldMatrix[i][j] != newMatrix[i][j]) {
					System.out.println("Changed " + rowLabels.get(i) + " - " + colLabels.get(j) + " from " + oldMatrix[i][j] + " to " + newMatrix[i][j]);
				}
			}
			
		}
	}
	
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		// TODO Auto-generated method stub
		
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
