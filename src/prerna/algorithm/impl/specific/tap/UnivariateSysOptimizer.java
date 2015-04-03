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

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.TreeSet;

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

import prerna.ui.components.specific.tap.DHMSMHelper;
import prerna.ui.components.specific.tap.QueryProcessor;
import prerna.ui.components.specific.tap.SysOptGraphFunctions;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.util.Utility;

/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class UnivariateSysOptimizer extends UnivariateOpt {
	
	private static final Logger LOGGER = LogManager.getLogger(UnivariateSysOptimizer.class.getName());
	
	private ResidualSystemOptFillData resFunc;
	public ResidualSystemOptimizer sysOpt;
	private String sysQuery, dataQuery, bluQuery, regionQuery;
	public ArrayList<String> sysList, dataList, bluList, regionList;
	public ArrayList<String> systemMustModernize, systemMustDecommission;
	protected double dataExposeCost, numMaintenanceSavings, preTransitionMaintenanceCost, postTransitionMaintenanceCost;
	public double budget = 0.0, optNumYears = 0.0, netSavings = 0.0, roi = 0.0, irr = 0.0;
	boolean noErrors = true;
	boolean includeRegionalization = false;
	boolean ignoreTheatGarr = false;
	boolean includeTheater = false;
	boolean includeGarrison = false;
	private String errorMessage = "";
	private boolean reducedFunctionality = false;
	public ArrayList<Double> cumSavingsList, breakEvenList, sustainCostList, installCostList, workDoneList;
	private int[] provideDataBLUNow;
	private int[] provideDataBLUFuture;
	
	private String capability, system, geoCapability;
	private ArrayList<String> geoSpatialMapSystemList;
	
	public void setSelectDropDowns(ArrayList<String> sysList, Boolean theaterSelected, Boolean garrisonSelected, ArrayList<String> capList, ArrayList<String> dataList, ArrayList<String> bluList, ArrayList<String> modSysList, ArrayList<String> decomSysList, boolean includeRegionalization, boolean ignoreTheatGarr) {
		this.includeRegionalization = includeRegionalization;
		this.ignoreTheatGarr = ignoreTheatGarr;
		if (!theaterSelected && !garrisonSelected) {
			includeTheater = true;
			includeGarrison = true;
		} else {
			includeTheater = theaterSelected;
			includeGarrison = garrisonSelected;
		}
		systemMustModernize = modSysList;
		systemMustDecommission = decomSysList;
		
		sysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}}";
		if(sysList.size() > 0)
			sysQuery = sysQuery + " BINDINGS ?System " + SysOptUtilityMethods.makeBindingString("System", sysList);

		if (includeRegionalization) {
			regionQuery = "SELECT DISTINCT ?Region WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?DeployedAt2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>} {?Includes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Includes>} {?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HealthServiceRegion>} {?Located <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Located>} {?System ?DeployedAt1 ?SystemDCSite} {?SystemDCSite ?DeployedAt2 ?DCSite} {?DCSite ?Includes ?MTF} {?MTF ?Located ?Region} } ORDER BY ASC(?Region) ";
			if(sysList.size() > 0)
				regionQuery = regionQuery + " BINDINGS ?System " + SysOptUtilityMethods.makeBindingString("System", sysList);
		}
		// if there are data and blu provided, then use them
		if (dataList.size() > 0 || bluList.size() > 0) {
			dataQuery = "SELECT DISTINCT ?DataObject WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}";
			bluQuery = "SELECT DISTINCT ?BusinessLogicUnit WHERE { {?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}}";
			if(dataList.size() > 0)
				dataQuery = dataQuery + " BINDINGS ?DataObject " + SysOptUtilityMethods.makeBindingString("DataObject", dataList);
			if(bluList.size() > 0)
				bluQuery = bluQuery + " BINDINGS ?BusinessLogicUnit " + SysOptUtilityMethods.makeBindingString("BusinessLogicUnit", bluList);
		}// if there are capabilities provided use them
		else if (capList.size() > 0) {
			dataQuery = "SELECT DISTINCT ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} }";
			bluQuery = "SELECT DISTINCT ?BLU WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BLU}}";
			dataQuery = dataQuery + " BINDINGS ?Capability " + SysOptUtilityMethods.makeBindingString("Capability", capList);
			bluQuery = bluQuery + " BINDINGS ?Capability "  + SysOptUtilityMethods.makeBindingString("Capability", capList);

			
		}// otherwise use the systems list to generate
		else {
			this.dataQuery = "NULL";
			DHMSMHelper dhelp = new DHMSMHelper();
			dhelp.setUseDHMSMOnly(false);
			dhelp.runData(playSheet.engine);
			ArrayList<String> systems = sysList;
			dataList = new ArrayList<String>();
			for (int sysInd = 0; sysInd < systems.size(); sysInd++) {
				String sys = systems.get(sysInd);
				dataList.addAll(dhelp.getAllDataFromSys(sys, "C"));
			}
			dataList = SysOptUtilityMethods.removeDuplicates(dataList);
			this.bluQuery = "SELECT DISTINCT ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU.}}";
			if(bluList.size() > 0) {
				bluQuery = bluQuery + " BINDINGS ?System "  + SysOptUtilityMethods.makeBindingString("System", sysList);
				
			}
		}
	}

	private void getData() {
		String engine = playSheet.engine.getEngineName();
		
		resFunc = new ResidualSystemOptFillData();
		resFunc.setMaxYears(maxYears);
		this.sysList = SysOptUtilityMethods.runListQuery(engine, sysQuery);
		if (this.sysList.size() < 2) {
			if (this.sysList.size() == 0)
				errorMessage = "No systems were selected. Please select systems under the Select System Functionality tab.";
			else if (sysList.size() == 1)
				errorMessage = "Only one system exists was selected. Please select more than one system under the Select System Functionality tab.";
			noErrors = false;
			return;
		}
		if (includeRegionalization)
			this.regionList = SysOptUtilityMethods.runListQuery("TAP_Site_Data", regionQuery);
		if (!dataQuery.equals("NULL"))
			this.dataList = SysOptUtilityMethods.runListQuery(engine, dataQuery);
		this.bluList = SysOptUtilityMethods.runListQuery(engine, bluQuery);
		if (this.dataList.size() == 0 && this.bluList.size() == 0) {
			errorMessage = "No data objects or business logic units were selected. Please select at least one under the Select System Functionality tab.";
			noErrors = false;
			return;
		}
		resFunc.setPlaySheet((SysOptPlaySheet) playSheet);
		if (includeRegionalization)
			resFunc.setSysDataBLULists(SysOptUtilityMethods.deepCopy(sysList), SysOptUtilityMethods.deepCopy(dataList), SysOptUtilityMethods.deepCopy(bluList), SysOptUtilityMethods.deepCopy(regionList), SysOptUtilityMethods.deepCopy(systemMustModernize),
					SysOptUtilityMethods.deepCopy(systemMustDecommission));
		else
			resFunc.setSysDataBLULists(SysOptUtilityMethods.deepCopy(sysList), SysOptUtilityMethods.deepCopy(dataList), SysOptUtilityMethods.deepCopy(bluList), null, SysOptUtilityMethods.deepCopy(systemMustModernize),
					SysOptUtilityMethods.deepCopy(systemMustDecommission));
		
		if (ignoreTheatGarr)
			reducedFunctionality = resFunc.fillDataStores();
		else
			reducedFunctionality = resFunc.fillTheaterGarrisonDataStores(includeTheater, includeGarrison);
		
		if (resFunc.doManualModDecommOverlap()) {
			errorMessage = "There is at least one system on the manually modernize and manually decommission. Please resolve the lists.";
			noErrors = false;
			return;
		}
	}
	
	private void getModernizedSysList() {
		playSheet.progressBar.setString("Determining Modernized List");
		if (ignoreTheatGarr)
			sysOpt = new ResidualSystemOptimizer();
		else
			sysOpt = new ResidualSystemTheatGarrOptimizer();
		sysOpt.setPlaySheet(playSheet);
		sysOpt.setDataSet(this.sysList, this.dataList, this.bluList, this.regionList, resFunc.systemDataMatrix, resFunc.systemBLUMatrix,
				resFunc.systemCostOfDataMatrix, resFunc.systemRegionMatrix, resFunc.systemCostOfMaintenance, resFunc.systemCostOfDB,
				resFunc.systemNumOfSites, resFunc.systemModernize, resFunc.systemDecommission, resFunc.dataRegionSORSystemCountReduced,
				resFunc.bluRegionProviderCountReduced);
		
		if (ignoreTheatGarr)
			noErrors = sysOpt.runOpt();
		else {
			((ResidualSystemTheatGarrOptimizer) sysOpt).setTheatGarrDataSet(resFunc.systemTheater,
					resFunc.systemGarrison,
					resFunc.dataRegionSORSystemTheaterCountReduced,
					resFunc.dataRegionSORSystemGarrisonCountReduced,
					resFunc.bluRegionProviderTheaterCountReduced,
					resFunc.bluRegionProviderGarrisonCountReduced);
			noErrors = ((ResidualSystemTheatGarrOptimizer) sysOpt).runOpt();
		}
		
		errorMessage = sysOpt.errorMessage;
		
		this.dataExposeCost = sysOpt.numTransformationTotal; // total cost to expose all data for all systems at all sites
		this.numMaintenanceSavings = sysOpt.numMaintenanceTotal;
		this.preTransitionMaintenanceCost = sysOpt.denomCurrentMaintenance;
		this.postTransitionMaintenanceCost = preTransitionMaintenanceCost - numMaintenanceSavings;
	}
	
	protected void optimizeBudget() {
		
		progressBar = playSheet.progressBar;
		f.setConsoleArea(playSheet.consoleArea);
		f.setProgressBar(progressBar);
		f.setVariables(maxYears, attRate, hireRate, infRate, disRate, scdLT, iniLC, scdLC);
		((UnivariateSysOptFunction) f).setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost, preTransitionMaintenanceCost,
				postTransitionMaintenanceCost, scdLT, iniLC, scdLC);
		if (f instanceof SysIRRFunction)
			((SysIRRFunction) f).createLinearInterpolation();
		((UnivariateSysOptFunction) f).createYearAdjuster(sysList, dataList, hourlyCost);
		
		// budget in LOE
		UnivariateOptimizer optimizer = new BrentOptimizer(.001, .001);
		RandomGenerator rand = new Well1024a(500);
		MultiStartUnivariateOptimizer multiOpt = new MultiStartUnivariateOptimizer(optimizer, noOfPts, rand);
		UnivariateObjectiveFunction objF = new UnivariateObjectiveFunction(f);
		SearchInterval search = new SearchInterval(minBudget, maxBudget); // budget in LOE
		optimizer.getStartValue();
		MaxEval eval = new MaxEval(200);
		
		OptimizationData[] data = new OptimizationData[] { search, objF, GoalType.MAXIMIZE, eval };
		try {
			UnivariatePointValuePair pair = multiOpt.optimize(data);
			progressBar.setIndeterminate(false);
			progressBar.setVisible(false);
			if (((UnivariateSysOptFunction) f).solutionExists) {
				budget = pair.getPoint();
				optNumYears = ((UnivariateSysOptFunction) f).calculateYears(budget);
				optNumYears = ((UnivariateSysOptFunction) f).yearAdjuster.adjustTimeToTransform(budget, optNumYears);
				if (optNumYears < 1) {
					optNumYears = 1;
					budget = ((UnivariateSysOptFunction) f).calculateBudgetForOneYear();
					// budget = ((SysNetSavingsFunction)f).workPerformedArray.get(0);
				}
				calculateSavingsROIAndIRR();
			} else {
				((SysOptPlaySheet) playSheet).solutionLbl.setText("No solution available within the given time frame");
				return;
			}
		} catch (TooManyEvaluationsException fee) {
			noErrors = false;
			playSheet.consoleArea.setText(playSheet.consoleArea.getText() + "\nError: " + fee);
			progressBar.setIndeterminate(false);
			progressBar.setVisible(false);
			clearPlaysheet();
		}
	}
	
	protected void display() {
		playSheet.consoleArea.setText(playSheet.consoleArea.getText() + "\nBudget: " + budget);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText() + "\nNumber of Years to consolidate systems: " + optNumYears);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText() + "\nGiven timespan to accumulate savings over: " + maxYears);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText() + "\nCumulative savings: " + netSavings);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText() + "\nROI: " + roi);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText() + "\nIRR: " + irr);
		displayResults();
		displaySystemSpecifics();
		displayCurrFunctionality();
		displayFutureFunctionality();
		displayHeatMap();
		// displayClusterHeatMap();
		setComboBoxes();
		SysOptPlaySheet.createSysCapButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				displaySysCapComparison();
			}
		});
		SysOptPlaySheet.createGeoSpatialMapButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				displayGeoSpatialMap();
			}
		});
	}
	
	public void optimize() {
		getData();
		if (noErrors == false) {
			playSheet.progressBar.setVisible(false);
			Utility.showError(errorMessage);
			return;
		}
		getModernizedSysList();
		if (noErrors == false) {
			playSheet.progressBar.setVisible(false);
			Utility.showError(errorMessage);
			return;
		}
		if (numMaintenanceSavings < serMainPerc * dataExposeCost) {
			// playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+"Potential annual sustainment savings is less than annual maintenance of exposed data objects. Rationalization solution is not available.");
			playSheet.consoleArea
					.setText(playSheet.consoleArea.getText()
							+ "\nError: "
							+ "Cost of current annual maintenance of systems is the same as rationalized annual maintenance. Rationalization solution is not available.");
			playSheet.progressBar.setVisible(false);
			// Utility.showError("Potential annual sustainment savings is less than annual maintenance of exposed data objects. Rationalization solution is not available.");
			Utility.showError("Cost of current annual maintenance of systems is the same as rationalized annual maintenance. Rationalization solution is not available.");
			return;
		}
		
		optimizeBudget();
		
		if (noErrors == false) {
			playSheet.consoleArea.setText(playSheet.consoleArea.getText() + "\nError: "
					+ "No possible Internal Rate of Return. Rationalization solution is not available.");
			playSheet.progressBar.setVisible(false);
			Utility.showError("No possible Internal Rate of Return. Rationalization solution is not available.");
			return;
		}
		
		display();
		
	}
	
	public void calculateSavingsROIAndIRR() {
		
		SysNetSavingsFunction savingsF = new SysNetSavingsFunction();
		savingsF.setVariables(maxYears, attRate, hireRate, infRate, disRate, scdLT, iniLC, scdLC);
		savingsF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost, preTransitionMaintenanceCost, postTransitionMaintenanceCost,
				scdLT, iniLC, scdLC);
		netSavings = savingsF.calculateRet(budget, optNumYears);
		
		SysROIFunction roiF = new SysROIFunction();
		roiF.setVariables(maxYears, attRate, hireRate, infRate, disRate, scdLT, iniLC, scdLC);
		roiF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost, preTransitionMaintenanceCost, postTransitionMaintenanceCost,
				scdLT, iniLC, scdLC);
		roi = roiF.calculateRet(budget, optNumYears);
		
		SysIRRFunction irrF = new SysIRRFunction();
		irrF.setVariables(maxYears, attRate, hireRate, infRate, disRate, scdLT, iniLC, scdLC);
		irrF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost, preTransitionMaintenanceCost, postTransitionMaintenanceCost,
				scdLT, iniLC, scdLC);
		irrF.createLinearInterpolation();
		irr = irrF.calculateRet(budget, optNumYears, netSavings);
		// playSheet.consoleArea.setText(playSheet.consoleArea.getText()+irrF.linInt.printString);
		if (f instanceof SysIRRFunction) {
			disRate = irr;
			if (irr == -1.0E30)
				noErrors = false;
		}
	}

	
	public void displaySystemSpecifics() {
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		String[] colNames;
		if (!ignoreTheatGarr) {
			int size = 11;
			if (includeRegionalization)
				size = 12;
			colNames = new String[size];
			colNames[0] = "System";
			colNames[1] = "Probability";
			colNames[2] = "MHS Specific";
			colNames[3] = "Theater";
			colNames[4] = "Garrison";
			colNames[5] = "Functional Area";
			colNames[6] = "Capability Group";
			colNames[7] = "Capability";
			colNames[8] = "Action";
			colNames[9] = "Number Of Data Provided";
			colNames[10] = "Number of BLU Provided";
			if (includeRegionalization)
				colNames[11] = "Number of Regions";
			for (int i = 0; i < sysList.size(); i++) {
				Object[] newRow = new Object[size];
				newRow[0] = resFunc.sysList.get(i);
				newRow[1] = resFunc.systemLPI[i];
				newRow[2] = resFunc.systemMHSSpecific[i];
				if (resFunc.systemTheater != null
						&& resFunc.systemTheater[i] > 0)
					newRow[3] = "X";
				if (resFunc.systemGarrison != null
						&& resFunc.systemGarrison[i] > 0)
					newRow[4] = "X";
				newRow[5] = resFunc.systemCONOPS[i];
				newRow[6] = resFunc.systemCapabilityGroup[i];
				newRow[7] = resFunc.systemCapability[i];
				if (sysOpt.systemIsModernized[i] > 0)
					newRow[8] = "Modernize";
				else
					newRow[8] = "Decommission";
				newRow[9] = SysOptUtilityMethods.sumRow(resFunc.systemDataMatrix[i]);
				newRow[10] = SysOptUtilityMethods.sumRow(resFunc.systemBLUMatrix[i]);
				if (includeRegionalization)
					newRow[11] = SysOptUtilityMethods.sumRow(resFunc.systemRegionMatrix[i]);
				list.add(newRow);
			}
		} else {
			int size = 9;
			if (includeRegionalization)
				size = 10;
			colNames = new String[size];
			colNames[0] = "System";
			colNames[1] = "Probability";
			colNames[2] = "MHS Specific";
			colNames[3] = "Functional Area";
			colNames[4] = "Capability Group";
			colNames[5] = "Capability";
			colNames[6] = "Action";
			colNames[7] = "Number Of Data Provided";
			colNames[8] = "Number of BLU Provided";
			if (includeRegionalization)
				colNames[9] = "Number of Regions";
			for (int i = 0; i < sysList.size(); i++) {
				Object[] newRow = new Object[size];
				newRow[0] = resFunc.sysList.get(i);
				newRow[1] = resFunc.systemLPI[i];
				newRow[2] = resFunc.systemMHSSpecific[i];
				newRow[3] = resFunc.systemCONOPS[i];
				newRow[4] = resFunc.systemCapabilityGroup[i];
				newRow[5] = resFunc.systemCapability[i];
				if (sysOpt.systemIsModernized[i] > 0)
					newRow[6] = "Modernize";
				else
					newRow[6] = "Decommission";
				newRow[7] = SysOptUtilityMethods.sumRow(resFunc.systemDataMatrix[i]);
				newRow[8] = SysOptUtilityMethods.sumRow(resFunc.systemBLUMatrix[i]);
				if (includeRegionalization)
					newRow[9] = SysOptUtilityMethods.sumRow(resFunc.systemRegionMatrix[i]);
				list.add(newRow);
			}
		}
		displayListOnTab(colNames, list, ((SysOptPlaySheet) playSheet).specificSysAlysPanel);
	}
	
	protected void displayCurrFunctionality() {
		provideDataBLUNow = new int[dataList.size() + bluList.size()];
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		int size = resFunc.sysList.size() + 3;
		if (includeRegionalization)
			size += resFunc.regionList.size() + 1;
		String[] colNames = new String[size];// number of systems+2
		colNames[0] = "Data/BLU";
		colNames[1] = "Type";
		colNames[2] = "Number of Systems Providing";
		for (int i = 0; i < resFunc.sysList.size(); i++)
			colNames[i + 3] = resFunc.sysList.get(i);
		if (includeRegionalization) {
			colNames[resFunc.sysList.size() + 3] = "Number of Regions Provided At";
			for (int i = 0; i < resFunc.regionList.size(); i++)
				colNames[resFunc.sysList.size() + 4 + i] = "Region " + resFunc.regionList.get(i);
		}
		for (int dataInd = 0; dataInd < resFunc.dataList.size(); dataInd++) {
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.dataList.get(dataInd);
			newRow[1] = "Data";
			// newRow[2] = sumRow(resFunc.systemDataMatrix[dataInd]);
			int numSystems = 0;
			for (int sysInd = 0; sysInd < resFunc.sysList.size(); sysInd++)
				if (resFunc.systemDataMatrix[sysInd][dataInd] == 1) {
					newRow[sysInd + 3] = "X";
					numSystems++;
				}
			newRow[2] = numSystems;
			provideDataBLUNow[dataInd] = numSystems;
			if (includeRegionalization) {
				int numRegions = 0;
				for (int regionInd = 0; regionInd < resFunc.regionList.size(); regionInd++)
					if (resFunc.dataRegionSORSystemCount[dataInd][regionInd] >= 1) {
						numRegions++;
						newRow[resFunc.sysList.size() + 4 + regionInd] = "X";
					}
				newRow[resFunc.sysList.size() + 3] = numRegions;
			}
			list.add(newRow);
		}
		for (int bluInd = 0; bluInd < resFunc.bluList.size(); bluInd++) {
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.bluList.get(bluInd);
			newRow[1] = "BLU";
			// newRow[2] = sumRow(resFunc.systemBLUMatrix[bluInd]);
			int numSystems = 0;
			for (int sysInd = 0; sysInd < resFunc.sysList.size(); sysInd++)
				if (resFunc.systemBLUMatrix[sysInd][bluInd] == 1) {
					newRow[sysInd + 3] = "X";
					numSystems++;
				}
			newRow[2] = numSystems;
			provideDataBLUNow[dataList.size() + bluInd] = numSystems;
			if (includeRegionalization) {
				int numRegions = 0;
				for (int regionInd = 0; regionInd < resFunc.regionList.size(); regionInd++)
					if (resFunc.bluRegionProviderCount[bluInd][regionInd] >= 1) {
						numRegions++;
						newRow[resFunc.sysList.size() + 4 + regionInd] = "X";
					}
				newRow[resFunc.sysList.size() + 3] = numRegions;
			}
			list.add(newRow);
		}
		displayListOnTab(colNames, list, ((SysOptPlaySheet) playSheet).currentFuncPanel, true);
	}
	
	protected void displayFutureFunctionality() {
		provideDataBLUFuture = new int[dataList.size() + bluList.size()];
		
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		int numModernizedSys = sysOpt.countModernized();
		int size = numModernizedSys + 3;
		if (includeRegionalization)
			size += resFunc.regionList.size() + 1;
		String[] colNames = new String[size];// number of systems+2
		colNames[0] = "Data/BLU";
		colNames[1] = "Type";
		colNames[2] = "Number of Systems Providing";
		int colIndex = 0;
		for (int i = 0; i < resFunc.sysList.size(); i++) {
			if (sysOpt.systemIsModernized[i] > 0) {
				colNames[colIndex + 3] = resFunc.sysList.get(i);
				colIndex++;
			}
		}
		if (includeRegionalization) {
			colNames[numModernizedSys + 3] = "Number of Regions Provided At";
			for (int i = 0; i < resFunc.regionList.size(); i++)
				colNames[numModernizedSys + 4 + i] = "Region " + resFunc.regionList.get(i);
		}
		for (int dataInd = 0; dataInd < resFunc.dataList.size(); dataInd++) {
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.dataList.get(dataInd);
			newRow[1] = "Data";
			colIndex = 0;
			int numSystems = 0;
			for (int sysInd = 0; sysInd < resFunc.sysList.size(); sysInd++)
				if (sysOpt.systemIsModernized[sysInd] > 0) {
					if (resFunc.systemDataMatrix[sysInd][dataInd] == 1) {
						newRow[colIndex + 3] = "X";
						numSystems++;
					}
					colIndex++;
				}
			newRow[2] = numSystems;
			provideDataBLUFuture[dataInd] = numSystems;
			if (includeRegionalization) {
				int numRegions = 0;
				for (int regionInd = 0; regionInd < resFunc.regionList.size(); regionInd++)
					if (resFunc.dataRegionSORSystemCount[dataInd][regionInd] >= 1) {
						numRegions++;
						newRow[numModernizedSys + 4 + regionInd] = "X";
					}
				newRow[numModernizedSys + 3] = numRegions;
			}
			list.add(newRow);
		}
		for (int bluInd = 0; bluInd < resFunc.bluList.size(); bluInd++) {
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.bluList.get(bluInd);
			newRow[1] = "BLU";
			colIndex = 0;
			int numSystems = 0;
			for (int sysInd = 0; sysInd < resFunc.sysList.size(); sysInd++)
				if (sysOpt.systemIsModernized[sysInd] > 0) {
					if (resFunc.systemBLUMatrix[sysInd][bluInd] == 1) {
						newRow[colIndex + 3] = "X";
						numSystems++;
					}
					colIndex++;
				}
			newRow[2] = numSystems;
			provideDataBLUFuture[dataList.size() + bluInd] = numSystems;
			if (includeRegionalization) {
				int numRegions = 0;
				for (int regionInd = 0; regionInd < resFunc.regionList.size(); regionInd++)
					if (resFunc.bluRegionProviderCount[bluInd][regionInd] >= 1) {
						numRegions++;
						newRow[numModernizedSys + 4 + regionInd] = "X";
					}
				newRow[numModernizedSys + 3] = numRegions;
			}
			list.add(newRow);
		}
		displayListOnTab(colNames, list, ((SysOptPlaySheet) playSheet).futureFuncPanel, true);
	}
	
	protected void displayHeatMap() {
		Hashtable dataHash = new Hashtable();
		String[] var = new String[] { "Modernized Systems", "Decommissioned Systems", "Value" };
		String xName = var[0]; // Modernized Systems
		String yName = var[1]; // Decommissioned Systems
		ArrayList<Integer> modIndicies = sysOpt.getModernizedIndicies();
		ArrayList<Integer> decommIndicies = sysOpt.getDecommissionedIndicies();
		
		for (Integer decommIndex : decommIndicies) {
			String decommSysName = sysList.get(decommIndex); // modernized system
			// check if this system has data objects in no longer supported list
			if(ignoreTheatGarr) {
				if (resFunc.didSystemProvideReducedDataBLUReg(decommIndex))
					decommSysName += "*";
			}else {
				if (resFunc.didSystemProvideReducedDataBLUTG(decommIndex))
					decommSysName += "*";
			}
			for (Integer modIndex : modIndicies) {
				String modSysName = sysList.get(modIndex); // modernized system
				Hashtable elementHash = new Hashtable();
				modSysName = modSysName.replaceAll("\"", "");
				decommSysName = decommSysName.replaceAll("\"", "");
				String key = modSysName + "-" + decommSysName;
				double count;
				if(ignoreTheatGarr) {
					count = resFunc.percentDataBLUReplacedReg(modIndex, decommIndex);
				}else {
					count =resFunc.percentDataBLUReplacedTG(modIndex, decommIndex);
				}
				
				elementHash.put(xName, modSysName);
				elementHash.put(yName, decommSysName);
				elementHash.put(var[2], count);
				dataHash.put(key, elementHash);
			}
		}
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		allHash.put("title", var[0] + " vs " + var[1]);
		allHash.put("xAxisTitle", var[0]);
		allHash.put("yAxisTitle", var[1]);
		allHash.put("value", var[2]);
		// display output for heatmap tab
		((SysOptPlaySheet) playSheet).replacementHeatMap.callIt(allHash);
		((SysOptPlaySheet) playSheet).replacementHeatMap.setVisible(true);
	}
	
	public void setComboBoxes() {
		
		Set<String> capSet = new TreeSet<String>(Arrays.asList(resFunc.systemCapability));
		
		for (String cap : capSet) {
			SysOptPlaySheet.capComboBox.addItem(cap);
			SysOptPlaySheet.geoCapComboBox.addItem(cap);
		}
		SysOptPlaySheet.capComboBox.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				String capSystemQuery = "SELECT ?Capability ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?System ?Supports ?Capability}{?Supports <http://semoss.org/ontologies/Relation/Contains/Calculated> \"Yes\"}}";
				HashMap<String, TreeSet<String>> capSystemMap = new HashMap<String, TreeSet<String>>();
				capSystemMap = QueryProcessor.getStringSetMap(capSystemQuery, "HR_Core");
				SysOptPlaySheet.sysComboBox.removeAllItems();
				
				capability = SysOptPlaySheet.capComboBox.getSelectedItem().toString();
				
				for (int i = 0; i < resFunc.sysList.size(); i++) {
					if (resFunc.systemCapability[i].equalsIgnoreCase(capability)) {
						SysOptPlaySheet.sysComboBox.addItem(resFunc.sysList.get(i));
					} else if (capSystemMap.get(capability) != null && capSystemMap.get(capability).contains(resFunc.sysList.get(i))) {
						SysOptPlaySheet.sysComboBox.addItem(resFunc.sysList.get(i) + "*");
					}
				}
				if (SysOptPlaySheet.sysComboBox.getSelectedItem() != null) {
					system = SysOptPlaySheet.sysComboBox.getSelectedItem().toString();
				}
				SysOptPlaySheet.sysComboBox.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						if (SysOptPlaySheet.sysComboBox.getSelectedItem() != null) {
							system = SysOptPlaySheet.sysComboBox.getSelectedItem().toString();
						}
					}
				});
			}
		});
		// GeoSpatialMap ComboBox
		SysOptPlaySheet.geoCapComboBox.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				geoSpatialMapSystemList = new ArrayList<String>();
				geoCapability = SysOptPlaySheet.geoCapComboBox.getSelectedItem().toString();
				
				String capSystemQuery = "SELECT ?Capability ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?System ?Supports ?Capability}{?Supports <http://semoss.org/ontologies/Relation/Contains/Calculated> \"Yes\"}}";
				HashMap<String, TreeSet<String>> capSystemMap = new HashMap<String, TreeSet<String>>();
				capSystemMap = QueryProcessor.getStringSetMap(capSystemQuery, "HR_Core");
				
				for (int i = 0; i < resFunc.sysList.size(); i++) {
					if (resFunc.systemCapability[i].equalsIgnoreCase(capability)) {
						geoSpatialMapSystemList.add(resFunc.sysList.get(i));
					} else if (capSystemMap.get(capability) != null && capSystemMap.get(capability).contains(resFunc.sysList.get(i))) {
						geoSpatialMapSystemList.add(resFunc.sysList.get(i) + "*");
					}
				}
			}
		});
		
	}
	
	public void displaySysCapComparison() {
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		HashMap<String, TreeSet<String>> capSystemMap = new HashMap<String, TreeSet<String>>();
		HashMap<String, ArrayList<String>> dataSystemMap = new HashMap<String, ArrayList<String>>();
		HashMap<String, ArrayList<String>> bluSystemMap = new HashMap<String, ArrayList<String>>();
		TreeSet<String> systemSet = new TreeSet<String>();
		
		String capSystemQuery = "SELECT ?Capability ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?System ?Supports ?Capability}{?Supports <http://semoss.org/ontologies/Relation/Contains/Calculated> \"Yes\"}}";
		capSystemMap = QueryProcessor.getStringSetMap(capSystemQuery, "HR_Core");
		
		int selectSysIndex = -1;
		if (system.contains("*")) {
			selectSysIndex = resFunc.sysList.indexOf(system.substring(0, system.indexOf("*")));
		} else {
			selectSysIndex = resFunc.sysList.indexOf(system);
		}
		
		for (int dataInd = 0; dataInd < resFunc.dataList.size(); dataInd++) {
			String tempData = resFunc.dataList.get(dataInd);
			if (resFunc.systemDataMatrix[selectSysIndex][dataInd] == 1) {
				dataSystemMap.put(tempData, new ArrayList<String>());
			}
			for (int sysInd = 0; sysInd < resFunc.sysList.size(); sysInd++) {
				String tempSystem = resFunc.sysList.get(sysInd);
				if (sysOpt.systemIsModernized[sysInd] == 0 && capSystemMap.get(capability) != null
						&& capSystemMap.get(capability).contains(tempSystem)) {
					capSystemMap.get(capability).remove(tempSystem);
				}
				if (sysOpt.systemIsModernized[sysInd] > 0 && resFunc.systemDataMatrix[sysInd][dataInd] == 1
						&& resFunc.systemDataMatrix[selectSysIndex][dataInd] == 1 && resFunc.systemCapability[sysInd].equalsIgnoreCase(capability)) {
					dataSystemMap.get(tempData).add(tempSystem);
					if (!systemSet.contains(tempSystem)) {
						systemSet.add(tempSystem);
					}
				} else if (sysOpt.systemIsModernized[sysInd] > 0 && resFunc.systemDataMatrix[sysInd][dataInd] == 1
						&& resFunc.systemDataMatrix[selectSysIndex][dataInd] == 1 && capSystemMap.get(capability) != null
						&& capSystemMap.get(capability).contains(tempSystem)) {
					dataSystemMap.get(tempData).add(tempSystem);
				}
			}
		}
		for (int bluInd = 0; bluInd < resFunc.bluList.size(); bluInd++) {
			for (int sysInd = 0; sysInd < resFunc.sysList.size(); sysInd++) {
				String tempBLU = resFunc.bluList.get(bluInd);
				if (!bluSystemMap.containsKey(tempBLU) && resFunc.systemBLUMatrix[selectSysIndex][bluInd] == 1) {
					bluSystemMap.put(tempBLU, new ArrayList<String>());
				}
				String tempSystem = resFunc.sysList.get(sysInd);
				if (sysOpt.systemIsModernized[sysInd] > 0 && resFunc.systemBLUMatrix[sysInd][bluInd] == 1
						&& resFunc.systemBLUMatrix[selectSysIndex][bluInd] == 1 && resFunc.systemCapability[sysInd].equalsIgnoreCase(capability)) {
					bluSystemMap.get(tempBLU).add(tempSystem);
					if (!systemSet.contains(tempSystem)) {
						systemSet.add(tempSystem);
					}
				} else if (sysOpt.systemIsModernized[sysInd] > 0 && resFunc.systemBLUMatrix[sysInd][bluInd] == 1
						&& resFunc.systemBLUMatrix[selectSysIndex][bluInd] == 1 && capSystemMap.get(capability) != null
						&& capSystemMap.get(capability).contains(tempSystem)) {
					bluSystemMap.get(tempBLU).add(tempSystem);
					
				}
			}
		}
		
		int size = systemSet.size() + 3;
		if (capSystemMap.get(capability) != null) {
			size += capSystemMap.get(capability).size();
		}
		
		String[] colNames = new String[size];// number of systems+2
		colNames[0] = "Data/BLU";
		colNames[1] = "Type";
		colNames[2] = "Number of Systems Providing";
		int colIndex = 0;
		if (capSystemMap.get(capability) != null) {
			for (String columnSystem : capSystemMap.get(capability)) {
				colNames[colIndex + 3] = columnSystem + "*";
				colIndex++;
			}
		}
		for (String columnSystem : systemSet) {
			colNames[colIndex + 3] = columnSystem;
			colIndex++;
		}
		
		for (String tempData : dataSystemMap.keySet()) {
			Object[] newRow = new Object[size];
			newRow[0] = tempData;
			newRow[1] = "Data";
			colIndex = 0;
			int numSystems = 0;
			if (capSystemMap.get(capability) != null) {
				for (String columnSystem : capSystemMap.get(capability)) {
					if (dataSystemMap.get(tempData).contains(columnSystem)) {
						newRow[colIndex + 3] = "X";
						numSystems++;
					}
					colIndex++;
				}
			}
			for (String columnSystem : systemSet) {
				if (dataSystemMap.get(tempData).contains(columnSystem)) {
					newRow[colIndex + 3] = "X";
					numSystems++;
				}
				colIndex++;
			}
			newRow[2] = numSystems;
			list.add(newRow);
		}
		for (String tempBLU : bluSystemMap.keySet()) {
			Object[] newRow = new Object[size];
			newRow[0] = tempBLU;
			newRow[1] = "BLU";
			colIndex = 0;
			int numSystems = 0;
			if (capSystemMap.get(capability) != null) {
				for (String columnSystem : capSystemMap.get(capability)) {
					if (bluSystemMap.get(tempBLU).contains(columnSystem)) {
						newRow[colIndex + 3] = "X";
						numSystems++;
					}
					colIndex++;
				}
			}
			for (String columnSystem : systemSet) {
				if (bluSystemMap.get(tempBLU).contains(columnSystem)) {
					newRow[colIndex + 3] = "X";
					numSystems++;
				}
				colIndex++;
			}
			newRow[2] = numSystems;
			list.add(newRow);
		}
		
		displayListOnTab(colNames, list, ((SysOptPlaySheet) playSheet).sysCapDisplayPanel, true);
	}
	
	public void displayGeoSpatialMap() {
		
		HashSet data = new HashSet();
		String[] var = { "System", "Lat", "Long", "size" };
		String getSystemCoordinatesQuery = "SELECT DISTINCT ?System ?Lat ?Long WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite}{?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite}{?DCSite <http://semoss.org/ontologies/Relation/Contains/LONG> ?Long}{?DCSite <http://semoss.org/ontologies/Relation/Contains/LAT> ?Lat}}";
		HashMap<String, ArrayList<String[]>> systemCoordinatesList = new HashMap<String, ArrayList<String[]>>();
		QueryProcessor.getStringTwoArrayListMap(getSystemCoordinatesQuery, "TAP_Site_Data");
		String systemKey = "";
		// Possibly filter out all US Facilities from the query?
		
		for (String system : geoSpatialMapSystemList) {
			if (system.contains("*")) {
				int asteriskIndex = system.indexOf("*");
				systemKey = system.substring(0, asteriskIndex);
			} else {
				systemKey = system;
			}
			for (String coordinates[] : systemCoordinatesList.get(systemKey)) {
				LinkedHashMap elementHash = new LinkedHashMap();
				elementHash.put("System", system);
				elementHash.put("Lat", Double.parseDouble(coordinates[0]));
				elementHash.put("Long", Double.parseDouble(coordinates[1]));
				elementHash.put("size", 1000000);
				data.add(elementHash);
			}
		}
		
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", data);
		
		allHash.put("lat", var[1]);
		allHash.put("lon", var[2]);
		if (var.length > 3 && !var[3].equals(null))
			allHash.put("size", var[3]);
		else
			allHash.put("size", "");
		allHash.put("locationName", var[0]);
		
		((SysOptPlaySheet) playSheet).geoSpatialMap.callIt(allHash);
		((SysOptPlaySheet) playSheet).geoSpatialMap.setVisible(true);
		
	}
	
	protected void createGraphData() {
		f.createLearningYearlyConstants((int) Math.ceil(optNumYears), scdLT, iniLC, scdLC);
		cumSavingsList = ((UnivariateSysOptFunction) f).createCumulativeSavings(budget, optNumYears);
		breakEvenList = ((UnivariateSysOptFunction) f).createBreakEven(budget, optNumYears);
		sustainCostList = ((UnivariateSysOptFunction) f).createSustainmentCosts(budget, optNumYears);
		installCostList = ((UnivariateSysOptFunction) f).createInstallCosts(budget, optNumYears);
	}
	
	protected void displayHeaderLabels() {
		
		String netSavingsString = Utility.sciToDollar(netSavings);
		((SysOptPlaySheet) playSheet).savingLbl.setText(netSavingsString);
		double roiVal = Utility.round(roi * 100, 2);
		((SysOptPlaySheet) playSheet).roiLbl.setText(Double.toString(roiVal) + "%");
		if ((netSavings < 0 && numMaintenanceSavings - serMainPerc * dataExposeCost < 0))
			((SysOptPlaySheet) playSheet).irrLbl.setText("N/A");
		else {
			double irrVal = Utility.round(irr * 100, 2);
			((SysOptPlaySheet) playSheet).irrLbl.setText(Double.toString(irrVal) + "%");
		}
		if (optNumYears > maxYears)
			((SysOptPlaySheet) playSheet).timeTransitionLbl.setText("Beyond Max Time");
		else {
			double timeTransition = Utility.round(optNumYears, 2);
			((SysOptPlaySheet) playSheet).timeTransitionLbl.setText(Double.toString(timeTransition) + " Years");
		}
		String annualBudgetString = Utility.sciToDollar(budget);
		((SysOptPlaySheet) playSheet).annualBudgetLbl.setText(annualBudgetString);
		
		int breakEvenYear = 0;
		for (int i = 0; i < breakEvenList.size(); i++) {
			if (breakEvenList.get(i) < 0)
				breakEvenYear = i + 1;
		}
		if (breakEvenList.get(breakEvenList.size() - 1) < 0)
			((SysOptPlaySheet) playSheet).bkevenLbl.setText("Beyond Max Time");
		else if (breakEvenYear == 0) {
			((SysOptPlaySheet) playSheet).bkevenLbl.setText("1 Year");
		} else {
			double amountInLastYear = breakEvenList.get(breakEvenYear) - breakEvenList.get(breakEvenYear - 1);
			double fraction = (-breakEvenList.get(breakEvenYear)) / amountInLastYear;
			double breakEven = Utility.round(breakEvenYear + 1 + fraction, 2);
			((SysOptPlaySheet) playSheet).bkevenLbl.setText(Double.toString(breakEven) + " Years");
		}
		
	}
	
	/**
	 * Displays the results from various optimization calculations. These include net savings, ROI, and IRR functions. This
	 * populates the overview tab Optimizer used for TAP-specific calculations.
	 */
	public void displayResults() {
		displaySolutionLabel();
		createGraphData();
		displayHeaderLabels();
		
		SysOptGraphFunctions graphF = new SysOptGraphFunctions();
		graphF.setOptimzer(this);
		
		// Hashtable modernizedSysHeatMapChartHash = graphF.createModernizedHeatMap();
		Hashtable chartHash3 = graphF.createCostChart();
		Hashtable chartHash4 = graphF.createCumulativeSavings();
		Hashtable chartHash5 = graphF.createBreakevenGraph();
		Hashtable chartHash6 = graphF.createLearningCurve();
		
		((SysOptPlaySheet) playSheet).tab3.callIt(chartHash3);
		((SysOptPlaySheet) playSheet).tab4.callIt(chartHash4);
		((SysOptPlaySheet) playSheet).tab5.callIt(chartHash5);
		((SysOptPlaySheet) playSheet).tab6.callIt(chartHash6);
		playSheet.setGraphsVisible(true);
	}
	
	public void displaySolutionLabel() {
		if (reducedFunctionality) {
			((SysOptPlaySheet) playSheet).solutionLbl
					.setText("Solution available with reduced functionality (see functionality analysis tab for details)");
			return;
		}
		
		double savingsROIOrIRR = 0.0;
		String savingsROIOrIRRString = "";
		if (f instanceof SysNetSavingsFunction) {
			savingsROIOrIRR = netSavings;
			savingsROIOrIRRString = "savings";
		} else if (f instanceof SysROIFunction) {
			savingsROIOrIRR = roi;
			savingsROIOrIRRString = "ROI";
		} else if (f instanceof SysIRRFunction) {
			savingsROIOrIRR = irr;
			savingsROIOrIRRString = "IRR";
		}
		
		String positiveOrNegativeString = "";
		if (savingsROIOrIRR >= 0.0)
			positiveOrNegativeString = "positive";
		if (savingsROIOrIRR < 0.0)
			positiveOrNegativeString = "negative";
		if (optNumYears > maxYears)
			((SysOptPlaySheet) playSheet).solutionLbl.setText("Construction takes longer than allotted " + maxYears + " Years");
		else
			((SysOptPlaySheet) playSheet).solutionLbl.setText("Solution available with " + positiveOrNegativeString + " " + savingsROIOrIRRString);
	}
	
}
