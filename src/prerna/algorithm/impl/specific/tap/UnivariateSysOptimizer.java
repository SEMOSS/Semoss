/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

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

import prerna.ui.components.specific.tap.DHMSMCapabilitySelectPanel;
import prerna.ui.components.specific.tap.DHMSMDataBLUSelectPanel;
import prerna.ui.components.specific.tap.DHMSMHelper;
import prerna.ui.components.specific.tap.DHMSMSystemSelectPanel;
import prerna.ui.components.specific.tap.SysOptGraphFunctions;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.util.Utility;

/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class UnivariateSysOptimizer extends UnivariateOpt{
	
	static final Logger logger = LogManager.getLogger(UnivariateSysOptimizer.class.getName());
	
	String bindStr = "";
	
	ResidualSystemOptFillData resFunc;
	public ResidualSystemOptimizer sysOpt;
	String sysQuery, dataQuery, bluQuery, regionQuery;
	public ArrayList<String> sysList, dataList, bluList, regionList;
	public ArrayList<String> systemMustModernize, systemMustDecommission;
	double dataExposeCost;
	double numMaintenanceSavings;
	double preTransitionMaintenanceCost;
	double postTransitionMaintenanceCost;
	public double budget=0.0, optNumYears = 0.0, netSavings = 0.0, roi=0.0,irr=0.0;
	boolean noErrors=true;
	boolean includeRegionalization = false;
	boolean ignoreTheatGarr = false;
	boolean includeTheater = false;
	boolean includeGarrison = false;
	String errorMessage = "";
	boolean reducedFunctionality = false;
	public ArrayList<Double> cumSavingsList, breakEvenList, sustainCostList, installCostList;
	private int[] provideDataBLUNow;
	private int[] provideDataBLUFuture;

	public void setSelectDropDowns(DHMSMSystemSelectPanel sysSelectPanel,DHMSMCapabilitySelectPanel capabilitySelectPanel,DHMSMDataBLUSelectPanel dataBLUSelectPanel,DHMSMSystemSelectPanel systemModernizePanel, DHMSMSystemSelectPanel systemDecommissionPanel,boolean includeRegionalization,boolean ignoreTheatGarr)
	{
		this.includeRegionalization = includeRegionalization;
		this.ignoreTheatGarr = ignoreTheatGarr;
		if(!sysSelectPanel.theaterSysCheckBox.isSelected()&&!sysSelectPanel.garrisonSysCheckBox.isSelected()) {
			this.includeTheater = true;
			this.includeGarrison = true;
		} else {
			this.includeTheater = sysSelectPanel.theaterSysCheckBox.isSelected();
			this.includeGarrison = sysSelectPanel.garrisonSysCheckBox.isSelected();
		}
		this.systemMustModernize = systemModernizePanel.getSelectedSystems();
		this.systemMustDecommission= systemDecommissionPanel.getSelectedSystems();
		
		this.sysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}}";
		this.sysQuery = addBindings("System",sysSelectPanel.getSelectedSystems(),sysQuery);
		if(includeRegionalization)
		{
			this.regionQuery = "SELECT DISTINCT ?Region WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?DeployedAt2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>} {?Includes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Includes>} {?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HealthServiceRegion>} {?Located <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Located>} {?System ?DeployedAt1 ?SystemDCSite} {?SystemDCSite ?DeployedAt2 ?DCSite} {?DCSite ?Includes ?MTF} {?MTF ?Located ?Region} } ORDER BY ASC(?Region) ";
			this.regionQuery = addBindings("System",sysSelectPanel.getSelectedSystems(),regionQuery);
		}
		//if there are data and blu provided, then use them
		if(!dataBLUSelectPanel.noneSelected())
		{
			this.dataQuery = "SELECT DISTINCT ?DataObject WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}";
			this.bluQuery = "SELECT DISTINCT ?BusinessLogicUnit WHERE { {?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}}";
			this.dataQuery = addBindings("DataObject",dataBLUSelectPanel.getSelectedData(),dataQuery);
			this.bluQuery = addBindings("BusinessLogicUnit",dataBLUSelectPanel.getSelectedBLU(),bluQuery);
		}//if there are capabilities provided use them
		else if(!capabilitySelectPanel.noneSelected())
		{
			this.dataQuery = "SELECT DISTINCT ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} }";
			this.bluQuery = "SELECT DISTINCT ?BLU WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BLU}}";
			this.dataQuery = addBindings("Capability",capabilitySelectPanel.getSelectedCapabilities(),dataQuery);
			this.bluQuery = addBindings("Capability",capabilitySelectPanel.getSelectedCapabilities(),bluQuery);

		}//otherwise use the systems list to generate
		else
		{
			this.dataQuery = "NULL";
			DHMSMHelper dhelp = new DHMSMHelper();
			dhelp.setUseDHMSMOnly(false);
			dhelp.runData(playSheet.engine);
			ArrayList<String> systems = sysSelectPanel.getSelectedSystems();
			dataList = new ArrayList<String>();
			for(int sysInd = 0;sysInd < systems.size();sysInd++)
			{
				String sys = systems.get(sysInd);
				dataList.addAll(dhelp.getAllDataFromSys(sys, "C"));
			}
			dataList = removeDuplicates(dataList);
			this.bluQuery = "SELECT DISTINCT ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU.}}";
			this.bluQuery = addBindings("System",sysSelectPanel.getSelectedSystems(),bluQuery);
		}
	}
	public ArrayList<String> removeDuplicates(ArrayList<String> list)
	{
		ArrayList<String> retList = new ArrayList<String>();
		for(String entry : list)
		{
			if(!retList.contains(entry))
				retList.add(entry);
		}
		return retList;
	}
	public String addBindings(String type, List<String> bindingsList,String query)
	{
		if(bindingsList.size()==0)
			return "";
		query += "BINDINGS ?"+type+" {";
		for(int i=0;i<bindingsList.size();i++)
			query+="(<http://health.mil/ontologies/Concept/"+type+"/"+(String)bindingsList.get(i)+">)";
		query+="}";
		return query;
	}
	public void getData()
	{
		String engine = playSheet.engine.getEngineName();
		
		if(ignoreTheatGarr)
			resFunc = new ResidualSystemOptFillData();
		else
			resFunc = new ResidualSystemTheatGarrOptFillData();
		resFunc.setMaxYears(maxYears);
		this.sysList = resFunc.runListQuery(engine,sysQuery);
		if(this.sysList.size()<2)
		{
			if(this.sysList.size()==0)
				errorMessage = "No systems were selected. Please select systems under the Select System Functionality tab.";
			else if(sysList.size() == 1)
				errorMessage = "Only one system exists was selected. Please select more than one system under the Select System Functionality tab.";
			noErrors = false;
			return;
		}
		if(includeRegionalization)
			this.regionList = resFunc.runListQuery("TAP_Site_Data",regionQuery);
		if(!dataQuery.equals("NULL"))
			this.dataList = resFunc.runListQuery(engine,dataQuery);
		this.bluList = resFunc.runListQuery(engine,bluQuery);
		if(this.dataList.size()==0 && this.bluList.size()==0 )
		{
			errorMessage = "No data objects or business logic units were selected. Please select at least one under the Select System Functionality tab.";
			noErrors = false;
			return;
		}
		resFunc.setPlaySheet((SysOptPlaySheet)playSheet);
		if(includeRegionalization)
			resFunc.setSysDataBLULists(deepCopy(sysList),deepCopy(dataList),deepCopy(bluList),deepCopy(regionList),deepCopy(systemMustModernize),deepCopy(systemMustDecommission));
		else
			resFunc.setSysDataBLULists(deepCopy(sysList),deepCopy(dataList),deepCopy(bluList),null,deepCopy(systemMustModernize),deepCopy(systemMustDecommission));

		if(resFunc instanceof ResidualSystemTheatGarrOptFillData) {
			reducedFunctionality = ((ResidualSystemTheatGarrOptFillData)resFunc).fillDataStores(includeTheater,includeGarrison);
		} else {
			reducedFunctionality = resFunc.fillDataStores();
		}

		if(resFunc.doManualModDecommOverlap()) {
			errorMessage = "There is at least one system on the manually modernize and manually decommission. Please resolve the lists.";
			noErrors = false;
			return;
		}
	}
	public ArrayList<String> deepCopy(ArrayList<String> list)
	{
		ArrayList<String> retList = new ArrayList<String>();
		for(String element : list)
		{
			retList.add(element);
		}
		return retList;
	}
	public void getModernizedSysList()
	{
		playSheet.progressBar.setString("Determining Modernized List");
		if(ignoreTheatGarr)
			sysOpt = new ResidualSystemOptimizer();
		else
			sysOpt = new ResidualSystemTheatGarrOptimizer();
		sysOpt.setPlaySheet(playSheet);
		sysOpt.setDataSet(this.sysList,this.dataList,this.bluList,this.regionList,resFunc.systemDataMatrix,resFunc.systemBLUMatrix,resFunc.systemCostOfDataMatrix,resFunc.systemRegionMatrix,resFunc.systemCostOfMaintenance,resFunc.systemCostOfDB,resFunc.systemNumOfSites,resFunc.systemModernize,resFunc.systemDecommission,resFunc.dataRegionSORSystemCountReduced,resFunc.bluRegionProviderCountReduced);

		if(ignoreTheatGarr)
			noErrors = sysOpt.runOpt();
		else {
			((ResidualSystemTheatGarrOptimizer)sysOpt).setTheatGarrDataSet(((ResidualSystemTheatGarrOptFillData)resFunc).systemTheater,((ResidualSystemTheatGarrOptFillData)resFunc).systemGarrison,((ResidualSystemTheatGarrOptFillData)resFunc).dataRegionSORSystemTheaterCountReduced,((ResidualSystemTheatGarrOptFillData)resFunc).dataRegionSORSystemGarrisonCountReduced,((ResidualSystemTheatGarrOptFillData)resFunc).bluRegionProviderTheaterCountReduced,((ResidualSystemTheatGarrOptFillData)resFunc).bluRegionProviderGarrisonCountReduced);
			noErrors = ((ResidualSystemTheatGarrOptimizer)sysOpt).runOpt();
		}

		errorMessage = sysOpt.errorMessage;

		this.dataExposeCost = sysOpt.numTransformationTotal; //total cost to expose all data for all systems at all sites
		this.numMaintenanceSavings =sysOpt.numMaintenanceTotal;
		this.preTransitionMaintenanceCost = sysOpt.denomCurrentMaintenance;
		this.postTransitionMaintenanceCost = preTransitionMaintenanceCost - numMaintenanceSavings;
	}
	public void optimize()
	{
		getData();
		if(noErrors == false)
		{
			playSheet.progressBar.setVisible(false);
			Utility.showError(errorMessage);
			return;
		}
		getModernizedSysList();
		if(noErrors == false)
		{
			playSheet.progressBar.setVisible(false);
			Utility.showError(errorMessage);
			return;
		}
		if(numMaintenanceSavings < serMainPerc*dataExposeCost)
		{
        	//playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+"Potential annual sustainment savings is less than annual maintenance of exposed data objects. Rationalization solution is not available.");
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+"Cost of current annual maintenance of systems is the same as rationalized annual maintenance. Rationalization solution is not available.");
			playSheet.progressBar.setVisible(false);
			//Utility.showError("Potential annual sustainment savings is less than annual maintenance of exposed data objects. Rationalization solution is not available.");
			Utility.showError("Cost of current annual maintenance of systems is the same as rationalized annual maintenance. Rationalization solution is not available.");
			return;
		}
		
        progressBar = playSheet.progressBar;
        f.setConsoleArea(playSheet.consoleArea);
        f.setProgressBar(progressBar);
        f.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        ((UnivariateSysOptFunction)f).setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost,scdLT, iniLC, scdLC);
        if(f instanceof SysIRRFunction)
        	((SysIRRFunction)f).createLinearInterpolation();
        ((UnivariateSysOptFunction)f).createYearAdjuster(sysList, dataList, hourlyCost);

        //budget in LOE
        UnivariateOptimizer optimizer = new BrentOptimizer(.001, .001);
        RandomGenerator rand = new Well1024a(500);
        MultiStartUnivariateOptimizer multiOpt = new MultiStartUnivariateOptimizer(optimizer, noOfPts, rand);
        UnivariateObjectiveFunction objF = new UnivariateObjectiveFunction(f);
        SearchInterval search = new SearchInterval(minBudget,maxBudget); //budget in LOE
        optimizer.getStartValue();
        MaxEval eval = new MaxEval(200);
        
        OptimizationData[] data = new OptimizationData[]{search, objF, GoalType.MAXIMIZE, eval};
        try {
            UnivariatePointValuePair pair = multiOpt.optimize(data);
            progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
            if(((UnivariateSysOptFunction)f).solutionExists)
            {
	            budget = pair.getPoint();
	            optNumYears = ((UnivariateSysOptFunction)f).calculateYears(budget);
	            optNumYears =  ((UnivariateSysOptFunction)f).yearAdjuster.adjustTimeToTransform(budget, optNumYears);
	            if(optNumYears<1)
	            {
	            	optNumYears = 1;
	            	budget = ((UnivariateSysOptFunction)f).calculateBudgetForOneYear();
	            	//budget = ((SysNetSavingsFunction)f).workPerformedArray.get(0);
	            }
	            calculateSavingsROIAndIRR();
            }
            else
            {
            	((SysOptPlaySheet)playSheet).solutionLbl.setText("No solution available within the given time frame");
    			return;
            }
        } catch (TooManyEvaluationsException fee) {
        	noErrors = false;
        	playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+fee);
            progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
            clearPlaysheet();
        }
        //should this go in the try catch? prob shouldnt do it if we get an error....
        //runOptIteration();
        if(noErrors)
        {
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nBudget: "+budget);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nNumber of Years to consolidate systems: "+optNumYears);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nGiven timespan to accumulate savings over: "+maxYears);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nCumulative savings: "+netSavings);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nROI: "+roi);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nIRR: "+irr);
	        displayResults();
	        displaySystemSpecifics();
	        displayCurrFunctionality();
	        displayFutureFunctionality();
	        displayHeatMap();
	        displayClusterHeatMap();
        }
        else
		{
        	playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+"No possible Internal Rate of Return. Rationalization solution is not available.");
			playSheet.progressBar.setVisible(false);
			Utility.showError("No possible Internal Rate of Return. Rationalization solution is not available.");
			return;
		}
		
	}
	
	public void calculateSavingsROIAndIRR()
	{

        SysNetSavingsFunction savingsF = new SysNetSavingsFunction();
        savingsF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        savingsF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost, scdLT, iniLC, scdLC);
        netSavings = savingsF.calculateRet(budget,optNumYears);

        SysROIFunction roiF = new SysROIFunction();
        roiF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        roiF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost, scdLT, iniLC, scdLC);
        roi = roiF.calculateRet(budget,optNumYears);
        
        SysIRRFunction irrF = new SysIRRFunction();
        irrF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        irrF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost, scdLT, iniLC, scdLC);
        irrF.createLinearInterpolation();
        irr = irrF.calculateRet(budget,optNumYears,netSavings);        
//		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+irrF.linInt.printString);	   	 
        if(f instanceof SysIRRFunction)
        {
        	disRate = irr;
        	if(irr==-1.0E30)
        		noErrors = false;
        }
	}
	
	public int sumRow(int[] row)
	{
		int sum=0;
		for(int i=0;i<row.length;i++)
			sum+=row[i];
		return sum;
	}
	
	public void displaySystemSpecifics()
	{
		ArrayList <Object []> list = new ArrayList<Object []>();
		String[] colNames;
		if(resFunc instanceof ResidualSystemTheatGarrOptFillData) {
			int size = 8;
			if(includeRegionalization)
				size=9;
			colNames = new String[size];
			colNames[0]="System";
			colNames[1]="Probability";
			colNames[2]="MHS Specific";
			colNames[3]="Theater";
			colNames[4]="Garrison";
			colNames[5]="Action";
			colNames[6]="Number Of Data Provided";
			colNames[7]="Number of BLU Provided";
			if(includeRegionalization)
				colNames[8]="Number of Regions";
			for (int i = 0;i<sysList.size();i++)
			{
				Object[] newRow = new Object[size];
				newRow[0] = resFunc.sysList.get(i);
				newRow[1] = resFunc.systemLPI[i];
				newRow[2] = resFunc.systemMHSSpecific[i];
				if(((ResidualSystemTheatGarrOptFillData)resFunc).systemTheater!=null&&((ResidualSystemTheatGarrOptFillData)resFunc).systemTheater[i]>0)
					newRow[3] = "X";
				if(((ResidualSystemTheatGarrOptFillData)resFunc).systemGarrison!=null&&((ResidualSystemTheatGarrOptFillData)resFunc).systemGarrison[i]>0)
					newRow[4] = "X";
			
				if(sysOpt.systemIsModernized[i]>0)
					newRow[5] = "Modernize";
				else
					newRow[5] = "Decommission";
				newRow[6] = sumRow(resFunc.systemDataMatrix[i]);
				newRow[7] = sumRow(resFunc.systemBLUMatrix[i]);
				if(includeRegionalization)
					newRow[8] = sumRow(resFunc.systemRegionMatrix[i]);
				list.add(newRow);
			}
		} else {
			int size = 6;
			if(includeRegionalization)
				size=7;
			colNames = new String[size];
			colNames[0]="System";
			colNames[1]="Probability";
			colNames[2]="MHS Specific";
			colNames[3]="Action";
			colNames[4]="Number Of Data Provided";
			colNames[5]="Number of BLU Provided";
			if(includeRegionalization)
				colNames[6]="Number of Regions";
			for (int i = 0;i<sysList.size();i++)
			{
				Object[] newRow = new Object[size];
				newRow[0] = resFunc.sysList.get(i);
				newRow[1] = resFunc.systemLPI[i];
				newRow[2] = resFunc.systemMHSSpecific[i];
				if(sysOpt.systemIsModernized[i]>0)
					newRow[3] = "Modernize";
				else
					newRow[3] = "Decommission";
				newRow[4] = sumRow(resFunc.systemDataMatrix[i]);
				newRow[5] = sumRow(resFunc.systemBLUMatrix[i]);
				if(includeRegionalization)
					newRow[6] = sumRow(resFunc.systemRegionMatrix[i]);
				list.add(newRow);
			}
		}
		displayListOnTab(colNames,list,((SysOptPlaySheet)playSheet).specificSysAlysPanel);
	}
	private void displayCurrFunctionality() {
		provideDataBLUNow = new int[dataList.size()+bluList.size()];
		ArrayList <Object []> list = new ArrayList<Object []>();
		int size = resFunc.sysList.size()+3;
		if(includeRegionalization)
			size += resFunc.regionList.size()+1;
		String[] colNames = new String[size];//number of systems+2
		colNames[0]="Data/BLU";
		colNames[1]="Type";
		colNames[2]="Number of Systems Providing";
		for(int i=0;i<resFunc.sysList.size();i++)
			colNames[i+3] = resFunc.sysList.get(i);
		if(includeRegionalization)
		{
			colNames[resFunc.sysList.size()+3] = "Number of Regions Provided At";
			for(int i=0;i<resFunc.regionList.size();i++)
				colNames[resFunc.sysList.size()+4+i] = "Region "+resFunc.regionList.get(i);
		}
		for (int dataInd = 0;dataInd<resFunc.dataList.size();dataInd++)
		{
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.dataList.get(dataInd);
			newRow[1] = "Data";
			//newRow[2] = sumRow(resFunc.systemDataMatrix[dataInd]);
			int numSystems = 0;
			for(int sysInd=0;sysInd<resFunc.sysList.size();sysInd++)
				if(resFunc.systemDataMatrix[sysInd][dataInd]==1) {
					newRow[sysInd+3] = "X";
					numSystems++;
				}
			newRow[2] = numSystems;
			provideDataBLUNow[dataInd] = numSystems;
			if(includeRegionalization)
			{
				int numRegions = 0;
				for(int regionInd=0;regionInd<resFunc.regionList.size();regionInd++)
					if(resFunc.dataRegionSORSystemCount[dataInd][regionInd]>=1)
					{
						numRegions++;
						newRow[resFunc.sysList.size()+4+regionInd] = "X";
					}
				newRow[resFunc.sysList.size()+3] = numRegions;
			}
			list.add(newRow);
		}
		for (int bluInd = 0;bluInd<resFunc.bluList.size();bluInd++)
		{
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.bluList.get(bluInd);
			newRow[1] = "BLU";
			//newRow[2] = sumRow(resFunc.systemBLUMatrix[bluInd]);
			int numSystems = 0;
			for(int sysInd=0;sysInd<resFunc.sysList.size();sysInd++)
				if(resFunc.systemBLUMatrix[sysInd][bluInd]==1) {
					newRow[sysInd+3] = "X";
					numSystems++;
				}
			newRow[2] = numSystems;
			provideDataBLUNow[dataList.size()+bluInd] = numSystems;
			if(includeRegionalization)
			{
				int numRegions = 0;
				for(int regionInd=0;regionInd<resFunc.regionList.size();regionInd++)
					if(resFunc.bluRegionProviderCount[bluInd][regionInd]>=1)
					{
						numRegions++;
						newRow[resFunc.sysList.size()+4+regionInd] = "X";
					}
				newRow[resFunc.sysList.size()+3] = numRegions;
			}
			list.add(newRow);
		}
		displayListOnTab(colNames,list,((SysOptPlaySheet)playSheet).currentFuncPanel,true);
	}
	
	private void displayFutureFunctionality() {
		provideDataBLUFuture = new int[dataList.size()+bluList.size()];
		
		ArrayList <Object []> list = new ArrayList<Object []>();
		int numModernizedSys = sysOpt.countModernized();
		int size = numModernizedSys+3;
		if(includeRegionalization)
			size += resFunc.regionList.size()+1;
		String[] colNames = new String[size];//number of systems+2
		colNames[0]="Data/BLU";
		colNames[1]="Type";
		colNames[2]="Number of Systems Providing";
		int colIndex=0;
		for(int i=0;i<resFunc.sysList.size();i++) {
			if(sysOpt.systemIsModernized[i]>0) {
				colNames[colIndex+3] = resFunc.sysList.get(i);
				colIndex++;
			}
		}
		if(includeRegionalization) {
			colNames[numModernizedSys+3] = "Number of Regions Provided At";
			for(int i=0;i<resFunc.regionList.size();i++)
				colNames[numModernizedSys+4+i] = "Region "+resFunc.regionList.get(i);
		}
		for (int dataInd = 0;dataInd<resFunc.dataList.size();dataInd++) {
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.dataList.get(dataInd);
			newRow[1] = "Data";
			colIndex=0;
			int numSystems = 0;
			for(int sysInd=0;sysInd<resFunc.sysList.size();sysInd++)
				if(sysOpt.systemIsModernized[sysInd]>0) {
					if(resFunc.systemDataMatrix[sysInd][dataInd]==1) {
						newRow[colIndex+3] = "X";
						numSystems++;
					}
					colIndex++;
				}
			newRow[2] = numSystems;
			provideDataBLUFuture[dataInd] = numSystems;
			if(includeRegionalization) {
				int numRegions = 0;
				for(int regionInd=0;regionInd<resFunc.regionList.size();regionInd++)
					if(resFunc.dataRegionSORSystemCount[dataInd][regionInd]>=1)
					{
						numRegions++;
						newRow[numModernizedSys+4+regionInd] = "X";
					}
				newRow[numModernizedSys+3] = numRegions;
			}
			list.add(newRow);
		}
		for (int bluInd = 0;bluInd<resFunc.bluList.size();bluInd++) {
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.bluList.get(bluInd);
			newRow[1] = "BLU";
			colIndex=0;
			int numSystems = 0;
			for(int sysInd=0;sysInd<resFunc.sysList.size();sysInd++)
				if(sysOpt.systemIsModernized[sysInd]>0) {
					if(resFunc.systemBLUMatrix[sysInd][bluInd]==1) {
						newRow[colIndex+3] = "X";
						numSystems++;
					}
					colIndex++;
				}
			newRow[2] = numSystems;
			provideDataBLUFuture[dataList.size()+bluInd] = numSystems;
			if(includeRegionalization) {
				int numRegions = 0;
				for(int regionInd=0;regionInd<resFunc.regionList.size();regionInd++)
					if(resFunc.bluRegionProviderCount[bluInd][regionInd]>=1)
					{
						numRegions++;
						newRow[numModernizedSys+4+regionInd] = "X";
					}
				newRow[numModernizedSys+3] = numRegions;
			}
			list.add(newRow);
		}
		displayListOnTab(colNames,list,((SysOptPlaySheet)playSheet).futureFuncPanel,true);
	}
	
	public void displayHeatMap() {
		Hashtable dataHash = new Hashtable();
		String[] var = new String[]{"Modernized Systems","Decommissioned Systems","Value"};
		String xName = var[0]; //Modernized Systems
		String yName = var[1]; //Decommissioned Systems
		ArrayList<Integer> modIndicies = sysOpt.getModernizedIndicies();
		ArrayList<Integer> decommIndicies = sysOpt.getDecommissionedIndicies();

		for(Integer decommIndex : decommIndicies) {
			String decommSysName = sysList.get(decommIndex); //modernized system
			//check if this system has data objects in no longer supported list
			if(resFunc.didSystemProvideReducedDataBLU(decommIndex))
				decommSysName +="*";
			for(Integer modIndex : modIndicies) {
				String modSysName = sysList.get(modIndex); //modernized system
				Hashtable elementHash = new Hashtable();
				modSysName = modSysName.replaceAll("\"", "");
				decommSysName = decommSysName.replaceAll("\"", "");
				String key = modSysName +"-"+decommSysName;
				double count = resFunc.percentDataBLUReplaced(modIndex,decommIndex);
				elementHash.put(xName, modSysName);
				elementHash.put(yName, decommSysName);
				elementHash.put(var[2], count);
				dataHash.put(key, elementHash);
			}
		}
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		allHash.put("title",  var[0] + " vs " + var[1]);
		allHash.put("xAxisTitle", var[0]);
		allHash.put("yAxisTitle", var[1]);
		allHash.put("value", var[2]);
		// display output for heatmap tab
		((SysOptPlaySheet) playSheet).replacementHeatMap.callIt(allHash);
		((SysOptPlaySheet) playSheet).replacementHeatMap.setVisible(true);
	}
	
	public void displayClusterHeatMap() {
		ClusterHeatMapData dataCreater = new ClusterHeatMapData();
		if(resFunc instanceof ResidualSystemTheatGarrOptFillData)
			dataCreater.setData(sysList, dataList, bluList, sysOpt.systemIsModernized, resFunc.systemLPI, resFunc.systemMHSSpecific, ((ResidualSystemTheatGarrOptFillData)resFunc).systemTheater, ((ResidualSystemTheatGarrOptFillData)resFunc).systemGarrison, provideDataBLUNow, provideDataBLUFuture, resFunc.systemDataMatrix, resFunc.systemBLUMatrix);
		else
			dataCreater.setData(sysList, dataList, bluList, sysOpt.systemIsModernized, resFunc.systemLPI, resFunc.systemMHSSpecific, null, null, provideDataBLUNow, provideDataBLUFuture, resFunc.systemDataMatrix, resFunc.systemBLUMatrix);
		dataCreater.createFile();
		// display output for heatmap tab
//		((SysOptPlaySheet) playSheet).clusterHeatMap.callIt(allHash);
//		((SysOptPlaySheet) playSheet).clusterHeatMap.setVisible(true);
	}

	/**
	 * Displays the results from various optimization calculations.
	 * These include net savings, ROI, and IRR functions.
	 * This populates the overview tab
	 * Optimizer used for TAP-specific calculations. 
	 */
	public void displayResults()
	{
		displaySolutionLabel();
		
		f.createLearningYearlyConstants((int)Math.ceil(optNumYears), scdLT, iniLC, scdLC);
		cumSavingsList = ((UnivariateSysOptFunction)f).createCumulativeSavings(budget, optNumYears);
		breakEvenList = ((UnivariateSysOptFunction)f).createBreakEven(budget, optNumYears);
		sustainCostList = ((UnivariateSysOptFunction)f).createSustainmentCosts(budget, optNumYears);
		installCostList = ((UnivariateSysOptFunction)f).createInstallCosts(budget, optNumYears);
		
		String netSavingsString = Utility.sciToDollar(netSavings);
		((SysOptPlaySheet)playSheet).savingLbl.setText(netSavingsString);
		double roiVal = Utility.round(roi*100, 2);
		((SysOptPlaySheet)playSheet).roiLbl.setText(Double.toString(roiVal)+"%");
		if((netSavings<0&&numMaintenanceSavings - serMainPerc*dataExposeCost<0))
			((SysOptPlaySheet)playSheet).irrLbl.setText("N/A");
		else {
			double irrVal = Utility.round(irr*100,2);
			((SysOptPlaySheet)playSheet).irrLbl.setText(Double.toString(irrVal)+"%");
		}
		if(optNumYears>maxYears)
			((SysOptPlaySheet)playSheet).timeTransitionLbl.setText("Beyond Max Time");
		else {
			double timeTransition = Utility.round(optNumYears,2);
			((SysOptPlaySheet)playSheet).timeTransitionLbl.setText(Double.toString(timeTransition)+" Years");
		}
		String annualBudgetString = Utility.sciToDollar(budget);
		((SysOptPlaySheet)playSheet).annualBudgetLbl.setText(annualBudgetString); 
		
		int breakEvenYear = 0;
		for(int i=0;i<breakEvenList.size();i++) {
			if(breakEvenList.get(i)<0)
				breakEvenYear = i+1;
		}
		if(breakEvenList.get(breakEvenList.size()-1)<0)
			((SysOptPlaySheet)playSheet).bkevenLbl.setText("Beyond Max Time");
		else if(breakEvenYear == 0) {
			((SysOptPlaySheet)playSheet).bkevenLbl.setText("1 Year");
		} else {
			double amountInLastYear = breakEvenList.get(breakEvenYear)-breakEvenList.get(breakEvenYear-1);
			double fraction = ( - breakEvenList.get(breakEvenYear))/amountInLastYear;
			double breakEven = Utility.round(breakEvenYear+1+fraction,2);
			((SysOptPlaySheet)playSheet).bkevenLbl.setText(Double.toString(breakEven)+" Years");
		}
		
		SysOptGraphFunctions graphF= new SysOptGraphFunctions();
		graphF.setOptimzer(this);

		//Hashtable modernizedSysHeatMapChartHash = graphF.createModernizedHeatMap();
		Hashtable chartHash3 = graphF.createCostChart();
		Hashtable chartHash4 = graphF.createCumulativeSavings();
		Hashtable chartHash5 = graphF.createBreakevenGraph();
		Hashtable chartHash6 = graphF.createLearningCurve();

		((SysOptPlaySheet)playSheet).tab3.callIt(chartHash3);
		((SysOptPlaySheet)playSheet).tab4.callIt(chartHash4);
		((SysOptPlaySheet)playSheet).tab5.callIt(chartHash5);
		((SysOptPlaySheet)playSheet).tab6.callIt(chartHash6);
		playSheet.setGraphsVisible(true);
	}
	
	public void displaySolutionLabel()
	{
		if(reducedFunctionality) {
			((SysOptPlaySheet)playSheet).solutionLbl.setText("Solution available with reduced functionality (see functionality analysis tab for details)");
			return;
		}
		
		double savingsROIOrIRR = 0.0;
		String savingsROIOrIRRString = "";
		if(f instanceof SysNetSavingsFunction) {
			savingsROIOrIRR = netSavings;
			savingsROIOrIRRString = "savings";
		} else if(f instanceof SysROIFunction) {
			savingsROIOrIRR = roi;
			savingsROIOrIRRString = "ROI";
		} else if(f instanceof SysIRRFunction) {
			savingsROIOrIRR = irr;
			savingsROIOrIRRString = "IRR";				
		}
		
		String positiveOrNegativeString = "";
		if(savingsROIOrIRR >= 0.0)
			positiveOrNegativeString = "positive";
		if(savingsROIOrIRR < 0.0)
			positiveOrNegativeString = "negative";
		if(optNumYears>maxYears)
			((SysOptPlaySheet)playSheet).solutionLbl.setText("Construction takes longer than allotted " + maxYears + " Years");
		else
			((SysOptPlaySheet)playSheet).solutionLbl.setText("Solution available with "+positiveOrNegativeString+" "+savingsROIOrIRRString);
	}
	
	
}
