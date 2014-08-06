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
	double dataExposeCost;
	double numMaintenanceSavings;
	double preTransitionMaintenanceCost;
	double postTransitionMaintenanceCost;
	public double budget=0.0, optNumYears = 0.0, netSavings = 0.0, roi=0.0,irr=0.0;
	boolean noErrors=true;
	boolean includeRegionalization = false;
	String errorMessage = "";
	boolean reducedFunctionality = false;
	public ArrayList<Double> cumSavingsList, breakEvenList, sustainCostList, installCostList;

	public void setSelectDropDowns(DHMSMSystemSelectPanel sysSelectPanel,DHMSMCapabilitySelectPanel capabilitySelectPanel,DHMSMDataBLUSelectPanel dataBLUSelectPanel,boolean useSysList,boolean useDataBLU,boolean includeRegionalization)
	{
		this.includeRegionalization = includeRegionalization;
		this.sysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}}";
		this.sysQuery = addBindings("System",sysSelectPanel.getSelectedSystems(),sysQuery);
		if(includeRegionalization)
		{
			this.regionQuery = "SELECT DISTINCT ?Region WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?DeployedAt2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>} {?Includes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Includes>} {?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HealthServiceRegion>} {?Located <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Located>} {?System ?DeployedAt1 ?SystemDCSite} {?SystemDCSite ?DeployedAt2 ?DCSite} {?DCSite ?Includes ?MTF} {?MTF ?Located ?Region} } ORDER BY ASC(?Region) ";
			this.regionQuery = addBindings("System",sysSelectPanel.getSelectedSystems(),regionQuery);
		}
		if(useDataBLU)
		{
			this.dataQuery = "SELECT DISTINCT ?DataObject WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}";
			this.bluQuery = "SELECT DISTINCT ?BusinessLogicUnit WHERE { {?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}}";
			this.dataQuery = addBindings("DataObject",dataBLUSelectPanel.getSelectedData(),dataQuery);
			this.bluQuery = addBindings("BusinessLogicUnit",dataBLUSelectPanel.getSelectedBLU(),bluQuery);
		}
		else if(useSysList)
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
		else if(!useSysList)
		{
			this.dataQuery = "SELECT DISTINCT ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} }";
			this.bluQuery = "SELECT DISTINCT ?BLU WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BLU}}";
			this.dataQuery = addBindings("Capability",capabilitySelectPanel.getSelectedCapabilities(),dataQuery);
			this.bluQuery = addBindings("Capability",capabilitySelectPanel.getSelectedCapabilities(),bluQuery);
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
//	public void setQueries(String sysQuery, String dataQuery, String bluQuery, String regionQuery)
//	{
//		this.sysQuery = sysQuery;
//		this.dataQuery = dataQuery;
//		this.bluQuery = bluQuery;
//		this.regionQuery = regionQuery;
//	}
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
		
		resFunc = new ResidualSystemOptFillData();
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
			resFunc.setSysDataBLULists(deepCopy(sysList),deepCopy(dataList),deepCopy(bluList),deepCopy(regionList));
		else
			resFunc.setSysDataBLULists(deepCopy(sysList),deepCopy(dataList),deepCopy(bluList),null);
		reducedFunctionality = resFunc.fillDataStores(!dataQuery.equals("NULL"));
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
		sysOpt = new ResidualSystemOptimizer();
		sysOpt.setPlaySheet(playSheet);
//		if(includeRegionalization)
			sysOpt.setDataSet(this.sysList,this.dataList,this.bluList,this.regionList,resFunc.systemDataMatrix,resFunc.systemBLUMatrix,resFunc.systemCostOfDataMatrix,resFunc.systemRegionMatrix,resFunc.systemCostOfMaintenance,resFunc.systemCostOfDB,resFunc.systemNumOfSites,resFunc.systemRequired,resFunc.dataRegionSORSystemExists,resFunc.bluRegionProviderExists);
//		else
//			sysOpt.setDataSet(this.sysList,this.dataList,this.bluList,null,resFunc.systemDataMatrix,resFunc.systemBLUMatrix,resFunc.systemCostOfDataMatrixresFunc.systemRegionMatrix,,resFunc.systemCostOfMaintenance,resFunc.systemCostOfDB,resFunc.systemNumOfSites,resFunc.systemRequired,resFunc.dataRegionSORSystemExists,resFunc.bluRegionProviderExists);
		noErrors = sysOpt.runOpt();
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
        
        //getting the deployment strategy
//		SysDecommissionOptimizationFunctions optFunctions = new SysDecommissionOptimizationFunctions();
//		optFunctions.setSysList(sysList);
//		optFunctions.setDataList(dataList);
//		optFunctions.setHourlyCost(hourlyCost);
//		optFunctions.optimize(budget, optNumYears);		
//		optFunctions.timeConstraint = optNumYears;
//		optFunctions.resourcesConstraint = budget;
//		optFunctions.optimizeResource();
//		list = optFunctions.outputList;
//		column names: "System","Time to Transform","Number of Sites Deployed At","Resource Allocation","Number of Systems Transformed Simultaneously","Total Cost for System";

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
	        displayFunctionalitySpecifics();
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
		int size = 4;
		if(includeRegionalization)
			size=5;
		String[] colNames = new String[size];
		colNames[0]="System";
		colNames[1]="Action";
		colNames[2]="Number Of Data Provided";
		colNames[3]="Number of BLU Provided";
		if(includeRegionalization)
			colNames[4]="Number of Regions";
		for (int i = 0;i<sysList.size();i++)
		{
			Object[] newRow = new Object[size];
			newRow[0] = resFunc.sysList.get(i);
			if(sysOpt.systemIsModernized[i]>0)
				newRow[1] = "Modernize";
			else
				newRow[1] = "Decommission";
			newRow[2] = sumRow(resFunc.systemDataMatrix[i]);
			newRow[3] = sumRow(resFunc.systemBLUMatrix[i]);
			if(includeRegionalization)
				newRow[4] = sumRow(resFunc.systemRegionMatrix[i]);
			list.add(newRow);
		}
		displayListOnTab(colNames,list,((SysOptPlaySheet)playSheet).specificSysAlysPanel);
	}
	public void displayFunctionalitySpecifics()
	{
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
			newRow[2] = sumRow(resFunc.dataRegionSORSystemCount[dataInd]);
			for(int sysInd=0;sysInd<resFunc.sysList.size();sysInd++)
				if(resFunc.systemDataMatrix[sysInd][dataInd]==1)
					newRow[sysInd+3] = "X";
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
			newRow[2] = sumRow(resFunc.bluRegionProviderCount[bluInd]);
			for(int sysInd=0;sysInd<resFunc.sysList.size();sysInd++)
				if(resFunc.systemBLUMatrix[sysInd][bluInd]==1)
					newRow[sysInd+3] = "X";
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
		displayListOnTab(colNames,list,((SysOptPlaySheet)playSheet).specificFuncAlysPanel);
	}

	/**
	 * Displays the results from various optimization calculations. 
	 * These include net savings, ROI, and IRR functions.
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
		else
		{
			double irrVal = Utility.round(irr*100,2);
			((SysOptPlaySheet)playSheet).irrLbl.setText(Double.toString(irrVal)+"%");
		}
		if(optNumYears>maxYears)
			((SysOptPlaySheet)playSheet).timeTransitionLbl.setText("Beyond Max Time");
		else
		{
			double timeTransition = Utility.round(optNumYears,2);
			((SysOptPlaySheet)playSheet).timeTransitionLbl.setText(Double.toString(timeTransition)+" Years");
		}
		String annualBudgetString = Utility.sciToDollar(budget);
		((SysOptPlaySheet)playSheet).annualBudgetLbl.setText(annualBudgetString); 
		
		int breakEvenYear = 0;
		for(int i=0;i<breakEvenList.size();i++)
		{
			if(breakEvenList.get(i)<0)
				breakEvenYear = i+1;
		}
		if(breakEvenList.get(breakEvenList.size()-1)<0)
			((SysOptPlaySheet)playSheet).bkevenLbl.setText("Beyond Max Time");
		else if(breakEvenYear == 0)
		{
			((SysOptPlaySheet)playSheet).bkevenLbl.setText("1 Year");
		}
		else
		{
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
			((SysOptPlaySheet)playSheet).solutionLbl.setText("Construction takes longer than allotted "+maxYears+" "+" Years");
		else
			((SysOptPlaySheet)playSheet).solutionLbl.setText("Solution available with "+positiveOrNegativeString+" "+savingsROIOrIRRString);
	}
	
	
}
