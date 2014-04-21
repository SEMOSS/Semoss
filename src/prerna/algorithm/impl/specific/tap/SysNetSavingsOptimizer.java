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
import org.apache.log4j.Logger;

import prerna.ui.components.specific.tap.SysDecommissionOptimizationFunctions;
import prerna.ui.components.specific.tap.SysOptGraphFunctions;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.Utility;

/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class SysNetSavingsOptimizer extends UnivariateSvcOptimizer{
	
	ResidualSystemOptFillData resFunc;
	String sysQuery, dataQuery, bluQuery;
	ArrayList<String> sysList, dataList, bluList;
	double dataExposeCost;
	double numMaintenanceSavings;
	double preTransitionMaintenanceCost;
	double postTransitionMaintenanceCost;
	double dataPercent, bluPercent;
	public double budget=0.0, optNumYears = 0.0, netSavings = 0.0, roi=0.0;
	Logger logger = Logger.getLogger(getClass());
	boolean noErrors=true;
	public ArrayList<Double> cumSavingsList, breakEvenList, sustainCostList, installCostList;
	
	public void setDataBLUPercent(double dataPercent,double bluPercent)
	{
		this.dataPercent = dataPercent;
		this.bluPercent = bluPercent;
	}
	public void setSelectDropDowns(SelectScrollList sysSelectDropDown,SelectScrollList capSelectDropDown,SelectScrollList dataSelectDropDown,SelectScrollList bluSelectDropDown,boolean useDataBLU)
	{
		this.sysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}}";
		this.sysQuery = addBindings("System",sysSelectDropDown.list.getSelectedValuesList(),sysQuery);
		if(!useDataBLU)
		{
			this.dataQuery = "SELECT DISTINCT ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} }";
			this.bluQuery = "SELECT DISTINCT ?BLU WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BLU}}";
			this.dataQuery = addBindings("Capability",capSelectDropDown.list.getSelectedValuesList(),dataQuery);
			this.bluQuery = addBindings("Capability",capSelectDropDown.list.getSelectedValuesList(),bluQuery);
		}
		else
		{
			this.dataQuery = "SELECT DISTINCT ?DataObject WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}";
			this.bluQuery = "SELECT DISTINCT ?BusinessLogicUnit WHERE { {?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}}";
			this.dataQuery = addBindings("DataObject",dataSelectDropDown.list.getSelectedValuesList(),dataQuery);
			this.bluQuery = addBindings("BusinessLogicUnit",bluSelectDropDown.list.getSelectedValuesList(),bluQuery);
		}
	}
	
	public void setQueries(String sysQuery, String dataQuery, String bluQuery)
	{
		this.sysQuery = sysQuery;
		this.dataQuery = dataQuery;
		this.bluQuery = bluQuery;
	}
	public String addBindings(String type, List bindingsList,String query)
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
			noErrors = false;
			return;
		}
		this.dataList = resFunc.runListQuery(engine,dataQuery);
		this.bluList = resFunc.runListQuery(engine,bluQuery);
		resFunc.setPlaySheet((SysOptPlaySheet)playSheet);
		resFunc.setSysList(deepCopy(sysList));
		resFunc.setDataList(deepCopy(dataList));
		resFunc.setBLUList(deepCopy(bluList));
		resFunc.fillDataStores();
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
		ResidualSystemOptimizer sysOpt = new ResidualSystemOptimizer();
		sysOpt.setPlaySheet((SysOptPlaySheet)playSheet);
		sysOpt.setDataBLUPercent(dataPercent,bluPercent);
		sysOpt.setDataSet(this.sysList,this.dataList,this.bluList,resFunc.systemDataMatrix,resFunc.systemBLUMatrix,resFunc.systemCostOfDataMatrix,resFunc.systemCostOfMaintenance,resFunc.systemCostOfDB,resFunc.systemNumOfSites,resFunc.dataSORSystemExists,resFunc.bluProviderExists);
		noErrors = sysOpt.runOpt();

		this.dataExposeCost = sysOpt.numTransformationTotal; //total cost to expose all data for all systems at all sites
		this.numMaintenanceSavings =sysOpt.numMaintenanceTotal;
		this.preTransitionMaintenanceCost = sysOpt.denomCurrentMaintenance;
		this.postTransitionMaintenanceCost = preTransitionMaintenanceCost - numMaintenanceSavings;
		}
	
	public void runOpt()
	{
		getData();
		if(noErrors == false)
		{
			playSheet.progressBar.setVisible(false);
			if(sysList.size()==0)
				Utility.showError("No systems were selected. Please select systems under the Select System Functionality tab.");
			else if(sysList.size() == 1)
				Utility.showError("Only one system exists was selected. Please select more than one system under the Select System Functionality tab.");
			else
				Utility.showError("Error with system functionality selections.");
			return;
		}
		getModernizedSysList();
		if(noErrors == false)
		{
			playSheet.progressBar.setVisible(false);
			Utility.showError("All systems must be kept to maintain same functionality.");
			return;
		}
		if(numMaintenanceSavings < serMainPerc*dataExposeCost)
		{
        	playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+"Potential annual sustainment savings is less than annual maintenance of exposed data objects. Rationalization is not recommended.");
			playSheet.progressBar.setVisible(false);
			Utility.showError("Potential annual sustainment savings is less than annual maintenance of exposed data objects. Rationalization is not recommended.");
			return;
		}
		
        progressBar = playSheet.progressBar;
        f.setConsoleArea(playSheet.consoleArea);
        f.setProgressBar(progressBar);
        f.setVariables(maxYears, hourlyCost, interfaceCost, serMainPerc, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        ((SysNetSavingsFunction)f).setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        ((SysNetSavingsFunction)f).createLinearInterpolation(iniLC,scdLC, scdLT, dataExposeCost, 0, maxYears);

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
            budget = pair.getPoint();
            optNumYears = ((SysNetSavingsFunction)f).calculateYear(budget);
            calculateSavingsAndROI();
            progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
        } catch (TooManyEvaluationsException fee) {
        	noErrors = false;
        	playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+fee);
            progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
            clearPlaysheet();
        }
        
        //getting the deployment strategy
		SysDecommissionOptimizationFunctions optFunctions = new SysDecommissionOptimizationFunctions();
		optFunctions.setSysList(sysList);
		optFunctions.setDataList(dataList);
		optFunctions.costPerHour = hourlyCost;
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
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nMaximized net cumulative savings: "+netSavings);
	        displayResults(f.lin);
        }


		
	}
	public void calculateSavingsAndROI()
	{
        SysNetSavingsFunction savingsF = new SysNetSavingsFunction();
        savingsF.setVariables(maxYears, hourlyCost, interfaceCost, serMainPerc, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        savingsF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        savingsF.createLinearInterpolation(iniLC,scdLC, scdLT, dataExposeCost, 0, maxYears);
        netSavings = savingsF.calculateRet(budget,optNumYears);
        
        SysROIFunction roiF = new SysROIFunction();
        roiF.setVariables(maxYears, hourlyCost, interfaceCost, serMainPerc, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        roiF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        roiF.createLinearInterpolation(iniLC,scdLC, scdLT, dataExposeCost, 0, maxYears);
        roi = roiF.calculateRet(budget,optNumYears);
	}
	
	/**
	 * Runs the appropriate optimization iteration.
	 */
	@Override
	public void optimize()
	{
        f = new SysNetSavingsFunction();
        runOpt();
        //should this go in runOpt try catch?
//        if(noErrors)
//        {
//			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nBudget: "+budget);
//			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nNumber of Years to consolidate systems: "+optNumYears);
//			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nGiven timespan to accumulate savings over: "+maxYears);
//			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nMaximized net cumulative savings: "+netSavings);
//        }
	}   
	

//	/**
//	 * Runs a specific iteration of the optimization.
//	 */
//	@Override
//	public void runOptIteration()
//	{
//        displayResults(f.lin);
//	}
	

	/**
	 * Displays the results from various optimization calculations. 
	 * These include profit, ROI, Recoup, and breakeven functions.
	 * @param lin 	Optimizer used for TAP-specific calculations. 
	 */
	@Override
	public void displayResults(ServiceOptimizer lin)
	{
		f.createLearningYearlyConstants((int)Math.ceil(optNumYears), scdLT, iniLC, scdLC);
		cumSavingsList = ((SysNetSavingsFunction)f).createCumulativeSavings(budget, optNumYears);
		breakEvenList = ((SysNetSavingsFunction)f).createBreakEven(budget, optNumYears);
		sustainCostList = ((SysNetSavingsFunction)f).createSustainmentCosts(budget, optNumYears);
		installCostList = ((SysNetSavingsFunction)f).createInstallCosts(budget, optNumYears);
		
		String netSavingsString = Utility.sciToDollar(netSavings);
		playSheet.savingLbl.setText(netSavingsString);
		String annualBudgetString = Utility.sciToDollar(budget);
		((SysOptPlaySheet)playSheet).annualBudgetLbl.setText(annualBudgetString); 
		double timeTransition = Utility.round(optNumYears,2);
		((SysOptPlaySheet)playSheet).timeTransitionLbl.setText(Double.toString(timeTransition)+" Years");
		double roiVal = Utility.round(roi*100, 2);
		playSheet.roiLbl.setText(Double.toString(roiVal)+"%"); 
		
		double breakEvenYear = 0.0;
		for(int i=0;i<breakEvenList.size();i++)
		{
			if(breakEvenList.get(i)<0)
				breakEvenYear = i+1;
		}
		if(breakEvenList.get(breakEvenList.size()-1)<0)
			playSheet.bkevenLbl.setText("Beyond Max Time");
		else
			playSheet.bkevenLbl.setText(Double.toString(breakEvenYear)+" Years");
		
		SysOptGraphFunctions graphF= new SysOptGraphFunctions();
		graphF.setOptimzer(this);

		Hashtable chartHash3 = graphF.createCostChart();
		Hashtable chartHash4 = graphF.createCumulativeSavings();
		Hashtable chartHash5 = graphF.createBreakevenGraph();
		Hashtable chartHash6 = graphF.createLearningCurve();

		playSheet.tab3.callIt(chartHash3);
		playSheet.tab4.callIt(chartHash4);
		playSheet.tab5.callIt(chartHash5);
		playSheet.tab6.callIt(chartHash6);
		playSheet.tab3.setVisible(true);
		playSheet.tab4.setVisible(true);
		playSheet.tab5.setVisible(true);
		playSheet.tab6.setVisible(true);
	}
	
	
}
