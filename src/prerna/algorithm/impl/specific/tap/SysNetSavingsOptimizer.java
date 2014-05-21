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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.swing.JProgressBar;

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

import prerna.algorithm.api.IAlgorithm;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.DHMSMHelper;
import prerna.ui.components.specific.tap.OptimizationOrganizer;
import prerna.ui.components.specific.tap.SerOptPlaySheet;
import prerna.ui.components.specific.tap.SysDecommissionOptimizationFunctions;
import prerna.ui.components.specific.tap.SysOptGraphFunctions;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.Utility;

/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class SysNetSavingsOptimizer implements IAlgorithm{
	
	Logger logger = Logger.getLogger(getClass());
	
	SerOptPlaySheet playSheet;
	public int maxYears;
	double interfaceCost;
	public double serMainPerc;
	int noOfPts;
	double minBudget;
	double maxBudget;
	double hourlyCost;
	double[] learningConstants;
	public double iniLC;
	public int scdLT;
	public double scdLC;
	double attRate;
	double hireRate;
	public double infRate;
	double disRate;
	public UnivariateSvcOptFunction f;
	double optBudget =0.0;
	String bindStr = "";
	JProgressBar progressBar;
	OptimizationOrganizer optOrg;
	public String[] optSys;
	
	ResidualSystemOptFillData resFunc;
	public ResidualSystemOptimizer sysOpt;
	String sysQuery, dataQuery, bluQuery;
	public ArrayList<String> sysList, dataList, bluList;
	double dataExposeCost;
	double numMaintenanceSavings;
	double preTransitionMaintenanceCost;
	double postTransitionMaintenanceCost;
	public double budget=0.0, optNumYears = 0.0, netSavings = 0.0, roi=0.0,irr=0.0;
	boolean noErrors=true;
	String errorMessage = "";
	boolean reducedFunctionality = false;
	public ArrayList<Double> cumSavingsList, breakEvenList, sustainCostList, installCostList;

	/**
	 * Method setVariables.
	 * @param maxYears int
	 * @param interfaceCost double
	 * @param serMainPerc double
	 * @param attRate double
	 * @param hireRate double
	 * @param infRate double
	 * @param disRate double
	 * @param noOfPts int
	 * @param minBudget double
	 * @param maxBudget double
	 * @param hourlyCost double
	 * @param iniLC double
	 * @param scdLT int
	 * @param scdLC double
	 */
	public void setVariables(int maxYears, double interfaceCost, double serMainPerc, double attRate, double hireRate, double infRate, double disRate, int noOfPts, double minBudget, double maxBudget, double hourlyCost, double iniLC, int scdLT, double scdLC)
	{
		this.maxYears = maxYears;
		this.interfaceCost = interfaceCost*1000000;
		this.serMainPerc = serMainPerc;
		this.noOfPts = noOfPts;
		this.minBudget = minBudget*1000000;
		this.maxBudget = maxBudget*1000000;
		this.hourlyCost = hourlyCost;
		this.attRate = attRate;
		this.hireRate = hireRate;
		this.iniLC = iniLC;
		this.scdLT = scdLT;
		this.scdLC = scdLC;
		this.infRate = infRate;
		this.disRate = disRate;
	}
	public void setSelectDropDowns(SelectScrollList sysSelectDropDown,SelectScrollList capSelectDropDown,SelectScrollList dataSelectDropDown,SelectScrollList bluSelectDropDown,boolean useSysList,boolean useDataBLU)
	{
		this.sysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}}";
		this.sysQuery = addBindings("System",sysSelectDropDown.list.getSelectedValuesList(),sysQuery);
		if(useDataBLU)
		{
			this.dataQuery = "SELECT DISTINCT ?DataObject WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}";
			this.bluQuery = "SELECT DISTINCT ?BusinessLogicUnit WHERE { {?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}}";
			this.dataQuery = addBindings("DataObject",dataSelectDropDown.list.getSelectedValuesList(),dataQuery);
			this.bluQuery = addBindings("BusinessLogicUnit",bluSelectDropDown.list.getSelectedValuesList(),bluQuery);
		}
		else if(useSysList)
		{
			this.dataQuery = "NULL";
			DHMSMHelper dhelp = new DHMSMHelper();
			dhelp.setUseDHMSMOnly(false);
			dhelp.runData(playSheet.engine);
			ArrayList<String> systems = sysSelectDropDown.getSelectedValues();
			dataList = new ArrayList<String>();
			for(int sysInd = 0;sysInd < systems.size();sysInd++)
			{
				String sys = systems.get(sysInd);
				dataList.addAll(dhelp.getAllDataFromSys(sys, "C"));
			}
			dataList = removeDuplicates(dataList);
			this.bluQuery = "SELECT DISTINCT ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU.}}";
			this.bluQuery = addBindings("System",sysSelectDropDown.list.getSelectedValuesList(),bluQuery);
		}
		else if(!useSysList)
		{
			this.dataQuery = "SELECT DISTINCT ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} }";
			this.bluQuery = "SELECT DISTINCT ?BLU WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BLU}}";
			this.dataQuery = addBindings("Capability",capSelectDropDown.list.getSelectedValuesList(),dataQuery);
			this.bluQuery = addBindings("Capability",capSelectDropDown.list.getSelectedValuesList(),bluQuery);
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
			if(this.sysList.size()==0)
				errorMessage = "No systems were selected. Please select systems under the Select System Functionality tab.";
			else if(sysList.size() == 1)
				errorMessage = "Only one system exists was selected. Please select more than one system under the Select System Functionality tab.";
			noErrors = false;
			return;
		}
		if(!dataQuery.equals("NULL"))
			this.dataList = resFunc.runListQuery(engine,dataQuery);
		this.bluList = resFunc.runListQuery(engine,bluQuery);
		if(this.dataList.size()==0 && this.bluList.size()==0 )
		{
			errorMessage = "No data objects or business logic unites were selected. Please select at least one under the Select System Functionality tab.";
			noErrors = false;
			return;
		}
		resFunc.setPlaySheet((SysOptPlaySheet)playSheet);
		resFunc.setSysDataBLULists(deepCopy(sysList),deepCopy(dataList),deepCopy(bluList));
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
		sysOpt.setPlaySheet((SysOptPlaySheet)playSheet);
//		sysOpt.setDataBLUPercent(dataPercent,bluPercent);
		sysOpt.setDataSet(this.sysList,this.dataList,this.bluList,resFunc.systemDataMatrix,resFunc.systemBLUMatrix,resFunc.systemCostOfDataMatrix,resFunc.systemCostOfMaintenance,resFunc.systemCostOfDB,resFunc.systemNumOfSites,resFunc.dataSORSystemExists,resFunc.bluProviderExists);
		noErrors = sysOpt.runOpt();
		errorMessage = sysOpt.errorMessage;

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
        	playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+"Potential annual sustainment savings is less than annual maintenance of exposed data objects. Rationalization solution is not available.");
			playSheet.progressBar.setVisible(false);
			Utility.showError("Potential annual sustainment savings is less than annual maintenance of exposed data objects. Rationalization solution is not available.");
			return;
		}
		
        progressBar = playSheet.progressBar;
        f.setConsoleArea(playSheet.consoleArea);
        f.setProgressBar(progressBar);
        f.setVariables(maxYears, hourlyCost, interfaceCost, serMainPerc, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        ((SysNetSavingsFunction)f).setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost,scdLT, iniLC, scdLC);
        if(f instanceof SysIRRFunction)
        	((SysIRRFunction)f).createLinearInterpolation();
        ((SysNetSavingsFunction)f).createYearAdjuster(sysList, dataList, hourlyCost);

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
            if(((SysNetSavingsFunction)f).solutionExists)
            {
	            budget = pair.getPoint();
	            optNumYears = ((SysNetSavingsFunction)f).calculateYears(budget);
	            optNumYears =  ((SysNetSavingsFunction)f).yearAdjuster.adjustTimeToTransform(budget, optNumYears);
	            if(optNumYears<1)
	            {
	            	optNumYears = 1;
	            	budget = ((SysNetSavingsFunction)f).calculateBudgetForOneYear();
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
		SysDecommissionOptimizationFunctions optFunctions = new SysDecommissionOptimizationFunctions();
		optFunctions.setSysList(sysList);
		optFunctions.setDataList(dataList);
		optFunctions.hourlyCost = hourlyCost;
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
        SysIRRFunction irrF = new SysIRRFunction();
        irrF.setVariables(maxYears, hourlyCost, interfaceCost, serMainPerc, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        irrF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost, scdLT, iniLC, scdLC);
        irrF.createLinearInterpolation();
        irr = irrF.calculateRet(budget,optNumYears);
        if(f instanceof SysIRRFunction)
        {
        	disRate = irr;
        	if(irr==-1.0E30)
        		noErrors = false;
        }
        SysNetSavingsFunction savingsF = new SysNetSavingsFunction();
        savingsF.setVariables(maxYears, hourlyCost, interfaceCost, serMainPerc, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        savingsF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost, scdLT, iniLC, scdLC);
        netSavings = savingsF.calculateRet(budget,optNumYears);

        SysROIFunction roiF = new SysROIFunction();
        roiF.setVariables(maxYears, hourlyCost, interfaceCost, serMainPerc, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);
        roiF.setSavingsVariables(numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost, scdLT, iniLC, scdLC);
        roi = roiF.calculateRet(budget,optNumYears);
        
	}
	
	/**
	 * Runs the appropriate optimization iteration.
	 */
	public void optimize()
	{
        f = new SysNetSavingsFunction();
        runOpt();
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
		ArrayList <Object []> list = new ArrayList();
		String[] colNames = new String[4];
		colNames[0]="System";
		colNames[1]="Action";
		colNames[2]="Number Of Data Provided";
		colNames[3]="Number of BLU Provided";
		for (int i = 0;i<sysList.size();i++)
		{
			Object[] newRow = new Object[4];
			newRow[0] = resFunc.sysList.get(i);
			if(sysOpt.systemIsModernized[i]>0)
				newRow[1] = "Modernize";
			else
				newRow[1] = "Decommission";
			newRow[2] = sumRow(resFunc.systemDataMatrix[i]);
			newRow[3] = sumRow(resFunc.systemBLUMatrix[i]);
			list.add(newRow);
		}
		GridScrollPane pane = new GridScrollPane(colNames, list);
		playSheet.specificSysAlysPanel.removeAll();
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		playSheet.specificSysAlysPanel.setLayout(gridBagLayout);
		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		playSheet.specificSysAlysPanel.add(pane, gbc_panel_1_1);
		playSheet.specificSysAlysPanel.repaint();
	}
	public void displayFunctionalitySpecifics()
	{
		ArrayList <Object []> list = new ArrayList();
		String[] colNames = new String[resFunc.sysList.size()+3];//number of systems+2
		colNames[0]="Data/BLU";
		colNames[1]="Type";
		colNames[2]="Number of Systems Providing";
		for(int i=0;i<resFunc.sysList.size();i++)
			colNames[i+3] = resFunc.sysList.get(i);
		for (int dataInd = 0;dataInd<resFunc.dataList.size();dataInd++)
		{
			Object[] newRow = new Object[resFunc.sysList.size()+3];
			newRow[0] = resFunc.dataList.get(dataInd);
			newRow[1] = "Data";
			newRow[2] = resFunc.dataSORSystemCount[dataInd];
			for(int sysInd=0;sysInd<resFunc.sysList.size();sysInd++)
				if(resFunc.systemDataMatrix[sysInd][dataInd]==1)
					newRow[sysInd+3] = "X";
			list.add(newRow);
		}
		for (int bluInd = 0;bluInd<resFunc.bluList.size();bluInd++)
		{
			Object[] newRow = new Object[resFunc.sysList.size()+3];
			newRow[0] = resFunc.bluList.get(bluInd);
			newRow[1] = "BLU";
			newRow[2] = resFunc.bluProviderCount[bluInd];
			for(int sysInd=0;sysInd<resFunc.sysList.size();sysInd++)
				if(resFunc.systemBLUMatrix[sysInd][bluInd]==1)
					newRow[sysInd+3] = "X";
			list.add(newRow);
		}

		GridScrollPane pane = new GridScrollPane(colNames, list);
		pane.addHorizontalScroll();
		((SysOptPlaySheet)playSheet).specificFuncAlysPanel.removeAll();
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		((SysOptPlaySheet)playSheet).specificFuncAlysPanel.setLayout(gridBagLayout);
		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		((SysOptPlaySheet)playSheet).specificFuncAlysPanel.add(pane, gbc_panel_1_1);
		((SysOptPlaySheet)playSheet).specificFuncAlysPanel.repaint();
	}

	/**
	 * Displays the results from various optimization calculations. 
	 * These include profit, ROI, Recoup, and breakeven functions.
	 * @param lin 	Optimizer used for TAP-specific calculations. 
	 */
	public void displayResults()
	{
		if(reducedFunctionality)
			((SysOptPlaySheet)playSheet).solutionLbl.setText("Solution available with reduced functionality (see functionality analysis tab for details)");
		else{
			double savingsROIOrIRR = netSavings;
			String savingsROIOrIRRString = "savings";
			String positiveOrNegativeString = "positive";
			if(f instanceof SysROIFunction)
			{
				savingsROIOrIRR = roi;
				savingsROIOrIRRString = "ROI";
			}
			else if(f instanceof SysIRRFunction)
			{
				savingsROIOrIRR = irr;
				savingsROIOrIRRString = "IRR";				
			}
			if(savingsROIOrIRR < 0.0)
				positiveOrNegativeString = "negative";
			((SysOptPlaySheet)playSheet).solutionLbl.setText("Solution available with "+positiveOrNegativeString+" "+savingsROIOrIRRString);
		}
		
		f.createLearningYearlyConstants((int)Math.ceil(optNumYears), scdLT, iniLC, scdLC);
		cumSavingsList = ((SysNetSavingsFunction)f).createCumulativeSavings(budget, optNumYears);
		breakEvenList = ((SysNetSavingsFunction)f).createBreakEven(budget, optNumYears);
		sustainCostList = ((SysNetSavingsFunction)f).createSustainmentCosts(budget, optNumYears);
		installCostList = ((SysNetSavingsFunction)f).createInstallCosts(budget, optNumYears);
		
		String netSavingsString = Utility.sciToDollar(netSavings);
		playSheet.savingLbl.setText(netSavingsString);
		double roiVal = Utility.round(roi*100, 2);
		playSheet.roiLbl.setText(Double.toString(roiVal)+"%");
		if(irr<0||(netSavings<0&&numMaintenanceSavings - serMainPerc*dataExposeCost<0))
			((SysOptPlaySheet)playSheet).irrLbl.setText("N/A");
		else
		{
			double irrVal = Utility.round(irr*100,2);
			((SysOptPlaySheet)playSheet).irrLbl.setText(Double.toString(irrVal)+"%");
		}
		double timeTransition = Utility.round(optNumYears,2);
		((SysOptPlaySheet)playSheet).timeTransitionLbl.setText(Double.toString(timeTransition)+" Years");
		String annualBudgetString = Utility.sciToDollar(budget);
		((SysOptPlaySheet)playSheet).annualBudgetLbl.setText(annualBudgetString); 
		
		int breakEvenYear = 0;
		for(int i=0;i<breakEvenList.size();i++)
		{
			if(breakEvenList.get(i)<0)
				breakEvenYear = i+1;
		}
		if(breakEvenList.get(breakEvenList.size()-1)<0)
			playSheet.bkevenLbl.setText("Beyond Max Time");
		else if(breakEvenYear == 0)
		{
			playSheet.bkevenLbl.setText("1 Year");
		}
		else
		{
			double amountInLastYear = breakEvenList.get(breakEvenYear)-breakEvenList.get(breakEvenYear-1);
			double fraction = ( - breakEvenList.get(breakEvenYear))/amountInLastYear;
			double breakEven = Utility.round(breakEvenYear+1+fraction,2);
			playSheet.bkevenLbl.setText(Double.toString(breakEven)+" Years");
		}
		
		SysOptGraphFunctions graphF= new SysOptGraphFunctions();
		graphF.setOptimzer(this);

		Hashtable modernizedSysHeatMapChartHash = graphF.createModernizedHeatMap();
		Hashtable chartHash3 = graphF.createCostChart();
		Hashtable chartHash4 = graphF.createCumulativeSavings();
		Hashtable chartHash5 = graphF.createBreakevenGraph();
		Hashtable chartHash6 = graphF.createLearningCurve();


	//	((SysOptPlaySheet)playSheet).tabModernizedHeatMap.browser.refresh();
	//	((SysOptPlaySheet)playSheet).tabModernizedHeatMap.callIt(modernizedSysHeatMapChartHash);
		playSheet.tab3.callIt(chartHash3);
		playSheet.tab4.callIt(chartHash4);
		playSheet.tab5.callIt(chartHash5);
		playSheet.tab6.callIt(chartHash6);
	//	((SysOptPlaySheet)playSheet).tabModernizedHeatMap.setVisible(true);
		playSheet.tab3.setVisible(true);
		playSheet.tab4.setVisible(true);
		playSheet.tab5.setVisible(true);
		playSheet.tab6.setVisible(true);
	}
	/**
	 * Clears the playsheet by removing information from all panels.
	 */
	public void clearPlaysheet(){
		clearGraphs();
		playSheet.specificAlysPanel.removeAll();
		playSheet.specificSysAlysPanel.removeAll();
		playSheet.playSheetPanel.removeAll();
	}
	/**
	 * Clears graphs within the playsheets.
	 */
	public void clearGraphs()
	{
		playSheet.tab1.setVisible(false);
		playSheet.tab2.setVisible(false);
		playSheet.tab3.setVisible(false);
		playSheet.tab4.setVisible(false);
		playSheet.tab5.setVisible(false);
		playSheet.tab6.setVisible(false);
		playSheet.timeline.setVisible(false);
	}
	
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (SerOptPlaySheet) playSheet;
		
	}
	
	/**
	 * Gets variable names.
	 * */
	@Override
	public String[] getVariables() {
		return null;
	}

	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute(){
		optimize();
		
	}

	/**
	 * Gets the name of the algorithm.
	 * @return 	Algorithm name. */
	@Override
	public String getAlgoName() {
		return null;
	}
}
