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

import java.util.Hashtable;

import org.apache.commons.math3.exception.TooManyEvaluationsException;
import org.apache.commons.math3.optim.InitialGuess;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.OptimizationData;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.SimpleBounds;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.nonlinear.scalar.MultiStartMultivariateOptimizer;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.math.RandVectorGenerator;
import prerna.ui.components.specific.tap.OptChartUtilityMethods;
import prerna.ui.components.specific.tap.SysOptGraphFunctions;
import prerna.ui.components.specific.tap.SysOptPlaySheet;

/**
 * This optimizer is used for implementation of system optimization for a multivariate budget.
 */
public class MultivariateOptimizer extends UnivariateSysOptimizer{
	
	private int maxEvals;
	public MultivariateOptFunction f;
	double[] yearlyBudget;
	static final Logger LOGGER = LogManager.getLogger(MultivariateOptimizer.class.getName());

	public void setVariables(int maxYears, double interfaceCost, double serMainPerc, double attRate, double hireRate, double infRate, double disRate, int noOfPts, int maxEvals, double minBudget, double maxBudget, double hourlyCost, double iniLC, int scdLT, double scdLC)
	{
		super.setVariables(maxYears, interfaceCost, serMainPerc, attRate, hireRate, infRate, disRate, noOfPts, minBudget, maxBudget, hourlyCost, iniLC, scdLT, scdLC);
		this.maxEvals = maxEvals;
	}
	
	protected void optimizeBudget() {
		
        progressBar = playSheet.progressBar;
        f.setConsoleArea(playSheet.consoleArea);
        f.setProgressBar(progressBar);
        f.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC,numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        if(f instanceof MultiVarSysIRRFunction)
        	((MultiVarSysIRRFunction)f).createLinearInterpolation();
        //((MultivariateOptFunction)f).createYearAdjuster(sysList, dataList, hourlyCost);TODO add in year adjuster

        //budget in LOE
        double[] startPoint = new double[maxYears];
        double[] stdDev = new double[maxYears];
		double[][] boundaries = new double[2][maxYears];
        for(int i=0;i<maxYears;i++) {
//        	if(i<maxYears/2)
        		startPoint[i] = this.maxBudget / 2;
//        	else
//            	startPoint[i] = 0;
       		stdDev[i] = this.maxBudget * 0.01;
			boundaries[0][i] = this.minBudget;
			boundaries[1][i] = this.maxBudget;
        }
        MaxEval eval = new MaxEval(maxEvals);
     //   int numInterpolationPoints = startPoint.length+2;//2 * startPoint.length + 1;
        int numInterpolationPoints = ((startPoint.length+1)*(startPoint.length+2))/startPoint.length;
        BOBYQAOptimizer optimizer = new BOBYQAOptimizer(numInterpolationPoints,maxBudget*.01,maxBudget*.00001);
		ObjectiveFunction objF = new ObjectiveFunction(f);
      //TODO multistart, UnitSphereRandomVectorGenerator is not very good.
		RandVectorGenerator rand = new RandVectorGenerator();
		rand.setMax(this.maxBudget);
		rand.setSize(startPoint.length);
		//RandomVectorGenerator rand = new UncorrelatedRandomVectorGenerator(startPoint.length, new GaussianRandomGenerator(new Well1024a(50000)));
		MultiStartMultivariateOptimizer multiOpt = new MultiStartMultivariateOptimizer(optimizer, noOfPts, rand);

        OptimizationData[] data = new OptimizationData[]{new SimpleBounds(boundaries[0], boundaries[1]), objF, GoalType.MAXIMIZE, eval, new InitialGuess(rand.nextVector()) };
      try {
    	  multiOpt.optimize(data);
    	  PointValuePair[] pairs = multiOpt.getOptima();
    	  
    	  playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nFor starting points: ");
    	  for(int i=0;i<pairs.length;i++) {
    		  playSheet.consoleArea.setText(playSheet.consoleArea.getText()+rand.getStartingPoints().get(i)+", ");
    	  }
    	  playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nThe yearly budgets producing local maxima are: ");
     	  for(int i=0;i<pairs.length;i++) {
    		  playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n");
    		  yearlyBudget = pairs[i].getPoint();
    			for(int j=0;j<yearlyBudget.length;j++)
    				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+yearlyBudget[j]+", ");
    	  }
    	  
            progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
            if(((MultivariateOptFunction)f).solutionExists)
            {
            	yearlyBudget = pairs[0].getPoint();
	            optNumYears = ((MultivariateOptFunction)f).calculateYears(yearlyBudget);
	          //  optNumYears =  ((MultivariateOptFunction)f).yearAdjuster.adjustTimeToTransform(yearlyBudget, optNumYears);
	            if(optNumYears<1) {
	            	optNumYears = 1;
	            }
	            calculateSavingsROIAndIRR();
            } else  {
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
	}

	public void calculateSavingsROIAndIRR()
	{

        MultiVarSysNetSavingsFunction savingsF = new MultiVarSysNetSavingsFunction();
        savingsF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC,numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        savingsF.calculateYears(yearlyBudget);
        netSavings = savingsF.calculateRet(yearlyBudget,optNumYears);

        MultiVarSysROIFunction roiF = new MultiVarSysROIFunction();
        roiF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC,numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        roiF.calculateYears(yearlyBudget);
        roi = roiF.calculateRet(yearlyBudget,optNumYears);
        
        MultiVarSysIRRFunction irrF = new MultiVarSysIRRFunction();
        irrF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC,numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        irrF.createLinearInterpolation();
        irrF.calculateYears(yearlyBudget);
        irr = irrF.calculateRet(yearlyBudget,optNumYears);        
 	 
        if(f instanceof MultiVarSysIRRFunction)
        {
        	disRate = irr;
        	if(irr==-1.0E30)
        		noErrors = false;
        }
	}

	protected void display() {
  	  	playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nOf these, the Global Maxima occurs when yearly budget is: ");
		for(int i=0;i<yearlyBudget.length;i++)
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+yearlyBudget[i]+", ");
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nNumber of Years to consolidate systems: "+optNumYears);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nGiven timespan to accumulate savings over: "+maxYears);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nCumulative savings: "+netSavings);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nROI: "+roi);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nIRR: "+irr);
        displayOverview();
        displaySystemSpecifics();
        displayCurrFunctionality();
        displayFutureFunctionality();
        displayHeatMap();
  //      displayClusterHeatMap();
	}
	
	@Override
	protected void createGraphData() {
		f.createLearningYearlyConstants((int)Math.ceil(optNumYears), scdLT, iniLC, scdLC);
		cumSavingsList = ((MultivariateOptFunction)f).createCumulativeSavings(optNumYears);
		breakEvenList = ((MultivariateOptFunction)f).createBreakEven(yearlyBudget, optNumYears);
		sustainCostList = ((MultivariateOptFunction)f).createSustainmentCosts(optNumYears);
		installCostList = ((MultivariateOptFunction)f).createInstallCosts(yearlyBudget, optNumYears);
		workDoneList = ((MultivariateOptFunction)f).createWorkDoneCosts(yearlyBudget, optNumYears);
		
	}
	
	protected void displayHeaderLabels() {
		super.displayHeaderLabels();
		((SysOptPlaySheet)playSheet).annualBudgetLbl.setText("Varies");
	}
	
	/**
	 * Displays the results from various optimization calculations.
	 * These include net savings, ROI, and IRR functions.
	 * This populates the overview tab
	 * Optimizer used for TAP-specific calculations. 
	 */
	private void displayOverview()
	{
		displaySolutionLabel();

		createGraphData();
		
		displayHeaderLabels();

		SysOptGraphFunctions graphF= new SysOptGraphFunctions();
		graphF.setOptimizer(this);

		Hashtable chartHash3 = graphF.createCostChart();
		Hashtable chartHash4 = graphF.createCumulativeSavings();
		Hashtable chartHash5 = graphF.createBreakevenGraph();
		Hashtable chartHash6 = OptChartUtilityMethods.createLearningCurve(2014, iniLC, scdLT, scdLC, f.learningConstants);

		((SysOptPlaySheet)playSheet).tab3.callIt(chartHash3);
		((SysOptPlaySheet)playSheet).tab4.callIt(chartHash4);
		((SysOptPlaySheet)playSheet).tab5.callIt(chartHash5);
		((SysOptPlaySheet)playSheet).tab6.callIt(chartHash6);
		playSheet.setGraphsVisible(true);
	}
}
