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

import java.util.Hashtable;

import org.apache.commons.math3.exception.TooManyEvaluationsException;
import org.apache.commons.math3.optim.InitialGuess;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.SimpleBounds;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.specific.tap.SysOptGraphFunctions;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.util.Utility;

/**
 * This optimizer is used for implementation of system optimization for a multivariate budget.
 */
public class MultivariateOptimizer extends UnivariateSysOptimizer{
	
	public MultivariateOptFunction f;
	double[] yearlyBudget;
	static final Logger LOGGER = LogManager.getLogger(MultivariateOptimizer.class.getName());

	protected void optimizeBudget() {
		
        progressBar = playSheet.progressBar;
        f.setConsoleArea(playSheet.consoleArea);
        f.setProgressBar(progressBar);
        f.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC,numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        if(f instanceof MultiVarSysIRRFunction)
        	((MultiVarSysIRRFunction)f).createLinearInterpolation();
     //   ((MultivariateOptFunction)f).createYearAdjuster(sysList, dataList, hourlyCost);//TODO add back in

        //budget in LOE
        double[] startPoint = new double[maxYears];
		double[][] boundaries = new double[2][maxYears];
        for(int i=0;i<maxYears;i++) {
  //      	if(i < maxYears / 2)
        		startPoint[i] = this.maxBudget / 2;
//        	else
//        		startPoint[i] = 0;
			boundaries[0][i] = this.minBudget;
			boundaries[1][i] = this.maxBudget;
        }
        MaxEval eval = new MaxEval(500);
        int numInterpolationPoints = startPoint.length+2;//2 * startPoint.length + 1;
        BOBYQAOptimizer optimizer = new BOBYQAOptimizer(numInterpolationPoints,100,.001);
		ObjectiveFunction objF = new ObjectiveFunction(f);
      //TODO multistart
//		RandomVectorGenerator rand = new Well1024a(500);
//		MultiStartMultivariateOptimizer multiOpt = new MultiStartMultivariateOptimizer(optimizer, noOfPts, rand);
//      SearchInterval search = new SearchInterval(minBudget,maxBudget); //budget in LOE
//      optimizer.getStartValue();

      try {
            PointValuePair pair = optimizer.optimize(eval, objF, GoalType.MAXIMIZE, new InitialGuess(startPoint),  new SimpleBounds(boundaries[0], boundaries[1]));

            progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
            if(((MultivariateOptFunction)f).solutionExists)
            {
	            yearlyBudget = pair.getPoint();
	            optNumYears = ((MultivariateOptFunction)f).calculateYears(yearlyBudget);
	            //optNumYears =  ((MultivariateOptFunction)f).yearAdjuster.adjustTimeToTransform(budget, optNumYears);TODO add in year adjuster
	            if(optNumYears<1) {
	            	optNumYears = 1;
	            	//budget = ((MultivariateSysOptFunction)f).calculateBudgetForOneYear();//TODO add back in?
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
	}

	public void calculateSavingsROIAndIRR()
	{

        MultiVarSysNetSavingsFunction savingsF = new MultiVarSysNetSavingsFunction();
        savingsF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC,numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        netSavings = savingsF.calculateRet(yearlyBudget,optNumYears);

        MultiVarSysROIFunction roiF = new MultiVarSysROIFunction();
        roiF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC,numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        roi = roiF.calculateRet(yearlyBudget,optNumYears);
        
        MultiVarSysIRRFunction irrF = new MultiVarSysIRRFunction();
        irrF.setVariables(maxYears, attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC,numMaintenanceSavings, serMainPerc, dataExposeCost,preTransitionMaintenanceCost,postTransitionMaintenanceCost);
        irrF.createLinearInterpolation();
        irr = irrF.calculateRet(yearlyBudget,optNumYears,netSavings);        
 	 
        if(f instanceof MultiVarSysIRRFunction)
        {
        	disRate = irr;
        	if(irr==-1.0E30)
        		noErrors = false;
        }
	}

	protected void display() {
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nFor Budget B: ");
		for(int i=0;i<yearlyBudget.length;i++)
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+yearlyBudget[i]+", ");
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
		cumSavingsList = ((MultivariateOptFunction)f).createCumulativeSavings(optNumYears);
		breakEvenList = ((MultivariateOptFunction)f).createBreakEven(yearlyBudget, optNumYears);
		sustainCostList = ((MultivariateOptFunction)f).createSustainmentCosts(optNumYears);
		installCostList = ((MultivariateOptFunction)f).createInstallCosts(yearlyBudget, optNumYears);
		
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
//		String annualBudgetString = Utility.sciToDollar(budget);
//		((SysOptPlaySheet)playSheet).annualBudgetLbl.setText(annualBudgetString); //TODO what to put as budget
		
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
		graphF.setOptimzer(this,f.learningConstants);

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
}
