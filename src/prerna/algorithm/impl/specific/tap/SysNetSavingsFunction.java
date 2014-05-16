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

import prerna.algorithm.impl.LinearInterpolation;
import prerna.ui.components.specific.tap.SysDecommissionOptimizationFunctions;

/**
 * This class is used to calculate the number of years to decommission systems based on the budget.
 */
public class SysNetSavingsFunction extends UnivariateSvcOptFunction{
	
//	LinearInterpolation linInt;
	SysDecommissionOptimizationFunctions yearAdjuster;
	double numMaintenanceSavings;
	double serMainPerc;
	double dataExposeCost;
	double preTransitionMaintenanceCost;
	double postTransitionMaintenanceCost;
	double inflDiscFactor;
	double k, sigma;
	
	double investment;
	double workNeededAdj;
	ArrayList<Double> workPerformedArray;
	public boolean solutionExists = false;
		
	/**
	 * Given a budget, calculate the years in Savings.
	 * Gets the lists of potential yearly savings and yearly budgets and can write to the app console for each iteration.
	 * @param a 		Budget used in the service optimizer.
	 * @return double	Cumulative Net Savings */
	@Override
	public double value(double a) {
//		count++;
//		updateProgressBar("Iteration " + count);
//		linInt.setB(a);
//		linInt.execute();
//		double n = linInt.retVal;
//		if(n<=-1.0E30)
//		{
//			consoleArea.setText(consoleArea.getText()+"\nPerforming optimization iteration "+count);
//			consoleArea.setText(consoleArea.getText()+"\nThere is no solution for Budget B: "+a);
//			return n;
//		}
//		solutionExists = true;
//		
//		double nAdjusted = yearAdjuster.adjustTimeToTransform(a, n);
////		if(Math.abs(nAdjusted-n)>.1)
//// 			System.out.println("N is: "+n+" Adjusted N is: "+nAdjusted);
//		double savings = calculateRet(a,nAdjusted);
//		writeToAppConsole(a,n,savings);
//		return savings;
		

		count++;
		updateProgressBar("Iteration " + count);
		
		double n=calculateYears(a);
		solutionExists = true;
		
		double nAdjusted = yearAdjuster.adjustTimeToTransform(a, n);
//		if(Math.abs(nAdjusted-n)>.1)
// 			System.out.println("N is: "+n+" Adjusted N is: "+nAdjusted);
		double savings = calculateRet(a,nAdjusted);
		writeToAppConsole(a,n,savings);
		return savings;
	}
	
	public double calculateYears(double budget)
	{
		workPerformedArray = new ArrayList<Double>();
		workNeededAdj = 0.0;
		while(workNeededAdj<dataExposeCost)
		{
			double workPerformedInYearq = calculateWorkPerformedInYearq(budget, workPerformedArray.size()+1);
			workNeededAdj+=workPerformedInYearq;
			workPerformedArray.add(workPerformedInYearq);
		}
		return workPerformedArray.size();
	}
	public double calculateWorkPerformedInYearq(double budget, int q)
	{
		double P1q = calculateP1q(q);
		double workPerformedInYearq = budget * P1q;
		return workPerformedInYearq;
	}
	public double calculateP1q(int q)
	{
		double Pq = calculatePq(q);
		double hireSum = 0.0;
		for(int i=1;i<=q-1;i++)
		{
			hireSum+=Math.pow(1-attRate,i-1)*calculatePq(i);
		}
		double P1q = Pq*Math.pow(1-attRate,q-1)+hireRate*hireSum;
		return P1q;
	}
	public double calculatePq(int q)
	{
		return 1+sigma*Math.exp(-1*q*k);
	}
	public double calculateRet(double budget, double n)
	{
		double P1InflationSum = 0.0;
		for(int q=1;q<=n;q++)
		{
			double P1Inflation = 1.0;
			if(inflDiscFactor!=1)
				P1Inflation = Math.pow(inflDiscFactor, q-1);
			P1Inflation *= calculateP1q(q);
			P1InflationSum += P1Inflation;
		}
		investment = budget * P1InflationSum;
		//if it takes the full time, there is no savings, just return the investment?
		if(totalYrs == n)
			return -1*investment;
		//make the savings inflation/discount factor if applicable
		double savings =totalYrs-n;
		if(inflDiscFactor!=1)
			savings = Math.pow(inflDiscFactor,n+1) * (1-Math.pow(inflDiscFactor,totalYrs-n) ) / (1-inflDiscFactor);
		//multiply the savings for all years
		savings = savings * (numMaintenanceSavings - serMainPerc*investment);
		savings = savings - investment;
		return savings;
	}
	public double calculateRetForVariableTotal(double budget, double n,double totalNumYears)
	{
		double P1InflationSum = 0.0;
		for(int q=1;q<=n;q++)
		{
			double P1Inflation = 1.0;
			if(inflDiscFactor!=1)
				P1Inflation = Math.pow(inflDiscFactor, q-1);
			P1Inflation *= calculateP1q(q);
			P1InflationSum += P1Inflation;
		}
		double investment = budget * P1InflationSum;
		//if it takes the full time, there is no savings, just return the investment?
		if(totalNumYears == n)
			return -1*investment;
		//make the savings inflation/discount factor if applicable
		double savings =totalNumYears-n;
		if(inflDiscFactor!=1)
			savings = Math.pow(inflDiscFactor,n+1) * (1-Math.pow(inflDiscFactor,totalNumYears-n) ) / (1-inflDiscFactor);
		//multiply the savings for all years
		savings = savings * (numMaintenanceSavings - serMainPerc*investment);
		savings = savings - investment;
		return savings;
	}
	public double calculateSavingsForVariableTotal(double budget, double n,double totalNumYears)
	{
		double P1InflationSum = 0.0;
		for(int q=1;q<=n;q++)
		{
			double P1Inflation = 1.0;
			if(inflDiscFactor!=1)
				P1Inflation = Math.pow(inflDiscFactor, q-1);
			P1Inflation *= calculateP1q(q);
			P1InflationSum += P1Inflation;
		}
		double investment = budget * P1InflationSum;
		//if it takes the full time, there is no savings, just return the investment?
		if(totalNumYears == n)
			return -1*investment;
		//make the savings inflation/discount factor if applicable
		double savings =totalNumYears-n;
		if(inflDiscFactor!=1)
			savings = Math.pow(inflDiscFactor,n+1) * (1-Math.pow(inflDiscFactor,totalNumYears-n) ) / (1-inflDiscFactor);
		//multiply the savings for all years
		savings = savings * (numMaintenanceSavings - serMainPerc*investment);
		return savings;
	}
	
	public ArrayList<Double> createSustainmentCosts(double budget, double n)
	{
		ArrayList<Double> sustainmentList = new ArrayList<Double>();
		int index = 0;
		while(index<totalYrs)
		{
			if(index<n)
				sustainmentList.add(0.0);
			else
			{
				double factor=1.0;
				if(inflDiscFactor!=1)
					factor= Math.pow(inflDiscFactor,index+1);
				double sustainment = factor*(numMaintenanceSavings - serMainPerc*investment);
				sustainmentList.add(sustainment);
			}
			index++;
		}
		return sustainmentList;
	}
	public ArrayList<Double> createInstallCosts(double budget, double n)
	{
		ArrayList<Double> installList = new ArrayList<Double>();
		int index = 0;
		while(index<totalYrs)
		{
			if(index<n)//might need to say zero if null pointer
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				double buildCost = factor*budget;
				installList.add(buildCost);
			}
			else
				installList.add(0.0);
			index++;
		}
		return installList;
	}
	
	public ArrayList<Double> createCumulativeSavings(double budget, double n)
	{
		ArrayList<Double> cumSavingsList = new ArrayList<Double>();
		int index = 0;
		while(index<totalYrs)
		{
			if(index<n)
				cumSavingsList.add(0.0);
			else
				cumSavingsList.add(calculateSavingsForVariableTotal(budget,n,index+1));
			index++;
		}
		return cumSavingsList;
	}
	
	public ArrayList<Double> createBreakEven(double budget, double n)
	{
		ArrayList<Double> breakEvenList = new ArrayList<Double>();
		double workPerformedSum=0.0;
		int index = 0;
		while(index<totalYrs)
		{
			if(index+1<n)
			{
				workPerformedSum+= -1*workPerformedArray.get(index);
				breakEvenList.add(workPerformedSum);
			}
			else if(index<n)
				breakEvenList.add(calculateRetForVariableTotal(budget,n,Math.ceil(n)));
			else
				breakEvenList.add(calculateRetForVariableTotal(budget,n,index+1.0));
			index++;
		}
		return breakEvenList;
	}

	public void setSavingsVariables(double numMaintenanceSavings,double serMainPerc,double dataExposeCost, double preTransitionMaintenanceCost, double postTransitionMaintenanceCost,double scdLT,double iniLC,double scdLC)
	{
		this.numMaintenanceSavings = numMaintenanceSavings;
		this.serMainPerc = serMainPerc;
		this.dataExposeCost = dataExposeCost;
		this.preTransitionMaintenanceCost = preTransitionMaintenanceCost;
		this.postTransitionMaintenanceCost = postTransitionMaintenanceCost;
		this.k = (1/scdLT)*Math.log(((1-iniLC)/(1-scdLC)));
		this.sigma = ((1-iniLC)/k)*(1-Math.exp(k));
		this.inflDiscFactor = (1+infRate) / (1+disRate);
	}
	public void writeToAppConsole(double budget, double n, double savings)
	{
		consoleArea.setText(consoleArea.getText()+"\nPerforming optimization iteration "+count);
		consoleArea.setText(consoleArea.getText()+"\nFor Budget B: "+budget+" the minimum N is "+n+" and the savings are "+savings);
	}
	
//	//data Expose is in LOE
//	public void createLinearInterpolation(double initProc, double secondYearProc, double secondYear, double dataExposeCost, double minYears, double maxYears)
//	{
//		 linInt = new LinearInterpolation();
//		 linInt.setValues(initProc, secondYearProc, secondYear, dataExposeCost, minYears, maxYears);
//	}

	public void createYearAdjuster(ArrayList<String> sysList, ArrayList<String> dataList, double hourlyCost)
	{
		yearAdjuster = new SysDecommissionOptimizationFunctions();
		yearAdjuster.setSysList(sysList);
		yearAdjuster.setDataList(dataList);
		yearAdjuster.hourlyCost = hourlyCost;
		yearAdjuster.instantiate();
	}

}
