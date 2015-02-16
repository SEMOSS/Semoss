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

import prerna.ui.components.specific.tap.SysDecommissionOptimizationFunctions;

/**
 * This class is used to calculate the number of years to decommission systems based on the budget.
 */
public class UnivariateSysOptFunction extends UnivariateOptFunction{
	
	SysDecommissionOptimizationFunctions yearAdjuster;
	double numMaintenanceSavings, serMainPerc, dataExposeCost;
	double preTransitionMaintenanceCost, postTransitionMaintenanceCost;
	double k, sigma, inflDiscFactor;
	
	double investment;
	ArrayList<Double> workPerformedArray;
	public boolean solutionExists = false;


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
	
	/**
	 * Given a budget, calculate the years in Savings.
	 * Gets the lists of potential yearly savings and yearly budgets and can write to the app console for each iteration.
	 * @param a 		Budget used in the service optimizer.
	 * @return double	Cumulative Net Savings */
	@Override
	public double value(double a) {
		count++;
		updateProgressBar("Iteration " + count);
		
		double n=calculateYears(a);
		solutionExists = true;
		if(n==-1 )
			return -1.0E30;
		if(n>totalYrs)
		{
			return 0;
		}
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
		double workNeededAdj = 0.0;
		while(workNeededAdj<dataExposeCost)
		{
			//year 1, q=1 in index 0.
			double workPerformedInYearq = calculateWorkPerformedInYearq(budget, workPerformedArray.size()+1);
			workNeededAdj+=workPerformedInYearq;
			workPerformedArray.add(workPerformedInYearq);
		}
		double workPerformedInLastYear = workPerformedArray.get(workPerformedArray.size()-1);
		double fraction = (dataExposeCost - (workNeededAdj - workPerformedInLastYear))/workPerformedInLastYear;
		return workPerformedArray.size()-1+fraction;
	}
	public double calculateWorkPerformedInYearq(double budget, int q)
	{
		double P1q = calculateP1q(q);
		double workPerformedInYearq = budget * P1q;
		return workPerformedInYearq;
	}
	public double calculateP1q(double q)
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
	public double calculateP1qFraction(double q)
	{
		double Pq = calculatePq(q);
		double hireSum = 0.0;
		for(int i=1;i<=q;i++)
		{
			hireSum+=Math.pow(1-attRate,i-1)*calculatePq(i);
		}
		double P1q = Pq*Math.pow(1-attRate,q)+hireRate*hireSum;
		return P1q;
	}

	public double calculatePq(double q)
	{
		return 1+sigma*Math.exp(-1*q*k);
	}
	
	public double calculateRet(double budget, double n)
	{
		return -1.0;
	}
	public void writeToAppConsole(double budget, double n, double savings)
	{
	}
	
	public double calculateBudgetForOneYear()
	{
		return dataExposeCost/calculatePq(1);
	}

	public double calculateRetForVariableTotal(double budget, double n,double totalNumYears)
	{
		calculateInvestment(budget,n);
		//if it takes the full time, there is no savings, just return the investment?
		if(totalNumYears == n)
			return -1*investment;
		//make the savings inflation/discount factor if applicable
		double savings =totalNumYears-n;
		if(inflDiscFactor!=1)
			savings = Math.pow(inflDiscFactor,n+1) * (1-Math.pow(inflDiscFactor,totalNumYears-n) ) / (1-inflDiscFactor);
		//multiply the savings for all years
		savings = savings * (numMaintenanceSavings - serMainPerc*dataExposeCost);
		savings = savings - investment;
		return savings;
	}
	public double calculateSavingsForVariableTotal(double budget, double n,double totalNumYears)
	{
		calculateInvestment(budget,n);
		//if it takes the full time, there is no savings, just return the investment?
		if(totalNumYears == n)
			return -1*investment;
		//make the savings inflation/discount factor if applicable
		double savings =totalNumYears-n;
		if(inflDiscFactor!=1)
			savings = Math.pow(inflDiscFactor,n+1) * (1-Math.pow(inflDiscFactor,totalNumYears-n) ) / (1-inflDiscFactor);
		//multiply the savings for all years
		savings = savings * (numMaintenanceSavings - serMainPerc*dataExposeCost);
		return savings;
	}
	
	public void calculateInvestment(double budget, double n)
	{
		double P1InflationSum = 0.0;
		for(int q=1;q<=n;q++)
		{
			double P1Inflation = 1.0;
			if(inflDiscFactor!=1)
				P1Inflation = Math.pow(inflDiscFactor, q-1);
			//P1Inflation *= calculateP1q(q);
			P1InflationSum += P1Inflation;
		}
		double extraYear = 1.0;
		if(inflDiscFactor!=1)
			extraYear = Math.pow(inflDiscFactor,n);
		P1InflationSum+=extraYear*(n-Math.floor(n));
		investment = budget * P1InflationSum;
	}
	
	public ArrayList<Double> createSustainmentCosts(double budget, double n)
	{
		ArrayList<Double> sustainmentList = new ArrayList<Double>();
		int index = 0;
		while(index<totalYrs)
		{
			if(index+1<n)
				sustainmentList.add(0.0);
			else if(index<n)
			{
				double factor=1.0;
				if(inflDiscFactor!=1)
					factor= Math.pow(inflDiscFactor,index+1);
				//double sustainment = factor*(numMaintenanceSavings - serMainPerc*investment)*(n-(index+1));
				double sustainment = factor*(serMainPerc*dataExposeCost)*((index+1)-n);
				sustainmentList.add(sustainment);
			}
			else
			{
				double factor=1.0;
				if(inflDiscFactor!=1)
					factor= Math.pow(inflDiscFactor,index+1);
				//double sustainment = factor*(numMaintenanceSavings - serMainPerc*investment);
				double sustainment = factor*(serMainPerc*dataExposeCost);
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
			if(index+1<n)//might need to say zero if null pointer, this should probably be adjusted to investment cost at each year.
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				double buildCost = factor*budget;
				installList.add(buildCost);
			}
			else if(index<n)
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				double buildCost = factor*budget*(n-(index));
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
			if(index+1<n||(index==0&&n==1))
				cumSavingsList.add(0.0);
			else if(index<n)
				cumSavingsList.add(calculateSavingsForVariableTotal(budget,n,Math.ceil(n)));
			else
				cumSavingsList.add(calculateSavingsForVariableTotal(budget,n,index+1));
			index++;
		}
		return cumSavingsList;
	}
	
	public ArrayList<Double> createBreakEven(double budget, double n)
	{
		ArrayList<Double> breakEvenList = new ArrayList<Double>();
		double buildCost=0.0;
		int index = 0;
		while(index<totalYrs)
		{
			if(index+1<n||(index==0&&n==1))
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				buildCost += -1*factor*budget;
				breakEvenList.add(buildCost);
			}
			else if(index<n)
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				buildCost += -1*factor*budget*(n-(index));
				breakEvenList.add(buildCost+calculateSavingsForVariableTotal(budget,n,Math.ceil(n)));
			}
			else
				breakEvenList.add(buildCost+calculateSavingsForVariableTotal(budget,n,index+1.0));
			//breakEvenList.add(buildCost+calculateRetForVariableTotal(budget,n,index+1.0));
			index++;
		}
		return breakEvenList;
	}

	public void createYearAdjuster(ArrayList<String> sysList, ArrayList<String> dataList, double hourlyCost)
	{
		yearAdjuster = new SysDecommissionOptimizationFunctions();
		yearAdjuster.setSysList(sysList);
		yearAdjuster.setDataList(dataList);
		yearAdjuster.setHourlyCost(hourlyCost);
		yearAdjuster.instantiate();
		yearAdjuster.calculateForAllSystems();
	}

}
