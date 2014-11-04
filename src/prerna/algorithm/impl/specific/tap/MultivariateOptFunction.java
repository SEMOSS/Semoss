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

import javax.swing.JProgressBar;
import javax.swing.JTextArea;

import org.apache.commons.math3.analysis.MultivariateFunction;

import prerna.ui.components.specific.tap.SysDecommissionOptimizationFunctions;

/**
 * Interface representing a multivariate real function that is implemented for TAP system optimization functions.
 */
public class MultivariateOptFunction implements MultivariateFunction{
	
	SysDecommissionOptimizationFunctions yearAdjuster;
	double numMaintenanceSavings, serMainPerc, dataExposeCost;
	double preTransitionMaintenanceCost, postTransitionMaintenanceCost;
	double k, sigma, inflDiscFactor;
	
	double investment;
	ArrayList<Double> workPerformedArray;
	public boolean solutionExists = false;
	
	public int totalYrs;
	double cnstC, cnstK;
	public double attRate, hireRate, infRate, disRate;
	
	public double[] learningConstants;
	int count = 0;
	
	JTextArea consoleArea;
	boolean write = true;
	JProgressBar progressBar;
	
	/**
	 * Sets variables used in the optimization. 
	 * @param 	numberOfYears		Total number of years service is used.
	 * @param 	attRate				Attrition rate (how many employees leave) over a year.
	 * @param 	hireRate			Hire rate over the year.
	 * @param 	infRate				Inflation rate over the year.
	 * @param 	disRate				Discount rate over the year.
	 * @param 	secondProYear		Second pro year - year in which more information is known.
	 * @param 	initProc			How much information you have initially.
	 * @param 	secondProc			How much information you have at second pro year.
	 */
	public void setVariables(int numberOfYears, double attRate, double hireRate, double infRate, double disRate, int scdLT, double iniLC, double scdLC,double numMaintenanceSavings,double serMainPerc,double dataExposeCost, double preTransitionMaintenanceCost, double postTransitionMaintenanceCost){
		this.attRate = attRate;
		this.hireRate = hireRate;
		this.infRate = infRate;
		this.disRate = disRate;
		this.totalYrs = numberOfYears;
		this.numMaintenanceSavings = numMaintenanceSavings;
		this.serMainPerc = serMainPerc;
		this.dataExposeCost = dataExposeCost;
		this.preTransitionMaintenanceCost = preTransitionMaintenanceCost;
		this.postTransitionMaintenanceCost = postTransitionMaintenanceCost;
		this.k = (1.0/scdLT)*Math.log(((1.0-iniLC)/(1.0-scdLC)));
		this.sigma = ((1.0-iniLC)/k)*(1-Math.exp(k));
		this.inflDiscFactor = (1.0+infRate) / (1.0+disRate);
		createLearningYearlyConstants(numberOfYears, scdLT, iniLC, scdLC);
	}
	
	/**
	 * Given a budget, calculate the years in Savings.
	 * Gets the lists of potential yearly savings and yearly budgets and can write to the app console for each iteration.
	 * @param a 		Budget used in the service optimizer.
	 * @return double	Cumulative Net Savings */
	@Override
	public double value(double[] a) {
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
		//TODO add back in the adjustments
//		double nAdjusted = yearAdjuster.adjustTimeToTransform(a, n);
//		if(Math.abs(nAdjusted-n)>.1)
//		double savings = calculateRet(a,nAdjusted);
		double savings = calculateRet(a,n);
		writeToAppConsole(a,n,savings);
		return savings;
	}
	
	/**
	 * Sets the console area.
	 * @param JTextArea		Console area.
	 */
	public void setConsoleArea (JTextArea consoleArea)
	{
		this.consoleArea=consoleArea;
	}

	/**
	 * Sets properties of the progress bar.
	 * @param bar	Original bar that updates are made to.
	 */
	public void setProgressBar (JProgressBar bar)
	{
		this.progressBar=bar;
		this.progressBar.setVisible(true);
		this.progressBar.setIndeterminate(true);
		this.progressBar.setStringPainted(true);
	}
	
	/**
	 * Sets the value of the progress string on the progress bar.
	 * @param text	Text to be set.
	 */
	public void updateProgressBar (String text)
	{

		this.progressBar.setString(text);
	}
	
	/**
	 * Sets the write boolean.
	 * @param write	Boolean that is either true or false depending on optimization.
	 */
	public void setWriteBoolean (boolean write)
	{
		this.write = write;
	}
	
	
	/**
	 * Solve differential equations in order to obtain the learning curve yearly constants for a service.
	 * @param 	numberOfYears	Number of years service is used.
	 * @param 	secondProYear	Second pro year - year in which more information is known.
	 * @param 	initProC		How much information you have initially.
	 * @param 	secondProC		How much information you have at second pro year.
	
	 * @return 	An array of doubles containing the learning curve constants. */
	public double[] createLearningYearlyConstants(int numberOfYears, int secondProYear, double initProC, double secondProC)
	{
		//learning curve differential equation f(t) = Pmax-C*e^(-k*t)
		//after you solve for differential equation to get constants
		//here are the equations for the constants
		
		cnstC = 1.0-initProC;
		cnstK = (1.0/secondProYear)*Math.log((1.0-initProC)/(1.0-secondProC));
		int yearToCalc = Math.max(totalYrs, 10);
		learningConstants = new double[yearToCalc];
		double[] origLearningConstants = new double[yearToCalc];
		for (int i = 0; i<origLearningConstants.length;i++)
		{
			origLearningConstants[i]=1.0+(cnstC/cnstK)*Math.exp(-(i+1.0)*cnstK)*(1.0-Math.exp(cnstK));
		}
		for (int i = 0; i<learningConstants.length;i++)
		{
			//account for turnover
			//ensure number of iterations does not pass
			for (int j=0;j<i;j++)
			{
				learningConstants[i]=learningConstants[i]+origLearningConstants[j]*hireRate*Math.pow(1-attRate, j);
			}
			learningConstants[i]=learningConstants[i]+origLearningConstants[i]*Math.pow((1-attRate),i);
		}
		return learningConstants;
		
	}
	public double calculateYears(double[] budget)
	{
		int year = 0;
		workPerformedArray = new ArrayList<Double>();
		double workNeededAdj = 0.0;
		while(workNeededAdj<dataExposeCost&&year<budget.length)
		{
			//year 1, q=1 in index 0.
			double workPerformedInYearq = calculateWorkPerformedInYearq(budget[year], workPerformedArray.size()+1);
			workNeededAdj+=workPerformedInYearq;
			workPerformedArray.add(workPerformedInYearq);
			year++;
		}
		if(workNeededAdj<dataExposeCost) {
			return budget.length;
		}
			double workPerformedInLastYear = workPerformedArray.get(year-1);
			double fraction = (dataExposeCost - (workNeededAdj - workPerformedInLastYear)) / workPerformedInLastYear;
			return year - 1 + fraction;
	}

	protected double calculateWorkPerformedInYearq(double budget, int q)
	{
		double P1q = calculateP1q(q);
		double workPerformedInYearq = budget * P1q;
		return workPerformedInYearq;
	}
	private double calculateP1q(double q)
	{
		double Pq = calculatePq(q);
		double hireSum = 0.0;
		for(int i=1;i<=q-1;i++)
		{
			hireSum+=Math.pow(1.0-attRate,i-1.0)*calculatePq(i);
		}
		double P1q = Pq*Math.pow(1.0-attRate,q-1.0)+hireRate*hireSum;
		return P1q;
	}
	private double calculateP1qFraction(double q)
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

	private double calculatePq(double q)
	{
		return 1.0+sigma*Math.exp(-1.0*q*k);
	}
	
	protected double calculateRet(double[] budget, double n)
	{
		return -1.0;
	}
	protected void writeToAppConsole(double[] budget, double n, double savings)
	{
	}
	
	public double calculateBudgetForOneYear()
	{
		return dataExposeCost/calculatePq(1);
	}

	protected double calculateSavingsForVariableTotal(double n,double totalNumYears)
	{
		//if it takes the full time, there is no savings, just return 0
		if(totalNumYears == n)
			return 0;
		//make the savings inflation/discount factor if applicable
		double savings =totalNumYears-n;
		if(inflDiscFactor!=1)
			savings = Math.pow(inflDiscFactor,n+1.0) * (1.0-Math.pow(inflDiscFactor,totalNumYears-n) ) / (1.0-inflDiscFactor);
		//multiply the savings for all years
		savings = savings * (numMaintenanceSavings - serMainPerc*dataExposeCost);
		return savings;
	}
	
	
	protected void calculateInvestment(double[] budget, double n)
	{
		investment = 0.0;
		int q=0;
		double P1Inflation = 1.0;
		for(q=0; q<n-1; q++) {
			P1Inflation = 1.0;
			if(inflDiscFactor!=1)
				P1Inflation = Math.pow(inflDiscFactor, q);
			investment += budget[q]* P1Inflation;
		}
		if(inflDiscFactor!=1)
			P1Inflation = Math.pow(inflDiscFactor,q);
		double fraction = n - Math.floor(n);
		double budgetUsedInLastYear = budget[q] * P1Inflation * fraction;
		investment+=budgetUsedInLastYear;
	}
	
	public ArrayList<Double> createSustainmentCosts(double n)
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
				double sustainment = factor*(serMainPerc*dataExposeCost)*((index+1)-n);
				sustainmentList.add(sustainment);
			}
			else
			{
				double factor=1.0;
				if(inflDiscFactor!=1)
					factor= Math.pow(inflDiscFactor,index+1);
				double sustainment = factor*(serMainPerc*dataExposeCost);
				sustainmentList.add(sustainment);
			}
			index++;
		}
		return sustainmentList;
	}
	public ArrayList<Double> createInstallCosts(double[] budget, double n)
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
				double buildCost = factor*budget[index];
				installList.add(buildCost);
			}
			else if(index<n)
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				double buildCost = factor*budget[index]*(n-(index));
				installList.add(buildCost);
			}
			else
				installList.add(0.0);
			index++;
		}
		return installList;
	}
	public ArrayList<Double> createWorkDoneCosts(double[] budget, double n)
	{
		ArrayList<Double> workDoneList = new ArrayList<Double>();
		int index = 0;
		while(index<totalYrs)
		{
			if(index+1<n)//might need to say zero if null pointer, this should probably be adjusted to investment cost at each year.
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				//double buildCost = factor*budget[index];
				double buildCost = factor*calculateWorkPerformedInYearq(budget[index],index+1);
				workDoneList.add(buildCost);
			}
			else if(index<n)
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				double workPerformedInLastYear = workPerformedArray.get(index);
				double fraction = n - Math.floor(n);
				double workPerformedInLastFraction = factor * workPerformedInLastYear * fraction;
				workDoneList.add(workPerformedInLastFraction);
			}
			else
				workDoneList.add(0.0);
			index++;
		}
		return workDoneList;
	}
	
	public ArrayList<Double> createCumulativeSavings(double n)
	{
		ArrayList<Double> cumSavingsList = new ArrayList<Double>();
		int index = 0;
		while(index<totalYrs)
		{
			if(index+1<n||(index==0&&n==1))
				cumSavingsList.add(0.0);
			else if(index<n)
				cumSavingsList.add(calculateSavingsForVariableTotal(n,Math.ceil(n)));
			else
				cumSavingsList.add(calculateSavingsForVariableTotal(n,index+1));
			index++;
		}
		return cumSavingsList;
	}
	
	public ArrayList<Double> createBreakEven(double[] budget, double n)
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
				buildCost += -1*factor*budget[index];
				breakEvenList.add(buildCost);
			}
			else if(index<n)
			{
				double factor = 1.0;
				if(inflDiscFactor!=1)
					factor = Math.pow(inflDiscFactor,index);
				buildCost += -1*factor*budget[index]*(n-(index));
				breakEvenList.add(buildCost+calculateSavingsForVariableTotal(n,Math.ceil(n)));
			}
			else
				breakEvenList.add(buildCost+calculateSavingsForVariableTotal(n,index+1.0));
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
