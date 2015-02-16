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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.impl.specific.tap.ServiceOptimizer;
import prerna.algorithm.impl.specific.tap.UnivariateSvcOptimizer;

/**
 * This class is used to optimize graph functions used in services calculations.
 */
public class SerOptGraphFunctions extends OptGraphFunctions {
	protected UnivariateSvcOptimizer opt = null;
	protected ServiceOptimizer lin = null;
//	protected double iniLC;
//	protected double scdLT;
//	protected double scdLC;
//	protected double[] learningConstants;
//	protected int maxYears;
	/**
	 * Sets the Univariate Service Optimizer.
	 * @param opt UnivariateSvcOptimizer
	 */
	public void setOptimzer (UnivariateSvcOptimizer opt)
	{
		this.opt=opt;
		this.iniLC=opt.iniLC;
		this.scdLT = opt.scdLT;
		this.scdLC = opt.scdLC;
		this.learningConstants = opt.f.learningConstants;
		this.maxYears = opt.maxYears;
	}
	
	/**
	 * Sets service optimizer for TAP specific services.
	 * @param lin ServiceOptimizer
	 */
	public void setSvcOpt (ServiceOptimizer lin) 
	{
		this.lin=lin;
	}
	
	/**
	 * Uses budget information to create hashtables used to create bar charts about SOA Build and Sustainment Costs.
	 * Plots Learning-Adjusted Cost vs Fiscal Year.
	
	 * @return Hashtable	Hashtable used to create bar charts. */
	public Hashtable createCostChart()
	{
		int thisYear = 2014;
		ArrayList<double[]> susPerYearList = createSusPerYear();
		int[] totalYearsAxis = new int[maxYears];
		for (int i=0;i<maxYears;i++)
		{
			totalYearsAxis[i]=thisYear+i;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("stack", "normal");
		barChartHash.put("title",  "SOA Build and Sustainment Cost ");
		barChartHash.put("yAxisTitle", "Learning-Adjusted Cost (Actual-time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		barChartHash.put("xAxis", totalYearsAxis);
		//seriesHash.put("SOA Service Spending", yearlyBuildCosts);
		for (int i=0;i<susPerYearList.size();i++)
		{
			double[] yearSeries = susPerYearList.get(i);
			seriesHash.put("Year "+i +" Services", yearSeries);
			//use high charts default charts
			//colorHash.put("Year "+(i+1) +" Services", colorArray.get(i));
		}
		colorHash.put("SOA Service Spending", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	public ArrayList<double[]> createSusPerYear()
	{
		ArrayList<double[]> susPerYearList = new ArrayList<double[]>();
		for (int i=0 ;i< lin.actualBudgetList.size();i++)
		{
			//yearlyBuildCosts[i]=lin.actualBudgetList.get(i);
			double[] newYear = new double[maxYears];
			newYear[i]=Math.round(lin.actualBudgetList.get(i)*Math.pow((1+opt.infRate), i));
			for (int j=i+1;j<maxYears;j++)
			{
				newYear[j]=Math.round(lin.actualBudgetList.get(i)*opt.serMainPerc*Math.pow((1+opt.infRate), j));
			}
			
			susPerYearList.add(newYear);
		}
		return susPerYearList;
	}

	/**
	 * Creates the build cost chart by putting budget information in the bar chart hash.
	
	 * @return Hashtable	Hashtable used to create the cost chart. */
	public Hashtable createBuildCostChart()
	{
		double[] yearlyCosts = new double[lin.actualBudgetList.size()];
		String[] yearCount = new String[lin.actualBudgetList.size()];
		int thisYear = 2013;
		for (int i=0 ;i< lin.actualBudgetList.size();i++)
		{
			yearCount[i]="Year "+(i+1) +" Services";
			yearlyCosts[i]=lin.actualBudgetList.get(i);
		}
		int[] totalYearsAxis = new int[lin.actualBudgetList.size()];
		for (int i=0;i<lin.actualBudgetList.size();i++)
		{
			totalYearsAxis[i]=thisYear+i+1;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("title",  "Annual SOA Service Build Cost");
		barChartHash.put("yAxisTitle", "Cost (Actual-time value)");
		barChartHash.put("xAxisTitle", "Year Count");
		barChartHash.put("xAxis", yearCount);
		seriesHash.put("Service Build Cost", yearlyCosts);
		colorHash.put("Service Build Cost", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	
	/**
	 * Stores information about the annual cost savings for services implemented by year.
	
	 * @return Hashtable	Hashtable containing information about service savings for the bar graph. */
	public Hashtable createServiceSavings()
	{
		double[] yearlyMaintenanceSavings = new double[lin.actualBudgetList.size()];
		String[] yearCount = new String[lin.actualBudgetList.size()];
		
		for (int i=0 ;i< lin.actualBudgetList.size();i++)
		{
			yearCount[i]="Year "+(i+1) +" Services";
			yearlyMaintenanceSavings[i]=lin.objectiveValueList.get(i);
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("title",  "Annual Cost Savings for Services Implemented by Year");
		barChartHash.put("yAxisTitle", "Savings (Present-time value)");
		barChartHash.put("xAxisTitle", "Year Count");
		barChartHash.put("xAxis", yearCount);
		seriesHash.put("Sustainment Cost Savings", yearlyMaintenanceSavings);
		colorHash.put("Sustainment Cost Savings", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
	}
	
	@Override
	public ArrayList<double[]> createSavingsPerYearList()
	{
		ArrayList<double[]> savingsPerYearList = new ArrayList<double[]>();
		for (int i=0 ;i< lin.actualBudgetList.size();i++)
		{
			double[] newYear = new double[maxYears];
			for (int j=i+1;j<maxYears;j++)
			{
				newYear[j]=Math.round(lin.objectiveValueList.get(i)*Math.pow((1+opt.infRate), j));
			}
			
			savingsPerYearList.add(newYear);
		}
		return savingsPerYearList;
	}
	
	@Override
	public double[][] createBalanceList(int thisYear)
	{
		double[][] balanceList  = new double[maxYears+1][2];
		balanceList[0][1]=0;
		balanceList[0][0]=thisYear;
		balanceList[1][1]=-lin.actualBudgetList.get(0);
		balanceList[1][0]=thisYear+1;
		for (int i=1; i<maxYears;i++)
		{
			if (i<lin.actualBudgetList.size())
			{
				balanceList[i+1][1]=balanceList[i][1]-lin.actualBudgetList.get(i);
				for (int j=0; j<i;j++)
				{
					balanceList[i+1][1]=balanceList[i+1][1]+lin.objectiveValueList.get(j);
				}
			}
			else
			{
				balanceList[i+1][1]=balanceList[i][1];
				for (int j=0; j<lin.objectiveValueList.size();j++)
				{
					balanceList[i+1][1]=balanceList[i+1][1]+lin.objectiveValueList.get(j);
				}
			}
			balanceList[i+1][0]=thisYear+i+1;

		}
		return balanceList;
	}

}
