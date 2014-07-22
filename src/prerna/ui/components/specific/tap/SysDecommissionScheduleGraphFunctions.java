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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.impl.specific.tap.SysDecommissionScheduleOptimizer;
import prerna.algorithm.impl.specific.tap.SysDecommissionSchedulingSavingsOptimizer;

/**
 * This class is used to optimize graph functions used in services calculations.
 */
public class SysDecommissionScheduleGraphFunctions {
	protected SysDecommissionScheduleOptimizer scheduleOpt = null;
	protected SysDecommissionSchedulingSavingsOptimizer schedulingSavingsOpt = null;
	private double[] savingsEachYear;
	/**
	 * Sets the Univariate Service Optimizer.
	 * @param opt UnivariateSvcOptimizer
	 */
	public void setOptimzer (SysDecommissionScheduleOptimizer scheduleOpt) {
		this.scheduleOpt=scheduleOpt;
		this.schedulingSavingsOpt = scheduleOpt.getSavingsOpt();
	}
	/**
	 * Creates the build cost chart by putting budget information in the bar chart hash.
	
	 * @return Hashtable	Hashtable used to create the cost chart. */
	public Hashtable createBuildCostChart()
	{
		ArrayList<Double> yearCosts = schedulingSavingsOpt.getYearInvestment();
		double[] yearlyCosts = new double[yearCosts.size()];
		String[] yearCount = new String[yearCosts.size()];
		double investmentTotal = 0.0;
		for (int i=0 ;i< yearlyCosts.length;i++)
		{
			yearCount[i]="Year "+(i+1) +" Systems";
			yearlyCosts[i] = yearCosts.get(i);
			investmentTotal+=yearCosts.get(i);
		}
		scheduleOpt.investment = investmentTotal;
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("title",  "Cost to Transition Systems by Year");
		barChartHash.put("yAxisTitle", "Cost (Present-time value)");
		barChartHash.put("xAxisTitle", "Year Count");
		barChartHash.put("xAxis", yearCount);
		seriesHash.put("Data Expose Cost", yearlyCosts);
		colorHash.put("Data Expose Cost", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	
	/**
	 * Stores information about the annual cost savings for systems decommissioned implemented by year.
	 * Should this take into consideration the annual sustain cost too? Right now doesnt.
	
	 * @return Hashtable	Hashtable containing information about service savings for the bar graph. */
	public Hashtable createSavingsByYear()
	{
		ArrayList<Double> yearMaintenanceSavings = schedulingSavingsOpt.getYearSavings();
		double[] yearlyMaintenanceSavings = new double[yearMaintenanceSavings.size()];
		String[] yearCount = new String[yearMaintenanceSavings.size()];
		double savingsTotal = 0.0;
		for (int i=0 ;i<yearlyMaintenanceSavings.length;i++)
		{
			yearCount[i]="Year "+(i+1) +" Systems";
			yearlyMaintenanceSavings[i]=yearMaintenanceSavings.get(i);
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("title",  "Sustainment Cost Savings for Systems Transitioned by Year");
		barChartHash.put("yAxisTitle", "Savings (Present-time value)");
		barChartHash.put("xAxisTitle", "Year Count");
		barChartHash.put("xAxis", yearCount);
		seriesHash.put("Sustainment Cost Savings", yearlyMaintenanceSavings);
		colorHash.put("Sustainment Cost Savings", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
	}
	/**
	 * Uses budget information to create hashtables used to create bar charts about SOA Build and Sustainment Costs.
	 * Plots Learning-Adjusted Cost vs Fiscal Year.
	
	 * @return Hashtable	Hashtable used to create bar charts. */
	public Hashtable createCostChart()
	{
		int thisYear = 1;
		ArrayList<double[]> susPerYearList = createSusPerYear();
		int[] totalYearsAxis = new int[susPerYearList.get(0).length];
		for (int i=0;i<susPerYearList.get(0).length;i++)
		{
			totalYearsAxis[i]=thisYear+i;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("stack", "normal");
		barChartHash.put("title",  "System Transition and Sustainment Cost");
		barChartHash.put("yAxisTitle", "Cost (Present-time value)");
		barChartHash.put("xAxisTitle", "Year");
		barChartHash.put("xAxis", totalYearsAxis);
		for (int i=0;i<susPerYearList.size();i++)
		{
			double[] yearSeries = susPerYearList.get(i);
			seriesHash.put("Year "+(i+1) +" Systems", yearSeries);
		}
		colorHash.put("Transition Spending", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	public ArrayList<double[]> createSusPerYear()
	{
		ArrayList<Double> yearCosts = schedulingSavingsOpt.getYearInvestment();
	//	ArrayList<Double> yearSustainCosts = schedulingSavingsOpt.getYearInvestmentSustainment();

		ArrayList<double[]> susPerYearList = new ArrayList<double[]>();
		for (int i=0 ;i< yearCosts.size();i++)
		{
			double[] newYear = new double[yearCosts.size()];
			newYear[i]=Math.round(yearCosts.get(i));
//			newYear[i]=Math.round(yearCosts.get(i)*Math.pow((1+opt.infRate), i));
			for (int j=i+1;j<yearCosts.size();j++)
			{
				newYear[j]=Math.round(yearCosts.get(i)*schedulingSavingsOpt.getSerMainPerc());
//				newYear[j]=Math.round(yearCosts.get(i)*opt.serMainPerc*Math.pow((1+opt.infRate), j));
			}
			
			susPerYearList.add(newYear);
		}
		return susPerYearList;
	}
	
	/**
	 * Uses budget information to create hashtables used to create bar charts about SOA Build and Sustainment Costs.
	 * Plots Learning-Adjusted Cost vs Fiscal Year.
	
	 * @return Hashtable	Hashtable used to create bar charts. */
	public Hashtable createYearlySavings()
	{
		int thisYear = 1;
		ArrayList<double[]> yearlySavingsList = createSavingsPerYearList();
		int[] totalYearsAxis = new int[yearlySavingsList.get(0).length];
		for (int i=0;i<yearlySavingsList.get(0).length;i++)
		{
			totalYearsAxis[i]=thisYear+i;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("stack", "normal");
		barChartHash.put("title",  "Cumulative Sustainment Cost Savings for Systems Transitioned");
		barChartHash.put("yAxisTitle", "Savings (Present-time value)");
		barChartHash.put("xAxisTitle", "Year");
		barChartHash.put("xAxis", totalYearsAxis);
		//seriesHash.put("SOA Service Spending", yearlyBuildCosts);
		for (int i=0;i<yearlySavingsList.size();i++)
		{
			double[] yearSeries = yearlySavingsList.get(i);
			seriesHash.put("Year "+(i+1) +" Systems", yearSeries);
		}
		colorHash.put("Sustainment Cost Savings", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	public ArrayList<double[]> createSavingsPerYearList()
	{
		ArrayList<Double> yearCosts = schedulingSavingsOpt.getYearSavings();

		ArrayList<double[]> savingsPerYearList = new ArrayList<double[]>();
		savingsEachYear = new double[yearCosts.size()];
		for (int i=1;i< yearCosts.size();i++)
		{
			for (int j=0;j<i;j++)
			{
				savingsEachYear[i]+=Math.round(yearCosts.get(j));
//				newYear[j]=Math.round(yearCosts.get(i)*opt.serMainPerc*Math.pow((1+opt.infRate), j));
			}
		}
		double[] cumSavings = new double[yearCosts.size()];
		for (int i=1;i< yearCosts.size();i++)
			cumSavings[i] = cumSavings[i-1]+savingsEachYear[i];
		savingsPerYearList.add(cumSavings);
		double totalSavings = cumSavings[cumSavings.length-1];
		scheduleOpt.totalSavings = totalSavings;
		return savingsPerYearList;
	}
	
	/**
	 * Used to create a hashtable with breakeven information for services used to create a bar graph.
	
	 * @return Hashtable		Hashtable containing information about the balance over a time horizon. */
	public Hashtable createBreakevenGraph()
	{
		double[][] balanceList  = createBalanceList(0);
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "line");
		barChartHash.put("title",  "Balance Over Time Horizon");
		barChartHash.put("yAxisTitle", "Balance(Present-time value)");
		barChartHash.put("xAxisTitle", "Year");
		//barChartHash.put("xAxis", totalYearsAxis);
		seriesHash.put("Balance Line", balanceList);
		colorHash.put("Balance Line", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		barChartHash.put("xAxisInterval", 1);
		return barChartHash;
	}
	
	public double[][] createBalanceList(int thisYear)
	{
		ArrayList<Double> yearCosts = schedulingSavingsOpt.getYearInvestment();
//		savingsEachYear
		double[][] balanceList  = new double[savingsEachYear.length+1][2];
		balanceList[0][1]=0;
		balanceList[0][0]=thisYear;
		balanceList[1][1]=-yearCosts.get(0)+savingsEachYear[0];
		balanceList[1][0]=thisYear+1;
		for (int i=1; i<savingsEachYear.length;i++)
		{
			balanceList[i+1][0]=thisYear+i+1;
			balanceList[i+1][1]=balanceList[i][1]-yearCosts.get(i)+savingsEachYear[i];
		}
		return balanceList;
	}


}
