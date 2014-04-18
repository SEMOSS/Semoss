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

import prerna.algorithm.impl.specific.tap.SysNetSavingsOptimizer;

/**
 * This class is used to optimize graph functions used in services calculations.
 */
public class SysOptGraphFunctions extends SerOptGraphFunctions{

	@Override
	public Hashtable createCostChart()
	{
		int thisYear = 2014;
		ArrayList<double[]> susPerYearList = createSusPerYear();
		int[] totalYearsAxis = new int[opt.maxYears];
		for (int i=0;i<opt.maxYears;i++)
		{
			totalYearsAxis[i]=thisYear+i;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("stack", "normal");
		barChartHash.put("title",  "Build and Sustainment Cost ");
		barChartHash.put("yAxisTitle", "Learning-Adjusted Cost (Actual-time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		barChartHash.put("xAxis", totalYearsAxis);
		double[] buildSeries = susPerYearList.get(0);
		seriesHash.put("Build Costs", buildSeries);
		double[] sustainSeries = susPerYearList.get(1);
		seriesHash.put("Sustainment Costs", sustainSeries);
		colorHash.put("Build Costs", "#4572A7");
		colorHash.put("Sustainment Costs", "#80699B");
		
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	
	@Override
	public Hashtable createCumulativeSavings()
	{
		Hashtable barChartHash = super.createCumulativeSavings();
		Hashtable seriesHash = (Hashtable)barChartHash.get("dataSeries");
		seriesHash.put("Savings",seriesHash.remove("Year 1 Services"));
		return barChartHash;
	}
	
	
	@Override
	public ArrayList<double[]> createSusPerYear()
	{
		ArrayList<double[]> susPerYearList = new ArrayList<double[]>();
		double[] buildCost = new double[opt.maxYears];
		for(int i=0;i<opt.maxYears;i++)
		{
			buildCost[i] = ((SysNetSavingsOptimizer)opt).installCostList.get(i);
		}
		double[] sustainCost = new double[opt.maxYears];
		for(int i=0;i<opt.maxYears;i++)
		{
			sustainCost[i] = ((SysNetSavingsOptimizer)opt).sustainCostList.get(i);
		}
		susPerYearList.add(buildCost);
		susPerYearList.add(sustainCost);		
		return susPerYearList;
	}
	
	@Override
	public ArrayList<double[]> createSavingsPerYearList()
	{
		ArrayList<double[]> savingsPerYearList = new ArrayList<double[]>();
		for (int i=0 ;i< 1;i++)
		{
			double[] newYear = new double[opt.maxYears];
			for (int j=i;j<opt.maxYears;j++)
			{
				newYear[j]=((SysNetSavingsOptimizer)opt).cumSavingsList.get(j);
			}
			savingsPerYearList.add(newYear);
		}
		return savingsPerYearList;
	}
	
	@Override
	public double[][] createBalanceList(int thisYear)
	{
		thisYear = 2014;
		double[][] balanceList  = new double[opt.maxYears][2];
		for (int i=0; i<opt.maxYears;i++)
		{	
			balanceList[i][0]=thisYear+i;
			balanceList[i][1]=((SysNetSavingsOptimizer)opt).breakEvenList.get(i);
		}
		return balanceList;
	}
	

}
