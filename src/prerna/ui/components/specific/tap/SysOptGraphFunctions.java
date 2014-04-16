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
		double[][] balanceList  = new double[opt.maxYears][2];
		for (int i=0; i<opt.maxYears;i++)
		{	
			balanceList[i][0]=thisYear+i;
			balanceList[i][1]=((SysNetSavingsOptimizer)opt).breakEvenList.get(i);
		}
		return balanceList;
	}
	
//	/**
//	 * Used to create a hashtable that stores information about cumulative savings used in a bar graph.
//	
//	 * @return Hashtable 	Contains information about cumulative sustainment savings. */
//	public Hashtable createCumulativeSavings()
//	{
//		int thisYear = 2014;
//		ArrayList<double[]> savingsPerYearList = new ArrayList<double[]>();
//		for (int i=0 ;i< 1;i++)
//		{
//			double[] newYear = new double[opt.maxYears];
//			for (int j=i;j<opt.maxYears;j++)
//			{
//				newYear[j]=((SysNetSavingsOptimizer)opt).cumSavingsList.get(j);
//			}
//			savingsPerYearList.add(newYear);
//		}
//		int[] totalYearsAxis = new int[opt.maxYears];
//		for (int i=0;i<opt.maxYears;i++)
//		{
//			totalYearsAxis[i]=thisYear+i;
//		}
//		Hashtable barChartHash = new Hashtable();
//		Hashtable seriesHash = new Hashtable();
//		Hashtable colorHash = new Hashtable();
//		barChartHash.put("type",  "column");
//		barChartHash.put("stack", "normal");
//		barChartHash.put("title",  "Accumulative Sustainment Savings");
//		barChartHash.put("yAxisTitle", "Savings (Actual-time value)");
//		barChartHash.put("xAxisTitle", "Fiscal Year");
//		barChartHash.put("xAxis", totalYearsAxis);
//		for (int i=0;i<savingsPerYearList.size();i++)
//		{
//			double[] yearSeries = savingsPerYearList.get(i);
//			seriesHash.put("Savings", yearSeries);
//			//use high charts default charts
//			//colorHash.put("Year "+(i+1) +" Services", colorArray.get(i));
//		}
//		
//		barChartHash.put("dataSeries",  seriesHash);
//		barChartHash.put("colorSeries", colorHash);
//		return barChartHash;
//	}
	
	
	/**
	 * Used to create a hashtable with breakeven information for services used to create a bar graph.
	
	 * @return Hashtable		Hashtable containing information about the balance over a time horizon. */
//	public Hashtable createBreakevenGraph()
//	{
//		int thisYear = 2014;
//		double[][] balanceList  = new double[opt.maxYears][2];
//		for (int i=0; i<opt.maxYears;i++)
//		{	
//			balanceList[i][0]=thisYear+i;
//			balanceList[i][1]=((SysNetSavingsOptimizer)opt).breakEvenList.get(i);
//		}
//		int[] totalYearsAxis = new int[opt.maxYears];
//		for (int i=0;i<opt.maxYears;i++)
//		{
//			totalYearsAxis[i]=thisYear+i+1;
//		}
//		Hashtable barChartHash = new Hashtable();
//		Hashtable seriesHash = new Hashtable();
//		Hashtable colorHash = new Hashtable();
//		barChartHash.put("type",  "line");
//		barChartHash.put("title",  "Balance Over Time Horizon");
//		barChartHash.put("yAxisTitle", "Balance(Actual time value)");
//		barChartHash.put("xAxisTitle", "Fiscal Year");
//		//barChartHash.put("xAxis", totalYearsAxis);
//		seriesHash.put("Balance Line", balanceList);
//		colorHash.put("Balance Line", "#4572A7");
//		barChartHash.put("dataSeries",  seriesHash);
//		barChartHash.put("colorSeries", colorHash);
//		barChartHash.put("xAxisInterval", 1);
//		return barChartHash;
//	}
	
	
//	/**
//	 * Solves differential equations that are used to create the learning curve.
//	
//	 * @return double[][]	Contains values for the learning curve. */
//	public double[][] createLearningCurvePoints()
//	{
//		//learning curve differential equation f(t) = Pmax-C*e^(-k*t)
//		//after you solve for differential equation to get constants
//		//here are the equations for the constants
//		double cnstC, cnstK;
//		cnstC = 1.0-opt.iniLC;
//		double nextYear = 2014;
//		cnstK = (1.0/opt.scdLT)*Math.log((1.0-opt.iniLC)/(1.0-opt.scdLC));
//		double[][] learningCurve = new double[opt.f.learningConstants.length*10][2];
//		for (int i = 0; i<opt.f.learningConstants.length*10;i++)
//		{
//			double x =nextYear+((double) i)/10;
//			learningCurve[i][1]=1.0-cnstC*Math.exp(-(((double) i)/10)*cnstK);
//			learningCurve[i][0]=x;
//		}
//		return learningCurve;
//		
//	}
	

}
