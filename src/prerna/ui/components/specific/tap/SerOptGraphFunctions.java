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

import prerna.algorithm.impl.specific.tap.ServiceOptimizer;
import prerna.algorithm.impl.specific.tap.UnivariateSvcOptimizer;

/**
 * This class is used to optimize graph functions used in services calculations.
 */
public class SerOptGraphFunctions {
	protected UnivariateSvcOptimizer opt = null;
	protected ServiceOptimizer lin = null;
	/**
	 * Sets the Univariate Service Optimizer.
	 * @param opt UnivariateSvcOptimizer
	 */
	public void setOptimzer (UnivariateSvcOptimizer opt)
	{
		this.opt=opt;
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
		double[] yearlyBuildCosts = new double[lin.actualBudgetList.size()];
		int thisYear = 2013;
		ArrayList<double[]> susPerYearList = new ArrayList<double[]>();
		for (int i=0 ;i< lin.actualBudgetList.size();i++)
		{
			//yearlyBuildCosts[i]=lin.actualBudgetList.get(i);
			double[] newYear = new double[opt.maxYears];
			newYear[i]=Math.round(lin.actualBudgetList.get(i)*Math.pow((1+opt.infRate), i));
			for (int j=i+1;j<opt.maxYears;j++)
			{
				newYear[j]=Math.round(lin.actualBudgetList.get(i)*opt.serMainPerc*Math.pow((1+opt.infRate), j));
			}
			
			susPerYearList.add(newYear);
		}
		int[] totalYearsAxis = new int[opt.maxYears];
		for (int i=0;i<opt.maxYears;i++)
		{
			totalYearsAxis[i]=thisYear+i+1;
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
			seriesHash.put("Year "+(i+1) +" Services", yearSeries);
			//use high charts default charts
			//colorHash.put("Year "+(i+1) +" Services", colorArray.get(i));
		}
		colorHash.put("SOA Service Spending", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
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
	
	/**
	 * Stores information about the learning curve, including work efficiency against learning curve.
	
	 * @return Hashtable 	Hashtable with information about the learning curve. */
	public Hashtable createLearningCurve()
	{
		double[][] data= createLearningCurvePoints();
		double[][] data2 = new double[opt.f.learningConstants.length][2];
		double nextYear = 2014;
		for (int i=0;i<data2.length;i++)
		{
			data2[i][0]=nextYear +((double)i)+.5;
			data2[i][1]=opt.f.learningConstants[i];
		}
		Hashtable curveChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		curveChartHash.put("type",  "spline");
		curveChartHash.put("title",  "Learning Curve");
		curveChartHash.put("yAxisTitle", "Work Efficiency");
		curveChartHash.put("xAxisTitle", "Year");
		//curveChartHash.put("xAxis", xAxis);
		seriesHash.put("Learning Curve", data);
		seriesHash.put("Learning Curve with Retention Rate", data2);
		colorHash.put("Learning Curve", "#4572A7");
		colorHash.put("Learning Curve with Retention Rate", "#80699B");
		curveChartHash.put("dataSeries",  seriesHash);
		curveChartHash.put("colorSeries", colorHash);
		curveChartHash.put("xAxisInterval", 1);
		return curveChartHash;
	}
	
	/**
	 * Used to create a hashtable that stores information about cumulative savings used in a bar graph.
	
	 * @return Hashtable 	Contains information about cumulative sustainment savings. */
	public Hashtable createCumulativeSavings()
	{
		int thisYear = 2013;
		ArrayList<double[]> savingsPerYearList = createSavingsPerYearList();
		int[] totalYearsAxis = new int[opt.maxYears];
		for (int i=0;i<opt.maxYears;i++)
		{
			totalYearsAxis[i]=thisYear+i+1;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("stack", "normal");
		barChartHash.put("title",  "Accumulative Sustainment Savings");
		barChartHash.put("yAxisTitle", "Savings (Actual-time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		barChartHash.put("xAxis", totalYearsAxis);
		for (int i=0;i<savingsPerYearList.size();i++)
		{
			double[] yearSeries = savingsPerYearList.get(i);
			seriesHash.put("Year "+(i+1) +" Services", yearSeries);
			//use high charts default charts
			//colorHash.put("Year "+(i+1) +" Services", colorArray.get(i));
		}
		
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
	}
	public ArrayList<double[]> createSavingsPerYearList()
	{
		ArrayList<double[]> savingsPerYearList = new ArrayList<double[]>();
		for (int i=0 ;i< lin.actualBudgetList.size();i++)
		{
			double[] newYear = new double[opt.maxYears];
			for (int j=i+1;j<opt.maxYears;j++)
			{
				newYear[j]=Math.round(lin.objectiveValueList.get(i)*Math.pow((1+opt.infRate), j));
			}
			
			savingsPerYearList.add(newYear);
		}
		return savingsPerYearList;
	}
	
	/**
	 * Used to create a hashtable with breakeven information for services used to create a bar graph.
	
	 * @return Hashtable		Hashtable containing information about the balance over a time horizon. */
	public Hashtable createBreakevenGraph()
	{
		int thisYear = 2013+1;
		double[][] balanceList  = createBalanceList(thisYear);
		int[] totalYearsAxis = new int[opt.maxYears];
		for (int i=0;i<opt.maxYears;i++)
		{
			totalYearsAxis[i]=thisYear+i+1;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "line");
		barChartHash.put("title",  "Balance Over Time Horizon");
		barChartHash.put("yAxisTitle", "Balance(Actual time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
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
		double[][] balanceList  = new double[opt.maxYears+1][2];
		balanceList[0][1]=0;
		balanceList[0][0]=thisYear;
		balanceList[1][1]=-lin.actualBudgetList.get(0);
		balanceList[1][0]=thisYear+1;
		for (int i=1; i<opt.maxYears;i++)
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
	
	
	/**
	 * Solves differential equations that are used to create the learning curve.
	
	 * @return double[][]	Contains values for the learning curve. */
	public double[][] createLearningCurvePoints()
	{
		//learning curve differential equation f(t) = Pmax-C*e^(-k*t)
		//after you solve for differential equation to get constants
		//here are the equations for the constants
		double cnstC, cnstK;
		cnstC = 1.0-opt.iniLC;
		double nextYear = 2014;
		cnstK = (1.0/opt.scdLT)*Math.log((1.0-opt.iniLC)/(1.0-opt.scdLC));
		double[][] learningCurve = new double[opt.f.learningConstants.length*10][2];
		for (int i = 0; i<opt.f.learningConstants.length*10;i++)
		{
			double x =nextYear+((double) i)/10;
			learningCurve[i][1]=1.0-cnstC*Math.exp(-(((double) i)/10)*cnstK);
			learningCurve[i][0]=x;
		}
		return learningCurve;
		
	}
	

}
