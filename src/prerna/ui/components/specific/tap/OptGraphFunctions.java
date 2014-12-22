/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.impl.specific.tap.UnivariateOpt;
import prerna.algorithm.impl.specific.tap.UnivariateSvcOptimizer;

/**
 * This class is used to optimize graph functions used in services calculations.
 */
public class OptGraphFunctions {
//	protected UnivariateOpt opt = null;
	protected double iniLC;
	protected double scdLT;
	protected double scdLC;
	protected double[] learningConstants;
	protected int maxYears;
//	/**
//	 * Sets the Univariate Service Optimizer.
//	 * @param opt UnivariateSvcOptimizer
//	 */
//	public void setOptimzer (UnivariateOpt opt)
//	{
//		this.opt=opt;
//		this.iniLC=opt.iniLC;
//		this.scdLT = opt.scdLT;
//		this.scdLC = opt.scdLC;
//		this.learningConstants = opt.f.learningConstants;
//		this.maxYears = opt.maxYears;
//	}
	
	/**
	 * Stores information about the learning curve, including work efficiency against learning curve.
	
	 * @return Hashtable 	Hashtable with information about the learning curve. */
	public Hashtable createLearningCurve()
	{
		double[][] data= createLearningCurvePoints();
		double[][] data2 = new double[learningConstants.length][2];
		double nextYear = 2014;
		for (int i=0;i<data2.length;i++)
		{
			data2[i][0]=nextYear +((double)i)+.5;
			data2[i][1]=learningConstants[i];
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
	 * Solves differential equations that are used to create the learning curve.
	
	 * @return double[][]	Contains values for the learning curve. */
	public double[][] createLearningCurvePoints()
	{
		//learning curve differential equation f(t) = Pmax-C*e^(-k*t)
		//after you solve for differential equation to get constants
		//here are the equations for the constants
		double cnstC, cnstK;
		cnstC = 1.0-iniLC;
		double nextYear = 2014;
		cnstK = (1.0/scdLT)*Math.log((1.0-iniLC)/(1.0-scdLC));
		double[][] learningCurve = new double[learningConstants.length*10][2];
		for (int i = 0; i<learningConstants.length*10;i++)
		{
			double x =nextYear+((double) i)/10;
			learningCurve[i][1]=1.0-cnstC*Math.exp(-(((double) i)/10)*cnstK);
			learningCurve[i][0]=x;
		}
		return learningCurve;
		
	}
	
	/**
	 * Used to create a hashtable that stores information about cumulative savings used in a bar graph.
	
	 * @return Hashtable 	Contains information about cumulative sustainment savings. */
	public Hashtable createCumulativeSavings()
	{
		int thisYear = 2013;
		ArrayList<double[]> savingsPerYearList = createSavingsPerYearList();
		int[] totalYearsAxis = new int[maxYears];
		for (int i=0;i<maxYears;i++)
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
		return savingsPerYearList;
	}
	
	/**
	 * Used to create a hashtable with breakeven information for services used to create a bar graph.
	
	 * @return Hashtable		Hashtable containing information about the balance over a time horizon. */
	public Hashtable createBreakevenGraph()
	{
		int thisYear = 2013+1;
		double[][] balanceList  = createBalanceList(thisYear);
		int[] totalYearsAxis = new int[maxYears];
		for (int i=0;i<maxYears;i++)
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
		double[][] balanceList  = new double[maxYears+1][2];
		return balanceList;
	}
	
	

	

}
