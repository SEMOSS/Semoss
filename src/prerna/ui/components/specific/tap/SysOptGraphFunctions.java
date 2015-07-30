/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import prerna.algorithm.impl.specific.tap.UnivariateSysOptimizer;

/**
 * This class is used to optimize graph functions used in system optimization calculations.
 */
public class SysOptGraphFunctions{

	private UnivariateSysOptimizer opt = null;
	private int maxYears;
	/**
	 * Sets the Systems Optimizer and other constants that will be used.
	 * @param opt SysNetSavingsOptimizer
	 */
	public void setOptimizer (UnivariateSysOptimizer opt)
	{
		this.opt=opt;
		this.maxYears = opt.maxYears;
	}
	
	public Hashtable createModernizedHeatMap()
	{
		ArrayList<String> sysList = opt.sysOpt.sysList;
		double[] modernizedList = opt.sysOpt.systemIsModernized;
		Hashtable dataHash = new Hashtable();
		String xName = "System";
		String yName = "Modernized";
		for (int i=0;i<sysList.size();i++)
		{
			Hashtable elementHash = new Hashtable();
			String sysName = sysList.get(i);	
			double modernizedVal = modernizedList[i];
			sysName = sysName.replaceAll("\"", "");
			String key = sysName +"-"+yName;
			if(modernizedVal>0)
				modernizedVal = 1.0;
			else
				modernizedVal = 0.0;
			elementHash.put(xName, sysName);
			elementHash.put(yName, yName);
			elementHash.put("value", modernizedVal);
			dataHash.put(key, elementHash);
			
		}

		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		allHash.put("title", "Modernized Systems"); //var1[0] + " vs " + var1[1]);
		allHash.put("xAxisTitle", xName);
		allHash.put("yAxisTitle", yName);
		allHash.put("value", "value");
		allHash.put("hideHeader","true");
		
		return allHash;
	}
	
	public Hashtable createCostChart()
	{
		int thisYear = 2014;
		double[] buildCost = new double[maxYears];
		double[] sustainCost = new double[maxYears];
		int[] totalYearsAxis = new int[maxYears];
		for(int i=0;i<maxYears;i++) {
			buildCost[i] = opt.installCostList.get(i);
			sustainCost[i] = opt.sustainCostList.get(i);
			totalYearsAxis[i]=thisYear+i;
		}

		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
//		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		//barChartHash.put("stack", "normal");
		barChartHash.put("title",  "Build and Sustainment Cost ");
		barChartHash.put("yAxisTitle", "Learning-Adjusted Cost (Actual-time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		barChartHash.put("xAxis", totalYearsAxis);
		seriesHash.put("Build Costs", buildCost);
		seriesHash.put("Sustainment Costs", sustainCost);
//		colorHash.put("Build Costs", "#4572A7");
//		colorHash.put("Sustainment Costs", "#80699B");
		
		if(opt.workDoneList!=null) {
			double[] workDoneList = new double[maxYears];
			for(int i=0;i<maxYears;i++)
				workDoneList[i] = opt.workDoneList.get(i);
			seriesHash.put("Work Performed", workDoneList);
//			colorHash.put("Work Performed", "#4592B7");
		}
		barChartHash.put("dataSeries",  seriesHash);
//		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	
	public Hashtable createCumulativeSavings()
	{
		int thisYear = 2013+1;

		int[] totalYearsAxis = new int[maxYears];
		double[] yearSeries = new double[maxYears];
		for (int i=0;i<maxYears;i++) {
			yearSeries[i]=opt.cumSavingsList.get(i);
			totalYearsAxis[i]=thisYear+i+1;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
//		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("stack", "normal");
		barChartHash.put("title",  "Accumulative Sustainment Savings");
		barChartHash.put("yAxisTitle", "Savings (Actual-time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		barChartHash.put("xAxis", totalYearsAxis);

		seriesHash.put("Savings", yearSeries);
	
		barChartHash.put("dataSeries",  seriesHash);
//		barChartHash.put("colorSeries", colorHash);
		
		return barChartHash;
	}

	/**
	 * Used to create a hashtable with breakeven information for services used to create a bar graph.
	
	 * @return Hashtable		Hashtable containing information about the balance over a time horizon. */
	public Hashtable createBreakevenGraph()
	{
		int thisYear = 2013+1;
		double[][] balanceList  = new double[maxYears][2];
		int[] totalYearsAxis = new int[maxYears];
		for (int i=0; i<maxYears;i++)
		{	
			balanceList[i][0]=thisYear+i;
			balanceList[i][1]=opt.breakEvenList.get(i);
			totalYearsAxis[i]=thisYear+i+1;
		}
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
//		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "line");
		barChartHash.put("title",  "Balance Over Time Horizon");
		barChartHash.put("yAxisTitle", "Balance(Actual time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		//barChartHash.put("xAxis", totalYearsAxis);
		seriesHash.put("Balance Line", balanceList);
//		colorHash.put("Balance Line", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
//		barChartHash.put("colorSeries", colorHash);
		barChartHash.put("xAxisInterval", 1);
		return barChartHash;
	}


}
