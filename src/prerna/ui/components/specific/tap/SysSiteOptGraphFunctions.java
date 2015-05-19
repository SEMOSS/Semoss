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

import java.util.Hashtable;

import prerna.algorithm.impl.specific.tap.SysSiteOptimizer;

/**
 * This class is used to optimize graph functions used in system optimization calculations.
 */
public class SysSiteOptGraphFunctions{

	private SysSiteOptimizer opt = null;
	private int maxYears;
	private int startYear;

	/**
	 * Sets the Systems Optimizer and other constants that will be used.
	 * @param opt SysNetSavingsOptimizer
	 */
	public void setOptimzer(SysSiteOptimizer opt)
	{
		this.opt=opt;
		this.maxYears = opt.maxYears;
		this.startYear = opt.startYear;
	}
	
	public Hashtable createCostChart()
	{
		int[] totalYearsAxis = new int[maxYears];
		for (int i=0;i<maxYears;i++) {
			totalYearsAxis[i]=startYear+i;
		}

		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("title",  "Build and Sustainment Cost ");
		barChartHash.put("yAxisTitle", "Cost (Actual-time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		barChartHash.put("xAxis", totalYearsAxis);
		seriesHash.put("Build Costs", opt.deployCostPerYearArr);
		seriesHash.put("Current Sustainment Costs", opt.currCostPerYearArr);
		seriesHash.put("Future Sustainment Costs", opt.futureCostPerYearArr);
		colorHash.put("Build Costs", "#4572A7");
		colorHash.put("Sustainment Costs", "#80699B");
		
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	
	/**
	 * Used to create a hashtable that stores information about cumulative savings used in a bar graph.
	
	 * @return Hashtable 	Contains information about cumulative sustainment savings. */
	public Hashtable createCumulativeSavings()
	{
		int[] totalYearsAxis = new int[maxYears];
		for (int i=0;i<maxYears;i++) {
			totalYearsAxis[i]=startYear+i;
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
		seriesHash.put("Sustainment Savings", opt.cummCostAvoidedArr);
		
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
	}

	/**
	 * Used to create a hashtable with breakeven information for services used to create a bar graph.
	
	 * @return Hashtable		Hashtable containing information about the balance over a time horizon. */
	public Hashtable createBreakevenGraph()
	{
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "line");
		barChartHash.put("title",  "Balance Over Time Horizon");
		barChartHash.put("yAxisTitle", "Balance (Actual-time value)");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		seriesHash.put("Balance Line", opt.balanceArr);
		colorHash.put("Balance Line", "#4572A7");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		barChartHash.put("xAxisInterval", 1);
		return barChartHash;
	}

}
