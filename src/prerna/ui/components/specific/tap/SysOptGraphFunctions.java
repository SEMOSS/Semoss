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

import prerna.algorithm.impl.specific.tap.UnivariateSysOptimizer;

/**
 * This class is used to optimize graph functions used in system optimization calculations.
 */
public class SysOptGraphFunctions extends OptGraphFunctions{

	protected UnivariateSysOptimizer opt = null;
	/**
	 * Sets the Systems Optimizer and other constants that will be used.
	 * @param opt SysNetSavingsOptimizer
	 */
	public void setOptimzer (UnivariateSysOptimizer opt)
	{
		this.opt=opt;
		this.iniLC=opt.iniLC;
		this.scdLT = opt.scdLT;
		this.scdLC = opt.scdLC;
		this.learningConstants = opt.f.learningConstants;
		this.maxYears = opt.maxYears;
	}
	/**
	 * Sets the Systems Optimizer and other constants that will be used.
	 * @param opt SysNetSavingsOptimizer
	 */
	public void setOptimzer (UnivariateSysOptimizer opt, double[] learningConstants)
	{
		this.opt=opt;
		this.iniLC=opt.iniLC;
		this.scdLT = opt.scdLT;
		this.scdLC = opt.scdLC;
		this.learningConstants = learningConstants;
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
		//barChartHash.put("stack", "normal");
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
		
		if(susPerYearList.size()>2) {
			double[] workDoneSeries = susPerYearList.get(2);
			seriesHash.put("Work Performed", workDoneSeries);
			colorHash.put("Work Performed", "#4592B7");
		}
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
		
	}
	
	public ArrayList<double[]> createSusPerYear()
	{
		ArrayList<double[]> susPerYearList = new ArrayList<double[]>();
		
		double[] buildCost = new double[maxYears];
		for(int i=0;i<maxYears;i++)
			buildCost[i] = opt.installCostList.get(i);
		
		double[] sustainCost = new double[maxYears];
		for(int i=0;i<maxYears;i++)
			sustainCost[i] = opt.sustainCostList.get(i);

		susPerYearList.add(buildCost);
		susPerYearList.add(sustainCost);	
		
		if(opt.workDoneList!=null) {
			double[] workDoneList = new double[maxYears];
			for(int i=0;i<maxYears;i++)
				workDoneList[i] = opt.workDoneList.get(i);
			susPerYearList.add(workDoneList);
		}
		return susPerYearList;
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
	public ArrayList<double[]> createSavingsPerYearList()
	{
		ArrayList<double[]> savingsPerYearList = new ArrayList<double[]>();
		for (int i=0 ;i< 1;i++)
		{
			double[] newYear = new double[maxYears];
			for (int j=i;j<maxYears;j++)
			{
				newYear[j]=opt.cumSavingsList.get(j);
			}
			savingsPerYearList.add(newYear);
		}
		return savingsPerYearList;
	}
	
	@Override
	public double[][] createBalanceList(int thisYear)
	{
		thisYear = 2014;
		double[][] balanceList  = new double[maxYears][2];
		for (int i=0; i<maxYears;i++)
		{	
			balanceList[i][0]=thisYear+i;
			balanceList[i][1]=opt.breakEvenList.get(i);
		}
		return balanceList;
	}
	

}
