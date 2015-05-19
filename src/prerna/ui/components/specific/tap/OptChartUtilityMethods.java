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

/**
 * This class is used to optimize graph functions used in services calculations.
 */
public final class OptChartUtilityMethods {

	
	/**
	 * Stores information about the learning curve, including work efficiency against learning curve.
	
		nextYear = 2014
	 * @return Hashtable 	Hashtable with information about the learning curve. */
	public static Hashtable createLearningCurve(int nextYear, double iniLC, double scdLT, double scdLC, double[] learningConstants)
	{
		double[][] data= createLearningCurvePoints(nextYear, iniLC, scdLT, scdLC, learningConstants);
		double[][] data2 = new double[learningConstants.length][2];
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
	private static double[][] createLearningCurvePoints(int nextYear, double iniLC, double scdLT, double scdLC, double[] learningConstants) {
		//learning curve differential equation f(t) = Pmax-C*e^(-k*t)
		//after you solve for differential equation to get constants
		//here are the equations for the constants
		double cnstC = 1.0-iniLC;
		double cnstK = (1.0/scdLT)*Math.log((1.0-iniLC)/(1.0-scdLC));
		double[][] learningCurve = new double[learningConstants.length*10][2];
		for (int i = 0; i<learningConstants.length*10;i++)
		{
			double x =nextYear+((double) i)/10;
			learningCurve[i][1]=1.0-cnstC*Math.exp(-(((double) i)/10)*cnstK);
			learningCurve[i][0]=x;
		}
		return learningCurve;
		
	}
	

}
