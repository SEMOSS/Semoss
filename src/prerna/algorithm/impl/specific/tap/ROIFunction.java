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
package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * This class is used to calculate the ROI (return on investment).
 */
public class ROIFunction extends UnivariateSvcOptFunction{
	

	
	/**
	 * Given a budget, calculate the ROI.
	 * Gets the lists of potential yearly savings and yearly budgets and can write to the app console for each iteration.
	 * @param a 		Budget used in the service optimizer.
	
	 * @return double	Return on investment. */
	@Override
	public double value(double a) {
		count++;
		lin.setBudget(a);

		Hashtable<String,ArrayList<Double>> returnHash = lin.runOpt(totalYrs, learningConstants, hourlyRate);
		ArrayList<Double> objList = returnHash.get("objective");
		ArrayList<Double> budgetList = returnHash.get("budget");
		lin.resetServiceOptimizer();
		

		double ROI = getROI(objList, budgetList);
		
		if (write)
		{
			writeToAppConsole(objList, budgetList, ROI);
			updateProgressBar("Iteration " + count);
		}
		
		return ROI;
	}
	
	/**
	 * Gets the return on investment (ROI) for the current iteration.
	 * @param	objList		List of potential yearly savings for current iteration.
	 * @param 	budgetList	List of yearly budgets for current iteration.
	
	 * @return double		Return on investment (ROI). */
	public double getROI(ArrayList<Double> objList, ArrayList<Double> budgetList)
	{
		double mu = (1+infRate)/(1+disRate);
		double numerator=0;
		double denominator = 0;
		if(mu==1)
		{
			for (int i=0;i<objList.size();i++)
			{
				int q = i+1;
				double yearObj = (Double) objList.get(i);
				double yearBgt = (Double) budgetList.get(i);
				numerator = numerator + Math.pow(mu, q)*yearObj*(totalYrs - q);
				//denominator = denominator + yearBgt;
				denominator = denominator + Math.pow(mu, q)*yearBgt;
			}
		}
		else
		{
			for (int i=0;i<objList.size();i++)
			{
				int q = i+1;
				double yearObj = (Double) objList.get(i);
				double yearBgt = (Double) budgetList.get(i);
				
				//formula
				//should be mu^totalYrs -(i+1)+1  which ends up being just totalYrs-i
				numerator = numerator +Math.pow(mu, q+1)* yearObj*(1-Math.pow(mu, totalYrs-(q)));
				denominator = denominator + Math.pow(mu, q)*yearBgt;
				
			}
			denominator = denominator * (1-mu);
		}
		
		double ROI = numerator/denominator-1;
		
		if(Double.isNaN(ROI))
			return 0;
		
		return ROI;
	}
}
