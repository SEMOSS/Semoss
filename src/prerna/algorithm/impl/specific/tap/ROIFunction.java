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

		Hashtable returnHash = lin.runOpt(totalYrs, learningConstants, hourlyRate);
		ArrayList objList = (ArrayList) returnHash.get("objective");
		ArrayList budgetList = (ArrayList) returnHash.get("budget");
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
	public double getROI(ArrayList objList, ArrayList budgetList)
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
