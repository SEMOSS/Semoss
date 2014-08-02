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
 * This class is used to calculate the breakeven point of a service.
 */
public class RecoupFunction extends UnivariateSvcOptFunction{
	

	/**
	 * Given a specific budget, calculate the breakeven point.
	 * Gets the list of potential yearly savings and yearly budgets.
	 * @param a 	Budget used in the service optimizer.
	
	 * @return double	Breakeven. */
	@Override
	public double value(double a) {
		count++;
		lin.setBudget(a);

		Hashtable returnHash = lin.runOpt(totalYrs, learningConstants, hourlyRate);
		ArrayList objList = (ArrayList) returnHash.get("objective");
		ArrayList budgetList = (ArrayList) returnHash.get("budget");
		lin.resetServiceOptimizer();
		
		
		//everything is in hours so I can divide to get N
		double breakeven = getBK(objList, budgetList);
		if (write)
		{
			writeToAppConsole(objList, budgetList, breakeven);
			updateProgressBar("Iteration " + count);
		}
		
		return breakeven;
	}
	
	/**
	 * Gets the breakeven point.
	 * @param	objList		List of potential yearly savings for current iteration.
	 * @param 	budgetList	List of yearly budgets for current iteration.
	
	 * @return double 		Breakeven point.*/
	public double getBK(ArrayList objList, ArrayList budgetList)
	{
		double mu = (1+infRate)/(1+disRate);
		double numerator=0;
		double denominator = 0;
		if(mu==1)
		{
			for (int i=0;i<objList.size();i++)
			{
				int year = i+1;
				double yearObj = (Double) objList.get(i);
				double yearBgt = (Double) budgetList.get(i);
				numerator = numerator + yearObj*(year) + yearBgt;
				denominator = denominator + yearObj;
			}
			double breakeven = numerator/denominator;
			return breakeven;
		}
		else
		{
			for (int i=0;i<objList.size();i++)
			{
				double yearObj = (Double) objList.get(i);
				double yearBgt = (Double) budgetList.get(i);
				
				//formula
				numerator = numerator +Math.pow(mu, i+1)* (mu*yearObj-(1-mu)*yearBgt);
				denominator = denominator +  yearObj;
			}
		}
		double breakeven = 1/Math.log(mu)*Math.log(numerator/denominator)-1;
		if(Double.isNaN(breakeven))
			return Double.POSITIVE_INFINITY;
		
		return breakeven;
	}
	
	
	
}
