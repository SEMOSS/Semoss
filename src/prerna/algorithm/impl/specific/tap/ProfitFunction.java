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
 * This class is used to calculate the profitability of a service and is used in TAP services optimization.
 */
public class ProfitFunction extends UnivariateSvcOptFunction{
	
	
	/**
	 * Given a specific budget, calculate the profit.
	 * Gets the list of potential yearly savings and yearly budgets.
	 * @param a 	Budget used in the service optimizer.
	 * @return 		Profit. */
	@Override
	public double value(double a) {
		count++;
		double profit=0;
		lin.setBudget(a);

		Hashtable<String,ArrayList<Double>> returnHash = lin.runOpt(totalYrs, learningConstants, hourlyRate);
		ArrayList<Double> objList = returnHash.get("objective");
		ArrayList<Double> budgetList = returnHash.get("budget");
		lin.resetServiceOptimizer();
		
		profit = getProfit(objList, budgetList);
		if (write)
		{
			writeToAppConsole(objList, budgetList, profit);
			updateProgressBar("Iteration " + count);
		}
		
		return profit;
	}
	
	/**
	 * Calculates the profit based on the current iteration.
	 * @param	objList		List of potential yearly savings for current iteration.
	 * @param 	budgetList	List of yearly budgets for current iteration.
	
	 * @return double	Profit. */
	public double getProfit(ArrayList<Double> objList, ArrayList<Double> budgetList)
	{
		double profit = 0;
		double mu = (1+infRate)/(1+disRate);
		if(mu==1)
		{
			for (int i=0;i<objList.size();i++)
			{
				int year = i+1;
				double yearObj = (Double) objList.get(i);
				double yearBgt = (Double) budgetList.get(i);
				profit = profit + yearObj*(totalYrs - year) - yearBgt;
			}
		}
		else
		{
			for (int i=0;i<objList.size();i++)
			{
				double yearObj = (Double) objList.get(i);
				double yearBgt = (Double) budgetList.get(i);
				
				//formula
				profit = profit +Math.pow(mu, i+1)* (yearObj*(mu-Math.pow(mu, totalYrs-i))/(1-mu)-yearBgt);
			}
		}
		
		
		
		return profit;
	}
	
	
}
