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

		Hashtable<String,ArrayList<Double>> returnHash = lin.runOpt(totalYrs, learningConstants, hourlyRate);
		ArrayList<Double> objList = returnHash.get("objective");
		ArrayList<Double> budgetList = returnHash.get("budget");
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
	public double getBK(ArrayList<Double> objList, ArrayList<Double> budgetList)
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
