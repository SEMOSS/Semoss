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


/**
 * This class is used to calculate the number of years to decommission systems based on the budget.
 */
public class SysROIFunction extends UnivariateSysOptFunction{
	
	@Override
	public double calculateRet(double budget, double n)
	{
		calculateInvestment(budget,n);
		//if it takes the full time, there is no savings, just return the investment?
		if(totalYrs == n)
			return -1*investment;
		//make the savings inflation/discount factor if applicable
		double roi =totalYrs-n;
		if(inflDiscFactor!=1)
			roi = Math.pow(inflDiscFactor,n+1) * (1-Math.pow(inflDiscFactor,totalYrs-n) ) / (1-inflDiscFactor);
		//multiply the savings for all years
		roi = roi * (numMaintenanceSavings - serMainPerc*dataExposeCost);
		roi = roi - investment;
		roi = roi / investment;
		return roi;
	}
	
	@Override
	public void writeToAppConsole(double budget, double n, double savings)
	{
		consoleArea.setText(consoleArea.getText()+"\nPerforming optimization iteration "+count);
		consoleArea.setText(consoleArea.getText()+"\nFor Budget B: "+budget+" the minimum N is "+n+" and the ROI is "+savings);
	}

}
