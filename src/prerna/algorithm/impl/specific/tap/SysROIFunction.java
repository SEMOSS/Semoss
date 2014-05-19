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


/**
 * This class is used to calculate the number of years to decommission systems based on the budget.
 */
public class SysROIFunction extends SysNetSavingsFunction{
	
	@Override
	public double calculateRet(double budget, double n)
	{
		double P1InflationSum = 0.0;
//		if(n<1)
//		{
//			double P1Inflation = 1.0;
//			P1Inflation *= calculateP1q(n+1);
//			P1InflationSum += P1Inflation;
//		}
		for(int q=1;q<=n;q++)
		{
			double P1Inflation = 1.0;
			if(inflDiscFactor!=1)
				P1Inflation = Math.pow(inflDiscFactor, q-1);
			P1Inflation *= calculateP1q(q);
			P1InflationSum += P1Inflation;
		}
		
		double P1Inflation = 1.0;
		if(inflDiscFactor!=1)
			P1Inflation = Math.pow(inflDiscFactor, n);
		P1Inflation *= calculateP1qFraction(n);
		P1InflationSum += P1Inflation;
		
		investment = budget * P1InflationSum;
		//if it takes the full time, there is no savings, just return the investment?
		if(totalYrs == n)
			return -1*investment;
		//make the savings inflation/discount factor if applicable
		double roi =totalYrs-n;
		if(inflDiscFactor!=1)
			roi = Math.pow(inflDiscFactor,n+1) * (1-Math.pow(inflDiscFactor,totalYrs-n) ) / (1-inflDiscFactor);
		//multiply the savings for all years
		roi = roi * (numMaintenanceSavings - serMainPerc*investment);
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
