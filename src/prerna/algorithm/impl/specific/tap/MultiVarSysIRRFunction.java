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
public class MultiVarSysIRRFunction extends MultivariateOptFunction{
	
	MultiVarSysIRRLinInterp linInt;

	@Override
	public double calculateRet(double[] budget, double n)
	{
		if(n>totalYrs)
			return -1.0E30;
		linInt.setBAndN(budget, n);
		linInt.execute();
		return linInt.retVal;
	}
//	
//	public double calculateRet(double[] budget,double n, double savings)
//	{
//		if(n>totalYrs)
//			return -1.0E30;
//		linInt.setBAndN(budget, n);
//		linInt.execute();
//		return linInt.retVal;
//	}
	
	@Override
	public void writeToAppConsole(double[] budget, double n, double savings)
	{
		consoleArea.setText(consoleArea.getText()+"\nPerforming optimization iteration "+count);
		consoleArea.setText(consoleArea.getText()+"\nFor Budget B: ");
		for(int i=0;i<budget.length;i++)
			consoleArea.setText(consoleArea.getText()+budget[i]+", ");
		consoleArea.setText(consoleArea.getText()+"the minimum N is "+n+" and the discount rate is "+savings);
	}

	//data Expose is in LOE
	public void createLinearInterpolation()
	{
		 linInt = new MultiVarSysIRRLinInterp();
		 linInt.setMinAndMax(-.95, 2);
		 linInt.setCalcParams(numMaintenanceSavings,serMainPerc, dataExposeCost,totalYrs,infRate,disRate);
		// linInt.setValues(numMaintenanceSavings,serMainPerc, dataExposeCost,totalYrs,infRate,disRate, -10, 10);
	}
}
