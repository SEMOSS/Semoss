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
