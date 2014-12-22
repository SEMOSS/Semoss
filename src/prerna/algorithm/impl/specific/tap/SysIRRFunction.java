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


/**
 * This class is used to calculate the number of years to decommission systems based on the budget.
 */
public class SysIRRFunction extends UnivariateSysOptFunction{
	
	SysIRRLinInterp linInt;
//	SysOptPlaySheet playsheet;
	
//	public void setPlaySheet(SysOptPlaySheet playsheet)
//	{
//		this.playsheet = playsheet;
//		linInt.setPlaySheet(playsheet);
//	}
	@Override
	public double calculateRet(double budget, double n)
	{
		//if it takes the full time, there is no IRR, just return 0?
//		if(totalYrs == n)
//			return 0;
		if(n>totalYrs)
			return -1.0E30;
		linInt.setBAndN(budget, n);
		linInt.execute();
		return linInt.retVal;
	}
	
	public double calculateRet(double budget,double n, double savings)
	{
		if(n>totalYrs)
			return -1.0E30;
		linInt.setBAndN(budget, n);
	//	linInt.setSavings(savings);
		linInt.execute();
	//	linInt.setSavings(0.0);
		return linInt.retVal;
	}
	
	@Override
	public double calculateYears(double budget)
	{
		workPerformedArray = new ArrayList<Double>();
		double workNeededAdj = 0.0;
		while(workNeededAdj<dataExposeCost)
		{
			if(workPerformedArray.size()>totalYrs+5)
				return -1;
			//year 1, q=1 in index 0.
			double workPerformedInYearq = calculateWorkPerformedInYearq(budget, workPerformedArray.size()+1);
			workNeededAdj+=workPerformedInYearq;
			workPerformedArray.add(workPerformedInYearq);
		}
		double workPerformedInLastYear = workPerformedArray.get(workPerformedArray.size()-1);
		double fraction = (dataExposeCost - (workNeededAdj - workPerformedInLastYear))/workPerformedInLastYear;
		return workPerformedArray.size()-1+fraction;
	}
	@Override
	public void writeToAppConsole(double budget, double n, double savings)
	{
		consoleArea.setText(consoleArea.getText()+"\nPerforming optimization iteration "+count);
		consoleArea.setText(consoleArea.getText()+"\nFor Budget B: "+budget+" the minimum N is "+n+" and the discount rate is "+savings);
	}

	//data Expose is in LOE
	public void createLinearInterpolation()
	{
		 linInt = new SysIRRLinInterp();
		 linInt.setMinAndMax(-.95, 5);
		 linInt.setCalcParams(numMaintenanceSavings,serMainPerc, dataExposeCost,totalYrs,infRate,disRate);
		// linInt.setValues(numMaintenanceSavings,serMainPerc, dataExposeCost,totalYrs,infRate,disRate, -10, 10);
	}
}
