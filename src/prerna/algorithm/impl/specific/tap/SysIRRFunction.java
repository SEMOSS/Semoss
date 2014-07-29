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
