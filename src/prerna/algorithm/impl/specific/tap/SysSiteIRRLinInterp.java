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

import prerna.algorithm.impl.LinearInterpolation;


/**
 * SysSiteLinInterp is used to estimate the IRR (mu) for a given discount rate using Linear Interpolation.
 */
public class SysSiteIRRLinInterp extends LinearInterpolation{

	private double budgetForYear, totalYrs, infRate;
	private double savingsForYear, yearsToComplete;


	/**
	 * Sets the parameters used in the equation.
	 * @param yearlySavings
	 * @param maxYearlyBudget
	 * @param yearsToComplete
	 * @param totalYrs
	 * @param infRate
	 */
	public void setVariables(double budgetForYear, double totalYrs, double infRate) {

		this.budgetForYear = budgetForYear;
		this.totalYrs = totalYrs;
		this.infRate = infRate;
	}
	
	public void setYearSavingsAndYearsToComplete(double savingsForYear, double yearsToComplete) {
		this.savingsForYear = savingsForYear;
		this.yearsToComplete = yearsToComplete;
	}
	
	@Override
	public void execute() {
		super.execute();
		if(retVal == -1.0E30) {
			retVal = Double.POSITIVE_INFINITY;
		}
	}

	/**
	 * Calculate the residual value for a given root estimate, possibleDiscRate.
	 * @param possibleDiscRate double	root estimate	
	 * @return Double	residual value for the calculation
	 */
	@Override
	public Double calcY(double possibleDiscRate) {
		double possibleMu = (1 + infRate / 100) / (1 + possibleDiscRate / 100);

		double adjustedTotalSavings = SysOptUtilityMethods.calculateAdjustedTotalSavings(possibleMu, yearsToComplete, totalYrs, savingsForYear);
		double adjustedDeploymentCost = SysOptUtilityMethods.calculateAdjustedDeploymentCost(possibleMu, yearsToComplete, budgetForYear);

		return adjustedTotalSavings - adjustedDeploymentCost;

	}
}
