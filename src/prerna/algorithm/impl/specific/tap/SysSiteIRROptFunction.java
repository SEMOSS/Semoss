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
 * Interface representing a univariate real function that is implemented for TAP system and service optimization functions.
 */
public class SysSiteIRROptFunction extends SysSiteOptFunction{
	
	SysSiteIRRLinInterp linInt;
	
	public void setVariables(int[][] systemDataMatrix, int[][] systemBLUMatrix, double[][] systemSiteMatrix, int[] systemTheater, int[] systemGarrison, Integer[] sysModArr, Integer[] sysDecomArr, double[] maintenaceCosts, double[] siteMaintenaceCosts, double[] siteDeploymentCosts, double[] systemCostConsumeDataArr, int[][] centralSystemDataMatrix,int[][] centralSystemBLUMatrix, int[] centralSystemTheater, int[] centralSystemGarrison, Integer[] centralModArr, Integer[] centralDecomArr, double[] centralSystemMaintenaceCosts, double budgetForYear, int years, double currentSustainmentCost, double infRate, double disRate, double trainingPerc) {
		
		super.setVariables(systemDataMatrix, systemBLUMatrix, systemSiteMatrix, systemTheater, systemGarrison, sysModArr, sysDecomArr, maintenaceCosts, siteMaintenaceCosts, siteDeploymentCosts, systemCostConsumeDataArr, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemTheater, centralSystemGarrison, centralModArr, centralDecomArr, centralSystemMaintenaceCosts,  budgetForYear, years, currentSustainmentCost, infRate, disRate, trainingPerc);

		createLinearInterpolation();
	}
	
	/**
	 * Given a budget, optimize the savings to return max savings.
	 * @return double	max savings possible when optimizing for budget*/
	public double value(double arg0) {
		
		runLPSolve(arg0);
		if(currentSustainmentCost == futureSustainmentCost) {
			irr =  -1E-30;
		}else {
			
			linInt.setYearSavingsAndYearsToComplete(currentSustainmentCost - futureSustainmentCost, yearsToComplete);
			linInt.execute();
			irr =  linInt.retVal * 100;
		}
		System.out.println("iteration " + count + ": budget entered " + arg0 + ", actual cost to deploy " + adjustedDeploymentCost + ", years to deploy " + yearsToComplete + ", internal rate of return "+irr + "%");
		
		return irr;
		
	}

	public void createLinearInterpolation()
	{
		 linInt = new SysSiteIRRLinInterp();
		 linInt.setMinAndMax(-99, 10000000);
		 linInt.setVariables(budgetForYear, totalYrs, infRate);
	}

}
