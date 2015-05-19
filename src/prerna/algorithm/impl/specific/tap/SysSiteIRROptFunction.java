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

import lpsolve.LpSolveException;



/**
 * Interface representing a univariate real function that is implemented for TAP system and service optimization functions.
 */
public class SysSiteIRROptFunction extends SysSiteOptFunction{
	
	SysSiteIRRLinInterp linInt;
	
	public void setVariables(int[][] localSystemDataMatrix, int[][] localSystemBLUMatrix, int[] localSystemIsTheaterArr, int[] localSystemIsGarrisonArr, Integer[] localSystemIsModArr, Integer[] localSystemIsDecomArr, double[] localSystemMaintenanceCostArr, double[] localSystemSiteMaintenaceCostArr, double[] localSystemSiteDeploymentCostArr, double[] localSystemSiteInterfaceCostArr, double[][] localSystemSiteMatrix, int[][] centralSystemDataMatrix, int[][] centralSystemBLUMatrix, int[] centralSystemIsTheaterArr, int[] centralSystemIsGarrisonArr, Integer[] centralSystemIsModArr, Integer[] centralSystemIsDecomArr, double[] centralSystemMaintenanceCostArr, double[] centralSystemInterfaceCostArr, double trainingPerc, double currentSustainmentCost, double budgetForYear, int years, double infRate, double disRate) {
		
		super.setVariables(localSystemDataMatrix, localSystemBLUMatrix, localSystemIsTheaterArr, localSystemIsGarrisonArr, localSystemIsModArr, localSystemIsDecomArr, localSystemMaintenanceCostArr, localSystemSiteMaintenaceCostArr, localSystemSiteDeploymentCostArr, localSystemSiteInterfaceCostArr, localSystemSiteMatrix, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemIsTheaterArr, centralSystemIsGarrisonArr, centralSystemIsModArr, centralSystemIsDecomArr, centralSystemMaintenanceCostArr, centralSystemInterfaceCostArr, trainingPerc, currentSustainmentCost, budgetForYear, years, infRate, disRate);

		createLinearInterpolation();
	}
	
	/**
	 * Given a budget, optimize the savings to return max savings.
	 * @return double	max savings possible when optimizing for budget*/
	public double value(double arg0) {
		try{
			runLPSolve(arg0);
			if(adjustedTotalSavings <= 0.0) {
				irr = -1E-40;
				printMessage("iteration " + count + ": budget entered " + arg0 + ", actual cost to deploy " + adjustedDeploymentCost + ", years to deploy " + yearsToComplete + ", internal rate of return " + "does not exist since no savings");
			}else {
				
				linInt.setYearSavingsAndYearsToComplete(currentSustainmentCost - futureSustainmentCost, yearsToComplete);
				linInt.execute();
				irr =  linInt.retVal;
				if(irr == -1E-30 || irr == -1E-31) {
					printMessage("iteration " + count + ": budget entered " + arg0 + ", actual cost to deploy " + adjustedDeploymentCost + ", years to deploy " + yearsToComplete + ", internal rate of return "+ "has problem with calculation");
				}else {
					printMessage("iteration " + count + ": budget entered " + arg0 + ", actual cost to deploy " + adjustedDeploymentCost + ", years to deploy " + yearsToComplete + ", internal rate of return "+(irr*100) + "%");
				}
			}
			
		}catch(LpSolveException e) {
			irr = -1E-40;
			printMessage("Error solving iteration " + count);
		}
		return irr;
		
	}

	public void createLinearInterpolation()
	{
		 linInt = new SysSiteIRRLinInterp();
		 linInt.setMinAndMax(0, 10000000);
		 linInt.setVariables(budgetForYear, totalYrs, infRate);
	}

}
