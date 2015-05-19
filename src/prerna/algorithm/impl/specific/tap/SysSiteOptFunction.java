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
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.SysSiteOptPlaySheet;



/**
 * Interface representing a univariate real function that is implemented for TAP system and service optimization functions.
 */
public class SysSiteOptFunction extends UnivariateOptFunction{
	
	protected int count = 0;
	protected SysSiteLPSolver lpSolver;
	
	protected SysSiteOptPlaySheet playSheet;
	
	//given
	protected double currentSustainmentCost;
	protected double budgetForYear;
	
	//calculated during run
	protected double futureSustainmentCost;
	protected double adjustedDeploymentCost;
	protected double yearsToComplete;
	
	//outputs of extended classes
	protected double adjustedTotalSavings;
	protected double roi;
	protected double irr;
	
	public SysSiteOptFunction() {
		lpSolver = new SysSiteLPSolver();
	}
	
	public void setVariables(int[][] localSystemDataMatrix, int[][] localSystemBLUMatrix, int[] localSystemIsTheaterArr, int[] localSystemIsGarrisonArr, Integer[] localSystemIsModArr, Integer[] localSystemIsDecomArr, double[] localSystemMaintenanceCostArr, double[] localSystemSiteMaintenaceCostArr, double[] localSystemSiteDeploymentCostArr, double[] localSystemSiteInterfaceCostArr, double[][] localSystemSiteMatrix, int[][] centralSystemDataMatrix, int[][] centralSystemBLUMatrix, int[] centralSystemIsTheaterArr, int[] centralSystemIsGarrisonArr, Integer[] centralSystemIsModArr, Integer[] centralSystemIsDecomArr, double[] centralSystemMaintenanceCostArr, double[] centralSystemInterfaceCostArr, double trainingPerc, double currentSustainmentCost, double budgetForYear, int years, double infRate, double disRate) {

		this.currentSustainmentCost = currentSustainmentCost;
		this.budgetForYear = budgetForYear;
		this.totalYrs = years;
		
		this.infRate = infRate;
		this.disRate = disRate;

		lpSolver.setVariables(localSystemDataMatrix, localSystemBLUMatrix, localSystemIsTheaterArr, localSystemIsGarrisonArr, localSystemIsModArr, localSystemIsDecomArr, localSystemMaintenanceCostArr, localSystemSiteMaintenaceCostArr, localSystemSiteDeploymentCostArr, localSystemSiteInterfaceCostArr, localSystemSiteMatrix, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemIsTheaterArr, centralSystemIsGarrisonArr, centralSystemIsModArr, centralSystemIsDecomArr, centralSystemMaintenanceCostArr, centralSystemInterfaceCostArr, trainingPerc, currentSustainmentCost);
	}
	
	protected void runLPSolve(double budget) throws LpSolveException {
		count++;
		printMessage("budget "+budget);
		
		long startTime;
		long endTime;

		try{
			startTime = System.currentTimeMillis();
			lpSolver.setMaxBudget(budget);
			lpSolver.setupModel();
	
			endTime = System.currentTimeMillis();
			printMessage("Time to set up for iteration " + count + ": " + (endTime - startTime) / 1000 );
	
			startTime = System.currentTimeMillis();
			lpSolver.execute();
			
			endTime = System.currentTimeMillis();
			printMessage("Time to run iteration " + count + ": " + (endTime - startTime) / 1000 );
	
			lpSolver.deleteModel();
			
			futureSustainmentCost = lpSolver.getObjectiveVal();
			
			if(budgetForYear == 0)
				yearsToComplete = 0.0;
			else
				yearsToComplete = lpSolver.getTotalDeploymentCost() / budgetForYear;
			
			double mu = (1 + infRate) / (1 + disRate);
			adjustedDeploymentCost = SysOptUtilityMethods.calculateAdjustedDeploymentCost(mu, yearsToComplete, budgetForYear);
			adjustedTotalSavings = SysOptUtilityMethods.calculateAdjustedTotalSavings(mu, yearsToComplete, totalYrs, currentSustainmentCost - futureSustainmentCost);

			if(adjustedDeploymentCost == 0)
				roi = 0;
			else
				roi = (adjustedTotalSavings / adjustedDeploymentCost - 1);
			

		
		} catch(LpSolveException e) {
			lpSolver.deleteModel();
			futureSustainmentCost = 0.0;
			yearsToComplete = 0.0;
			adjustedDeploymentCost = 0.0;
			adjustedTotalSavings = 0.0;
			roi = 0;
			throw new LpSolveException(e.getMessage());
		}

	}
	
	protected void printMessage(String message) {
		if(playSheet == null)
			System.out.println(message);
		else
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n" + message);
	}
	
	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (SysSiteOptPlaySheet) playSheet;
		
	}
	
	public double[] getSysKeptArr() {
		return lpSolver.getLocalSysKeptArr();
	}

	public double[] getCentralSysKeptArr() {
		return lpSolver.getCentralSysKeptArr();
	}
	
	public double[][] getSystemSiteResultMatrix() {
		return lpSolver.getLocalSystemSiteResultMatrix();
	}

	public double getFutureSustainmentCost() {
		return futureSustainmentCost;
	}
	
	public double getAdjustedDeploymentCost() {
		return adjustedDeploymentCost;
	}
	
	public double getYearsToComplete() {
		return yearsToComplete;
	}
	
	public double getAdjustedTotalSavings() {
		return adjustedTotalSavings;
	}
	
	public double getROI() {
		return roi;
	}
	
	public double getIRR() {
		return irr;
	}

}
