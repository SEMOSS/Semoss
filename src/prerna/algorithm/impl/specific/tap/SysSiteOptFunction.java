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
public class SysSiteOptFunction extends UnivariateOptFunction{
	
	protected int count = 0;
	protected SysSiteLPSolver lpSolver;
	
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
	
	
	public void setVariables(int[][] systemDataMatrix, int[][] systemBLUMatrix, double[][] systemSiteMatrix, int[] systemTheater, int[] systemGarrison, Integer[] sysModArr, Integer[] sysDecomArr, double[] maintenaceCosts, double[] siteMaintenaceCosts, double[] siteDeploymentCosts,int[][] centralSystemDataMatrix,int[][] centralSystemBLUMatrix, int[] centralSystemTheater, int[] centralSystemGarrison, Integer[] centralModArr, Integer[] centralDecomArr, double[] centralSystemMaintenaceCosts, double budgetForYear, int years, double currentSustainmentCost, double infRate, double disRate) {
		
		this.budgetForYear = budgetForYear;
		this.totalYrs = years;
		this.currentSustainmentCost = currentSustainmentCost;
		
		this.infRate = infRate;
		this.disRate = disRate;
		
		long startTime;
		long endTime;
		startTime = System.currentTimeMillis();			

		lpSolver = new SysSiteLPSolver(systemDataMatrix, systemBLUMatrix, systemSiteMatrix, systemTheater, systemGarrison, sysModArr, sysDecomArr, maintenaceCosts, siteMaintenaceCosts, siteDeploymentCosts, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemTheater, centralSystemGarrison, centralModArr, centralDecomArr, centralSystemMaintenaceCosts, budgetForYear * years / 2);
		lpSolver.setupModel();
		
		endTime = System.currentTimeMillis();
		System.out.println("Time to set up the LP solver " + (endTime - startTime) / 1000  + " seconds");

	}
	
	protected Boolean runLPSolve(double budget) {
		count++;
//		updateProgressBar("Iteration " + count);
		System.out.println("budget "+budget);
		
		long startTime;
		long endTime;
		startTime = System.currentTimeMillis();
		
		lpSolver.updateBudget(budget);
		lpSolver.setTimeOut(200);
		lpSolver.execute();
		
		endTime = System.currentTimeMillis();
		System.out.println("Time to run iteration " + count + ": " + (endTime - startTime) / 1000 );
		
		if(!lpSolver.isOptimalSolution()) {
			return false;
		}
		
		futureSustainmentCost = lpSolver.getObjectiveVal();
		double deploymentCost = lpSolver.getTotalDeploymentCost();
		
		if(budgetForYear == 0)
			yearsToComplete = 0.0;
		else
			yearsToComplete = deploymentCost / budgetForYear;
		
		double mu = (1 + infRate / 100) / (1 + disRate / 100);
		adjustedDeploymentCost = SysOptUtilityMethods.calculateAdjustedDeploymentCost(mu, yearsToComplete, budgetForYear);
		adjustedTotalSavings = SysOptUtilityMethods.calculateAdjustedTotalSavings(mu, yearsToComplete, totalYrs, currentSustainmentCost - futureSustainmentCost);

				
		return true;
	}
	
	public double[] getSysKeptArr() {
		return lpSolver.getSysKeptArr();
	}

	public double[] getCentralSysKeptArr() {
		return lpSolver.getCentralSysKeptArr();
	}
	
	public double[][] getSystemSiteResultMatrix() {
		return lpSolver.getSystemSiteResultMatrix();
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
