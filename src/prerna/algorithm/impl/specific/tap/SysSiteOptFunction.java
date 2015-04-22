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

import java.util.ArrayList;


/**
 * Interface representing a univariate real function that is implemented for TAP system and service optimization functions.
 */
public class SysSiteOptFunction extends UnivariateOptFunction{
	
	protected int count = 0;
	protected SysSiteLPSolver lpSolver;
	
	protected double currentSustainmentCost;
	protected int years;
	protected double maxBudget;
	
	public void setVariables(int[][] systemDataMatrix, int[][] systemBLUMatrix, double[][] systemSiteMatrix, int[] systemTheater, int[] systemGarrison, Integer[] sysModArr, Integer[] sysDecomArr, double[] maintenaceCosts, double[] siteMaintenaceCosts, double[] siteDeploymentCosts,int[][] centralSystemDataMatrix,int[][] centralSystemBLUMatrix, int[] centralSystemTheater, int[] centralSystemGarrison, Integer[] centralModArr, Integer[] centralDecomArr, double[] centralSystemMaintenaceCosts, double maxBudget, int years, double currentSustainmentCost, ArrayList<String> sysList, ArrayList<String> centralSysList, ArrayList<String> dataList, ArrayList<String> bluList, ArrayList<String> siteList) {		
		this.maxBudget = maxBudget;
		this.years = years;
		this.currentSustainmentCost = currentSustainmentCost;
		
		long startTime;
		long endTime;
		startTime = System.currentTimeMillis();			

		lpSolver = new SysSiteLPSolver(systemDataMatrix, systemBLUMatrix, systemSiteMatrix, systemTheater, systemGarrison, sysModArr, sysDecomArr, maintenaceCosts, siteMaintenaceCosts, siteDeploymentCosts, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemTheater, centralSystemGarrison, centralModArr, centralDecomArr, centralSystemMaintenaceCosts, maxBudget / 2);
		lpSolver.setVariableNames(sysList, centralSysList, dataList, bluList, siteList);
		lpSolver.setupModel();
		
		endTime = System.currentTimeMillis();
		System.out.println("Time to set up the solver " + (endTime - startTime) / 1000 );

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

	public double getTotalDeploymentCost() {
		return lpSolver.getTotalDeploymentCost();
	}

	public double getFutureSustainmentCost() {
		return lpSolver.getObjectiveVal();
	}

}
