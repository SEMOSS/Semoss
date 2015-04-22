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
public class SysSiteSavingsOptFunction extends SysSiteOptFunction{

	/**
	 * Given a budget, optimize the savings to return max savings.
	 * @return double	max savings possible when optimizing for budget*/
	public double value(double arg0) {

		count++;
//		updateProgressBar("Iteration " + count);
		System.out.println("budget "+arg0);
		
		long startTime;
		long endTime;
		startTime = System.currentTimeMillis();
		
		lpSolver.updateBudget(arg0);
		lpSolver.setTimeOut(200);
		lpSolver.execute();
		
		endTime = System.currentTimeMillis();
		System.out.println("Time to run iteration " + count + ": " + (endTime - startTime) / 1000 );
		
		if(!lpSolver.isOptimalSolution()) {
			System.out.println("iteration " + count + ": solution is not optimal ");
			return 0.0;
		}
		
		double deployCost = lpSolver.getTotalDeploymentCost();
		
		double yearsToComplete;
		if(maxBudget == 0)
			yearsToComplete = 0.0;
		else
			yearsToComplete = deployCost / (maxBudget / years);
		
		double savings = (years - Math.ceil(yearsToComplete)) * (currentSustainmentCost - lpSolver.getObjectiveVal());
		
		System.out.println("iteration " + count + ": budget entered " + arg0 + ", actual cost to deploy " + deployCost + ", savings over entire time frame "+savings);
		
		return savings;
	}

}
