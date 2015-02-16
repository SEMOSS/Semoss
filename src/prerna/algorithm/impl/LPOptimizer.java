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
package prerna.algorithm.impl;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IPlaySheet;

/**
 * This class has linear optimization functionality via the lpsolve import that is used primarily for ServiceOptimizer.
 */
public class LPOptimizer extends AbstractOptimizer{
	
	static final Logger logger = LogManager.getLogger(LPOptimizer.class.getName());
	
	public LpSolve solver;
	
	/**
	 * Sets the playsheet.
	 * @param 	Passed playsheet.
	 */
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		
	}

	/**
	 * Sets up the model for calculations to be performed upon.
	 */
	@Override
	public void setupModel() {
		try {
			setVariables();
		} catch (LpSolveException e1) {
			e1.printStackTrace();
		}
		setConstraints();
		solver.setAddRowmode(false);
		setObjFunction();
	}
	
	/**
	 * Gathers data set.
	 */
	@Override
	public void gatherDataSet() {
		
	}
	
	/**
	 * Gets variable names.

	 * //TODO: Return empty object instead of null
	 * @return 	List of variable names. */
	@Override
	public String[] getVariables() {
		return null;
	}

	/**
	 * Sets variables in model.
	 */
	@Override
	public void setVariables() throws LpSolveException {
		
	}
	
	/**
	 * Gets algorithm name.
	
	 * //TODO: Return empty object instead of null
	 * @return 	Name of algorithm. */
	@Override
	public String getAlgoName() {
		return null;
	}

	/**
	 * Sets constraints in the model.
	 */
	@Override
	public void setConstraints() {
		
	}

	/**
	 * Sets the function for calculations.
	 */
	@Override
	public void setObjFunction() {
		
	}

	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute() {
		solver.setVerbose(LpSolve.IMPORTANT);
		// solve the problem
		try {
			solver.solve();
		} catch (LpSolveException e) {
			e.printStackTrace();
		}
		
		// print solution
		//logger.info("Value of objective function: " + solver.getObjective());
	}

	/**
	 * Deletes the model in order to free memory.
	 */
	@Override
	public void deleteModel() {
		solver.deleteLp();
	}
}