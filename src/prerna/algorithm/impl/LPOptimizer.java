/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;
import prerna.ui.components.api.IPlaySheet;

/**
 * This class provides liner optimization functionality via the lpsolve import.
 * It is used for SOA service optimization, residual system optimization,
 * system rationalization at the site level, system replacement optimization,
 * and road map optimization.
 * General process for use is as follows: set variables, set objective function,
 * set constraints, execute, retrieve results, delete solver.
 * THIS WILL NOT RUN IF YOU DON'T HAVE LPSOLVE DLLs
 * @author ksmart
 */
public class LPOptimizer extends AbstractOptimizer{
	
	protected static final Logger LOGGER = LogManager.getLogger(LPOptimizer.class.getName());
	
	protected int solved;
	
	public LpSolve solver;
	
	/**
	 * Gathers data set.//TODO i don't think we ever use this
	 *
	 */
	@Override
	public void gatherDataSet() {
		
	}
	
	/**
	 * Sets up the model for calculations to be performed upon.
	 * Includes: setting variables, setting objective function,
	 * setting constraints, and writing LP model to model.lp in SEMOSS file for debugging.
	 * throws LpSolveException if any errors in failing to set up the model.
	 */
	@Override
	public void setupModel() throws LpSolveException{
		LOGGER.info("Setting up LP Solve model...");
		try {
			setVariables();
			solver.setAddRowmode(true);
			setObjFunction();
			setConstraints();
			solver.setAddRowmode(false);
		} catch (LpSolveException e1) {
			LOGGER.error("Failed to set up LP Solve model.");
			throw new LpSolveException("Failed to set up LP Solve model" + e1.getMessage());
		}

		LOGGER.info("Successfully set up LP Solve model");
		
		writeLp("model.lp");
	}
	
	/**
	 * Sets variables in model.
	 * Extensions should instantiate a new LpSolver with proper number of variables.
	 * Variables should be set to binary, original variable branch, etc as needed.
	 * Variables can also be named here for debugging.
	 * Generally using: solver = LpSolve.makeLp();
	 */
	@Override
	public void setVariables() throws LpSolveException {
		
	}
	
	/**
	 * Sets the function to be optimized as well as whether to minimize or maximize.
	 * Extensions should create the objective function and add to solver
	 * Generally using: solver.setObjFnex(); and solver.setMinim();
	 */
	@Override
	public void setObjFunction() throws LpSolveException  {
		
	}

	/**
	 * Sets constraints in the model.
	 * Extensions should create all constraints and add each to solver
	 * Generally using: solver.addConstraintex();
	 */
	@Override
	public void setConstraints() throws LpSolveException  {
		
	}
	
	/**
	 * Writes the Lp problem to the filename specified.
	 * @param filename
	 */
	public void writeLp(String filename){
		try{
			solver.writeLp(filename);
		}catch (LpSolveException e) {
			LOGGER.error("Could not write LP Solve model to file. Continuing anyways...");
		}
	}
	
	/**
	 * Executes the optimization.
	 * Solved is set to reflect if optimization was successful.
	 * Extensions should determine if solution is optimal and process results from the solver
	 * Generally using: solver.getVarPrimalresult()
	 */
	@Override
	public void execute() {
		solver.setVerbose(LpSolve.IMPORTANT);
		try {
			LOGGER.info("Executing LP Solve...");
			solved = solver.solve();
			LOGGER.info("Successfully executed LP Solve...");
		} catch (LpSolveException e) {
			solved = -1;
			LOGGER.error("Failed to execute LP Solve...");
		}
	}

	/**
	 * Deletes the model in order to free memory.
	 */
	@Override
	public void deleteModel() {
		if(solver!=null) {
			LOGGER.info("Deleting LP solver...");
			solver.deleteLp();
			solver = null;
			LOGGER.info("Successfully deleted LP solver.");
		}
	}

	/**
	 * Gets variable names.
	 * //TODO: Return empty object instead of null. don't think we ever use this
	 * @return 	List of variable names. */
	@Override
	public String[] getVariables() {
		return null;
	}
	
	/**
	 * Gets algorithm name.
	 * //TODO: Return empty object instead of null dont think we ever use this
	 * @return 	Name of algorithm. */
	@Override
	public String getAlgoName() {
		return null;
	}
	
	/**
	 * Sets the playsheet.
	 * @param 	Passed playsheet.
	 */
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		
	}
}