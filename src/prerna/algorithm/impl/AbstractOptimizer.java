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
package prerna.algorithm.impl;

import lpsolve.LpSolveException;
import prerna.algorithm.api.IAlgorithm;

/**
 * Extended to optimize the functionalities of various algorithms.
 */
public abstract class AbstractOptimizer implements IAlgorithm{

	/**
	 * Sets up model.
	 */
	public abstract void setupModel();
	
	/**
	 * Gathers data set.
	 */
	public abstract void gatherDataSet();
	
	/**
	 * Sets constraints for algorithm.
	 */
	public abstract void setConstraints();
	
	/**
	 * Sets the algorithm/function to be executed.
	 */
	public abstract void setObjFunction();
	
	/**
	 * Deletes model.
	 */
	public abstract void deleteModel();
	
	/**
	 * Sets variables in algorithm.
	 */
	public abstract void setVariables() throws LpSolveException;
	
	/**
	 * Executes algorithm.
	 */
	@Override
	public abstract void execute();
	
	
}
