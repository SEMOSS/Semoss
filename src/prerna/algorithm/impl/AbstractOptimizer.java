/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
