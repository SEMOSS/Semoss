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

import prerna.ui.components.api.IPlaySheet;

/**
 * This class has additional optimization functionality.
 */
public class NLOptimizer extends AbstractOptimizer{

	
	/**
	 * Sets the playsheet.
	 * @param playSheet IPlaySheet		Passed playsheet.
	 */
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
	}

	/**
	 * Sets up the model for calculations to be performed upon.
	 */
	@Override
	public void setupModel() {
	}
	
	/**
	 * Gets variable names.

	 * //TODO: Return empty object instead of null
	 * @return String[] 	List of variable names. */
	@Override
	public String[] getVariables() {

		return null;
	}

	/**
	 * Sets variables in model.
	 */
	@Override
	public void setVariables()  {
		
	}
	
	/**
	 * Gets algorithm name.
	
	 * //TODO: Return empty object instead of null
	 * @return String 	Name of algorithm. */
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
		
	}

	/**
	 * Gathers data set.
	 */
	@Override
	public void gatherDataSet() {
		
	}

	/**
	 * Deletes the model in order to free memory.
	 */
	@Override
	public void deleteModel() {
		
	}

}
