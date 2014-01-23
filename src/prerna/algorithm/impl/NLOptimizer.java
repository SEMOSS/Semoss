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
