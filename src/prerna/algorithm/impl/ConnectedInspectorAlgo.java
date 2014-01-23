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

import prerna.algorithm.api.IAlgorithm;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;

/**
 * Allows algorithms to be performed on different types of playsheets.
 */
public class ConnectedInspectorAlgo implements IAlgorithm {

	GraphPlaySheet gps = null;
	
	/**
	 * Casts the IPlaySheet into a GraphPlaySheet.
	 * @param IPlaySheet to be casted
	 */
	@Override
	public void setPlaySheet(IPlaySheet graphPlaySheet) {
		gps = (GraphPlaySheet)graphPlaySheet;
	}

	/**
	 * Gets variables
	 * //TODO: Return empty object instead of null
	 * @return null  */
	@Override
	public String[] getVariables() {
		return null;
	}

	/**
	 * Executes algorithm.
	 * Gets forest, adds nodes from forest into SimpleDirectedGraph, runs connectivity inspector, does the sets.
	 */
	@Override
	public void execute() {

	}

	/**
	 * Gets the name of the algorithm in String form.
	 * //TODO: Return empty object instead of null
	 * @return null
	 */
	@Override
	public String getAlgoName() {
		return null;
	}
	
	

}
