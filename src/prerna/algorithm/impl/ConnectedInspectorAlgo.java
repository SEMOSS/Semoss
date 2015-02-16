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
