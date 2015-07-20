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
package prerna.ui.helpers;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ui.components.playsheets.MachineLearningModulePlaySheet;

/**
 * This class helps with running the create view method for a playsheet.
 */
public class ClusteringModuleUpdateRunner implements Runnable{

	private MachineLearningModulePlaySheet playSheet = null;
	private ITableDataFrame dataFrame;
	private List<String> skipColumns;
	
	/**
	 * Constructor for PlaysheetCreateRunner.
	 * @param playSheet IPlaySheet
	 */
	public ClusteringModuleUpdateRunner(MachineLearningModulePlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}
	
	/**
	 * Method run.  Calls the create view method on the local play sheet.
	 */
	@Override
	public void run() {
		playSheet.recreateSimBarChart(dataFrame, skipColumns);
	}
	
	public void setValues(ITableDataFrame dataFrame, List<String> skipColumns) {
		this.dataFrame = dataFrame;
		this.skipColumns = skipColumns;
	}
	
}
