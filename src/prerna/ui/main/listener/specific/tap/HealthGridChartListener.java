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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.BrowserTabSheet3;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.QuestionPlaySheetStore;

/**
 * Loads the health grid chart playsheet when user selects the "Create Health Grid" option in JMenuItem in GraphNodePopup.java
 */
public class HealthGridChartListener implements ActionListener {

	static final Logger logger = LogManager.getLogger(HealthGridChartListener.class.getName());

	/**
	 * Loads the html file used for the health grid chart playsheet
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		logger.info("Engaged the health grid chart component successfully");
		// get to the vertexfilter data
		
		BrowserTabSheet3 tabS = new BrowserTabSheet3("/html/MHS-RDFSemossCharts/app/grid.html", playSheet);
		playSheet.jTab.add("Health Grid Charter", tabS);
		playSheet.jTab.setSelectedComponent(tabS);
		
		// get the edge hash
		// and the vertex hash
		// flesh them out into json and then call the page
	}	
	
}
