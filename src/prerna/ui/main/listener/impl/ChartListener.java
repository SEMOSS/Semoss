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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.BrowserTabSheet3;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.QuestionPlaySheetStore;

/**
 * This listener implements the minimum spanning tree.
 */
public class ChartListener implements ActionListener {
	
	static final Logger logger = LogManager.getLogger(ChartListener.class.getName());

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		logger.info("Engaged the chart component successfully");
		// get to the vertexfilter data
		
		BrowserTabSheet3 tabS = new BrowserTabSheet3("/html/MHS-RDFSemossCharts/app/index.html", playSheet);
		playSheet.jTab.add("Charter", tabS);
		playSheet.jTab.setSelectedComponent(tabS);
		
		// get the edge hash
		// and the vertex hash
		// flesh them out into json and then call the page
	}			
}
