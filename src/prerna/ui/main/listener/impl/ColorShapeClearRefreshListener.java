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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.Collection;

import javax.swing.JComponent;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Repaints and clears the active sheet when refresh is pressed.
 */
public class ColorShapeClearRefreshListener implements IChakraListener {
	
	static final Logger logger = LogManager.getLogger(ColorShapeClearRefreshListener.class.getName());
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param actionevent ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		logger.info("Calling action performed - refine view");	
//		GraphPlaySheet playSheet = (GraphPlaySheet)QuestionPlaySheetStore.getInstance().getActiveSheet();
		GraphPlaySheet playSheet = (GraphPlaySheet) ((OldInsight) InsightStore.getInstance().getActiveInsight()).getPlaySheet();
		TypeColorShapeTable.getInstance().clearAll();
		
		Collection<SEMOSSVertex> vertStore = playSheet.getVerts();
		for(SEMOSSVertex vert : vertStore)
			vert.resetColor();
		playSheet.getVertexLabelFontTransformer().clearSizeData();
		playSheet.getEdgeLabelFontTransformer().clearSizeData();
		
		JTable table = (JTable)DIHelper.getInstance().getLocalProp(Constants.COLOR_SHAPE_TABLE);
		table.repaint();
		
		playSheet.repaint();
		playSheet.genAllData();
		playSheet.showAll();
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {

	}
}
