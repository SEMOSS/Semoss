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

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.ui.components.MapComboBoxRenderer;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Controls selection of the perspective from the left hand pane.
 */
public class PerspectiveSelectionListener extends AbstractListener {
	
	private static final Logger logger = LogManager.getLogger(PerspectiveSelectionListener.class.getName());	

	public JComponent view = null;
	// needs to find what is being selected from event
	// based on that refresh the view of questions for that given perspective
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox<String> bx = (JComboBox<String>)e.getSource();
		String perspective = "";
		if(bx.getSelectedItem() != null) {
			perspective = bx.getSelectedItem().toString();
		}
		
		if(!perspective.isEmpty()) {
			logger.info("Selected " + perspective + " <> " + view);
			JComboBox<Map<String, String>> qp = (JComboBox<Map<String, String>>)view;
			ArrayList<String> tTip = new ArrayList<String>();
			qp.removeAllItems();
			// grab the selected engine
			JList<String> list = (JList<String>) Utility.getDIHelperLocalProperty(Constants.REPO_LIST);
			List<String> selectedValuesList = list.getSelectedValuesList();
			String selectedVal = selectedValuesList.get(selectedValuesList.size()-1).toString();
			IDatabaseEngine engine = Utility.getDatabase(selectedVal);
			
			// grab the list of insights in the selected engine, this list is already ordered
			IProject project = Utility.getProject(engine.getEngineId());
			Vector<String> questionsV = project.getInsights(perspective);
			Vector<String> newQuestionV = new Vector<String>();
			Vector<String> newQuestionID = new Vector<String>();
			if(questionsV != null){
				Vector<Insight> vectorInsight = project.getInsight(questionsV.toArray(new String[questionsV.size()]));
				for(int itemIndex = 0;itemIndex < vectorInsight.size();itemIndex++){
					Insight in = vectorInsight.get(itemIndex);
					// modify values to include the number ordering for optimal user experience <- cause we care about that
					newQuestionV.add(itemIndex + 1 + ". " + in.getInsightName());
					newQuestionID.add(in.getInsightId());
				}
			}
			
			// paint values
			for(int itemIndex = 0; itemIndex < newQuestionV.size(); itemIndex++) {
				tTip.add(newQuestionV.get(itemIndex));
				MapComboBoxRenderer renderer = new MapComboBoxRenderer();
				qp.setRenderer(renderer);
				renderer.setTooltips(tTip);
				renderer.setBackground(Color.WHITE);
				Map<String, String> comboMap = new HashMap<String, String>();
				comboMap.put(MapComboBoxRenderer.KEY, newQuestionID.get(itemIndex));
				comboMap.put(MapComboBoxRenderer.VALUE, newQuestionV.get(itemIndex));
				qp.addItem(comboMap);
			}
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		logger.debug("View is set " + view);
		this.view = view;
	}


}
