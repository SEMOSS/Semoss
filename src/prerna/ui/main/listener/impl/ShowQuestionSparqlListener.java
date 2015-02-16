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
package prerna.ui.main.listener.impl;

import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.util.Hashtable;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JTextArea;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;
import prerna.util.Utility;

/**
 * If the custom button is selected, shows the current SPARQL query
 * for the selected question when the show SPARQL button is clicked.
 */
public class ShowQuestionSparqlListener implements IChakraListener {

	JTextArea view = null;

	/**
	 * Method setModel.  Sets the model that the query will access.
	 * @param model JComponent
	 */
	public void setModel(JComponent model)
	{
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JButton btnGetQuestionSparql = (JButton)DIHelper.getInstance().getLocalProp(Constants.GET_CURRENT_SPARQL);
		JTextArea area = (JTextArea)DIHelper.getInstance().getLocalProp(Constants.SPARQL_AREA_FIELD);
		ToggleButton btnCustomSparql = (ToggleButton)DIHelper.getInstance().getLocalProp(Constants.CUSTOMIZE_SPARQL);

		// get the selected engine
		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repos = (Object[]) list.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(repos[0] + "");
		// get the selected question
		String insightString = ((JComboBox) DIHelper.getInstance().getLocalProp(Constants.QUESTION_LIST_FIELD)).getSelectedItem() + "";

		
		// create insight to get sparql text
		Insight insight = ((AbstractEngine)engine).getInsight2(insightString).get(0);
		if(insight.getOutput().equals("Unknown")) {
			String[] insightStringSplit = insightString.split("\\. ", 2);
			insightString = insightStringSplit[1];
			insight = ((AbstractEngine)engine).getInsight2(insightString).get(0);
		}
		String sparql = Utility.normalizeParam(insight.getSparql());

		// only allow use of get current sparql question btn when custom sparql btn is selected
		if(btnCustomSparql.isSelected())
		{
			// when get current sparql question btn is pressed
			if(btnGetQuestionSparql.isEnabled())
			{
				// code is taken from QuestionListener performs param filling so that the query that is inputed is complete
	
				// populate query to display based on parameters 
				JPanel panel = (JPanel) DIHelper.getInstance().getLocalProp(Constants.PARAM_PANEL_FIELD);
				DIHelper.getInstance().setLocalProperty(Constants.UNDO_BOOLEAN,false);
				// get the currently visible panel
				Component[] comps = panel.getComponents();
				JComponent curPanel = null;
				for (int compIndex = 0; compIndex < comps.length
						&& curPanel == null; compIndex++)
					if (comps[compIndex].isVisible())
						curPanel = (JComponent) comps[compIndex];

				// get all the param field
				Component[] fields = curPanel.getComponents();
				Hashtable paramHash = new Hashtable();

				for (int compIndex = 0; compIndex < fields.length; compIndex++) {
					if (fields[compIndex] instanceof ParamComboBox) {
						String fieldName = ((ParamComboBox) fields[compIndex])
								.getParamName();
						String fieldValue = ((ParamComboBox) fields[compIndex])
								.getSelectedItem() + "";
						String uriFill = ((ParamComboBox) fields[compIndex])
								.getURI(fieldValue);
						if (uriFill == null)
							uriFill = fieldValue;
						paramHash.put(fieldName, uriFill);
						}
				}
				sparql = prerna.util.Utility.fillParam(sparql, paramHash);
				
				// set the sparql area with the query
				area.setText(sparql);
				area.setFont(new Font("Tahoma", Font.PLAIN, 11));
				area.setForeground(Color.BLACK);
				
				// change the playsheet selected to the layout of the imported question query
				String layoutValue = (String)DIHelper.getInstance().getLocalProp(Constants.CURRENT_PLAYSHEET);
				JComboBox playSheetComboBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.PLAYSHEET_COMBOBOXLIST);
				playSheetComboBox.setSelectedItem(PlaySheetEnum.getNameFromClass(layoutValue));			
			}
		}
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.view = (JTextArea)view;
	}
}
