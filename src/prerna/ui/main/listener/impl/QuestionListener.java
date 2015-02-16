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

import java.awt.CardLayout;
import java.awt.event.ActionEvent;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.ui.components.ParamPanel;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;
import prerna.util.Utility;

/**
 *  listens for the change in questions, then refreshes the sparql area with the actual question in SPARQL
 *	 parses the SPARQL to find out all the parameters
 *	 refreshes the panel with all the parameters
 */
public class QuestionListener implements IChakraListener {

	Hashtable model = null;
	JPanel view = null; // reference to the param panel
	JTextArea sparqlArea = null;
	Hashtable panelHash = new Hashtable();
	String prevQuestionId = "";
	static final Logger logger = LogManager.getLogger(QuestionListener.class.getName());

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param actionevent ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		JComboBox questionBox = (JComboBox)actionevent.getSource();
		// get the currently selected index
		String fullQuestion = (String)questionBox.getSelectedItem();

		
		// get the question Hash from the DI Helper to get the question name
		// get the ID for the question
		if(fullQuestion != null)
		{
			String[] questionSplit = fullQuestion.split("\\. ", 2);
			String question = questionSplit[1];
			
			JToggleButton btnCustomSparql = (JToggleButton) DIHelper.getInstance().getLocalProp(Constants.CUSTOMIZE_SPARQL);
			btnCustomSparql.setSelected(false);

			JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			// get the selected repository
			List selectedValuesList = list.getSelectedValuesList();
			String selectedVal = selectedValuesList.get(selectedValuesList.size()-1).toString();

			Insight in = null;
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(selectedVal);
			if(!((AbstractEngine)engine).getInsight2(question).get(0).getOutput().equals("Unknown")) {
				in = ((AbstractEngine)engine).getInsight2(question).get(0);
			} else {
				question = fullQuestion;
				in = ((AbstractEngine)engine).getInsight2(question).get(0);
			}

			// now get the SPARQL query for this id
			String sparql = in.getSparql();
			String layoutValue = in.getOutput();
			// save the playsheet for the current question for modifying current query
			JComboBox playSheetComboBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.PLAYSHEET_COMBOBOXLIST);
			// set the model each time a question is choosen to include playsheets that are not in PlaySheetEnum
			playSheetComboBox.setModel(new DefaultComboBoxModel(PlaySheetEnum.getAllSheetNames().toArray()));
			if(!PlaySheetEnum.getAllSheetClasses().contains(layoutValue))
			{
				String addPlaySheet = layoutValue.substring(layoutValue.lastIndexOf(".") +1);
				playSheetComboBox.addItem("*" + addPlaySheet);
				playSheetComboBox.setName(layoutValue); // This is used to get the full layout value in ProcessQueryListener if the custom playsheet has been selected
				playSheetComboBox.setSelectedItem("*" + addPlaySheet);
			}
			else{
				playSheetComboBox.setSelectedItem(PlaySheetEnum.getNameFromClass(layoutValue));
			}

			logger.info("Sparql is " + sparql);

			Vector<SEMOSSParam> paramInfoVector = engine.getParams(question);
			
			ParamPanel panel = new ParamPanel();
			panel.setParams(paramInfoVector);
			//panel.setParamType(paramHash2);
			panel.setQuestionId(in.getId());
			panel.paintParam();

			// finally add the param to the core panel
			// confused about how to add this need to revisit
			JPanel mainPanel = (JPanel)DIHelper.getInstance().getLocalProp(Constants.PARAM_PANEL_FIELD);
			mainPanel.add(panel, question + "_1"); // mark it to the question index
			CardLayout layout = (CardLayout)mainPanel.getLayout();
			layout.show(mainPanel, question + "_1");
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.view = (JPanel)view;

	}
}
