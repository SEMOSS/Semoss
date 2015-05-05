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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.SparqlArea;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.helpers.PlaysheetRemoveRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

/**
 * 1. Get information from the textarea for the query
 * 2. Process the query to Remove the graph
 * 3. Create a playsheet and fill it with the respective information
 * 4. Set all the controls reference within the PlaySheet 
 * 
 */
public class RemoveQueryListener extends SparqlAreaListener {
	// where all the parameters are set
	// this will implement a cardlayout and then on top of that the param panel
	JPanel paramPanel = null; 

	// right hand side panel
	JComponent rightPanel = null;
	static final Logger logger = LogManager.getLogger(RemoveQueryListener.class.getName());

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param actionevent ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		// get all the component
		// get the current panel showing - need to do the isVisible
		// currently assumes all queries are SPARQL, needs some filtering if there are other types of queries
		// especially the ones that would use JGraph
		if (QuestionPlaySheetStore.getInstance().getActiveSheet() != null)
		{
			JToggleButton btnCustomSparql = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.CUSTOMIZE_SPARQL);
			//if (!extend.isSelected() && !spql.isSelected())
			if (!btnCustomSparql.isSelected())
			{
				clearQuery();
			}
			// get the query

			// gets the panel component and parameters
			JPanel panel = (JPanel)DIHelper.getInstance().getLocalProp(Constants.PARAM_PANEL_FIELD);
			DIHelper.getInstance().setLocalProperty(Constants.UNDO_BOOLEAN, false);
			// get the currently visible panel
			Component [] comps = panel.getComponents();
			JComponent curPanel = null;
			for(int compIndex = 0;compIndex < comps.length && curPanel == null;compIndex++)
				if(comps[compIndex].isVisible())
					curPanel = (JComponent)comps[compIndex];

			// get all the param field
			Component [] fields = curPanel.getComponents();
			Hashtable paramHash = new Hashtable();
			String title = " - ";
			for(int compIndex = 0;compIndex < fields.length;compIndex++)
			{			
				if(fields[compIndex] instanceof ParamComboBox)
				{	
					String fieldName = ((ParamComboBox)fields[compIndex]).getParamName();
					String fieldValue = ((ParamComboBox)fields[compIndex]).getSelectedItem() + "";
					paramHash.put(fieldName, fieldValue);
					title = title + " " +  fieldValue;
				}		
			}
			// now get the text area
			logger.debug("Param Hash is set to " + paramHash);
			this.sparql.setText(prerna.util.Utility.fillParam(this.sparql.getText(), paramHash));

			// Feed all of this information to the playsheet
			// get the layout class based on the query
			SparqlArea area = (SparqlArea)this.sparql;
			Properties prop = DIHelper.getInstance().getCoreProp();

			// uses pattern QUERY_Layout
			// need to get the key first here >>>>
			JComboBox questionList = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.QUESTION_LIST_FIELD);
			String id = DIHelper.getInstance().getIDForQuestion(questionList.getSelectedItem() + "");
			String keyToSearch = id + "_" + Constants.LAYOUT;
			String layoutValue = prop.getProperty(keyToSearch);

			// now just do class.forName for this layout Value and set it inside playsheet
			// need to template this out and there has to be a directive to identify 
			// specifically what sheet we need to refer to

			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			// get the selected repository
			Object [] repos = (Object [])list.getSelectedValues();

			logger.info("Layout value set to [" + layoutValue +"]");
			logger.info("Repository is " + repos);
			Runnable playRunner = null;

			for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
			{
				IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
				String question = id + QuestionPlaySheetStore.getInstance().getIDCount();
				// use the layout to load the sheet later
				// see if the append is on
				logger.debug("Toggle is selected");

				GraphPlaySheet playSheet = null;
				logger.debug("Appending ");
				playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
				// need to create a playsheet append runner
				playRunner = new PlaysheetRemoveRunner(playSheet);
				playSheet.setQuery(this.sparql.getText());
				// thread
				Thread playThread = new Thread(playRunner);
				playThread.start();
			}

		}
	}

	/**
	 * Method clearQuery.  Clears the query from the question box.
	 */
	public void clearQuery()
	{
		JComboBox questionBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.QUESTION_LIST_FIELD);
		// get the currently selected index
		String question = (String)questionBox.getSelectedItem();
		// get the question Hash from the DI Helper to get the question name
		// get the ID for the question
		if(question != null)
		{
			String id = DIHelper.getInstance().getIDForQuestion(question);

			// now get the SPARQL query for this id
			String sparql = DIHelper.getInstance().getProperty(id + "_" + Constants.QUERY);	

			// get all the parameters and names from the SPARQL
			Hashtable paramHash = Utility.getParams(sparql);
			// for each of the params pick out the type now
			Enumeration keys = paramHash.keys();
			while(keys.hasMoreElements())
			{	
				String key = (String)keys.nextElement();
				StringTokenizer tokens = new StringTokenizer(key, "-");
				// the first token is the name of the variable
				String varName = tokens.nextToken();
				String varType = Constants.EMPTY;
				if(tokens.hasMoreTokens())
					varType = tokens.nextToken();
				logger.debug(varName + "<<>>" + varType);
				paramHash.put(key,"@" + varName + "@");
			}
			sparql = Utility.fillParam(sparql, paramHash);
			logger.debug(sparql  + "<<<");

			// just replace the SPARQL Area - Dont do anything else
			JTextArea area = (JTextArea)DIHelper.getInstance().getLocalProp(Constants.SPARQL_AREA_FIELD);
			area.setText(sparql);
		}
	}

	/**
	 * Method setRightPanel.  Sets the right panel that the listener will access.
	 * @param view JComponent
	 */
	public void setRightPanel(JComponent view) {
		this.rightPanel = view;
	}


}
