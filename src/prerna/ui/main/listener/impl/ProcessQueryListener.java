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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.project.api.IProject;
import prerna.ui.components.MapComboBoxRenderer;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.helpers.OldInsightProcessor;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * 1. Get information from the textarea for the query 2. Process the query
 * to create a graph 3. Create a playsheet and fill it with the respective
 * information 4. Set all the controls reference within the PlaySheet
 */
public class ProcessQueryListener extends AbstractAction implements IChakraListener {
	// where all the parameters are set
	// this will implement a cardlayout and then on top of that the param panel
	JPanel paramPanel = null;
	// right hand side panel
	JComponent rightPanel = null;
	static final Logger logger = LogManager.getLogger(ProcessQueryListener.class.getName());
	JTextArea sparql = null;
	boolean custom = false;
	boolean append = false;
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param actionevent ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		//enable the top right playsheet selector button
		JButton btnShowPlaySheetsList = (JButton) Utility.getDIHelperLocalProperty(Constants.SHOW_PLAYSHEETS_LIST);
		btnShowPlaySheetsList.setEnabled(true);

		//setting the playsheet selectiondropdown as enabled when a graph is created
		((JButton) Utility.getDIHelperLocalProperty(Constants.SHOW_PLAYSHEETS_LIST)).setEnabled(true);
		
		JToggleButton btnCustomSparql = (JToggleButton) Utility.getDIHelperLocalProperty(Constants.CUSTOMIZE_SPARQL);
		JToggleButton appendBtn = (JToggleButton) Utility.getDIHelperLocalProperty(Constants.APPEND);

		// get the selected repository, in case someone selects multiple, it'll always use first one
		JList list = (JList) Utility.getDIHelperLocalProperty(Constants.REPO_LIST);
		String engineName = (String)list.getSelectedValue();

		Map<String, List<Object>> paramHash = new HashMap<>();
		OldInsight insight = null;
		IDatabaseEngine engine = (IDatabaseEngine) Utility.getDIHelperLocalProperty(engineName);

		// There are four options to get through here:
		// 1. Not custom; Not overlay ---- get insight and run it
		// 2. Not custom; Yes overlay ---- get new insight, get dmc from that insight, set dmc in stored insight
		// 3. Yes custom; Not overlay ---- create new insight, create dmc, set in new insight
		// 4. Yes custom; Yes overlay ---- create dmc, set in stored insight
		Vector<DataMakerComponent> dmcList = new Vector<>();

		if (appendBtn.isSelected()) { // if it is overlay, we will be setting a dmc into the stored insight to run
			logger.debug("Appending ");
			
			insight = (OldInsight) InsightStore.getInstance().getActiveInsight();
			insight.setAppend(true);
			
			if (btnCustomSparql.isSelected()) { // if it is custom, we need to create a dmc to pass in
				DataMakerComponent dmc = createDMC(engineName);
				dmcList.add(dmc);
			}
			else { // if it is not custom, we need to get dmc list off of selected insight
				OldInsight selectedInsight = getSelectedInsight(engine);
				dmcList.addAll(selectedInsight.getDataMakerComponents());
//				paramHash = Utility.getTransformedNodeNamesMap(engine, getParamHash(), false);
				paramHash = getParamHash();
			}
			insight.setDataMakerComponents(dmcList);
		}
		
		else { // if it is Not overlay, we'll be creating a new insight and storing it
			if (btnCustomSparql.isSelected()) { // if it is custom, create new insight, create dmc set in new insight
				DataMakerComponent dmc = createDMC(engineName);
				dmcList.add(dmc);
				String psString = getPlaySheetString();
				insight = new OldInsight(engine, null, psString);
				insight.setDataMakerComponents(dmcList);
			}
			else { // if it is not custom, we need to get dmc list off of selected insight
				insight = getSelectedInsight(engine);
//				paramHash = Utility.getTransformedNodeNamesMap(engine, getParamHash(), false);
				paramHash = getParamHash();
			}
		}

		insight.setParamHash(paramHash);
		OldInsightProcessor runner = new OldInsightProcessor(insight);
		runner.run();
	}

	/**
	 * Method setRightPanel.  Sets the right panel that the listener will access.
	 * @param view JComponent
	 */
	public void setRightPanel(JComponent view) {
		this.rightPanel = view;
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.sparql = (JTextArea) view;

	}
	
	private Map<String, List<Object>> getParamHash(){
		Map<String, List<Object>> paramHash = new HashMap<>();
		// get Swing UI and set ParamHash;
		JPanel panel = (JPanel) Utility.getDIHelperLocalProperty(Constants.PARAM_PANEL_FIELD);
		// get the currently visible panel
		Component[] comps = panel.getComponents();
		JComponent curPanel = null;
		for (int compIndex = 0; compIndex < comps.length
				&& curPanel == null; compIndex++)
			if (comps[compIndex].isVisible())
				curPanel = (JComponent) comps[compIndex];
		// get all the param field
		if (curPanel != null) {
			Component[] fields = curPanel.getComponents();
			for (int compIndex = 0; compIndex < fields.length; compIndex++) {
				if (fields[compIndex] instanceof ParamComboBox) {
					String fieldName = ((ParamComboBox) fields[compIndex]).getParamName();
					String fieldValue = ((ParamComboBox) fields[compIndex]).getSelectedItem() + "";
					String uriFill = ((ParamComboBox) fields[compIndex]).getURI(fieldValue);
					if (uriFill == null)
						uriFill = fieldValue;
					List<Object> uriFillList = new ArrayList<>();
					uriFillList.add(uriFill);
					paramHash.put(fieldName, uriFillList);
				}
			}
		}
		return paramHash;
	}
	
	private OldInsight getSelectedInsight(IDatabaseEngine engine){
		IProject project = Utility.getProject(engine.getEngineId());
		String insightString = ((Map<String, String>) ((JComboBox<Map<String,String>>) Utility.getDIHelperLocalProperty(Constants.QUESTION_LIST_FIELD)).getSelectedItem()).get(MapComboBoxRenderer.KEY);
		String[] insightStringSplit = insightString.split("\\. ", 2);
		if( ((OldInsight) project.getInsight(insightString).get(0)).getOutput().equals("Unknown")){
			insightString = insightStringSplit[1];
		}

		return (OldInsight) project.getInsight(insightString).get(0);
	}
	
	private DataMakerComponent createDMC(String engineName){
		String query = ((JTextArea) Utility.getDIHelperLocalProperty(Constants.SPARQL_AREA_FIELD)).getText();

		return new DataMakerComponent(engineName, query);
	}
	
	private String getPlaySheetString(){
		JComboBox playSheetComboBox = (JComboBox) Utility.getDIHelperLocalProperty(Constants.PLAYSHEET_COMBOBOXLIST);
		String playSheetString = playSheetComboBox.getSelectedItem()+"";
		if(playSheetString.startsWith("*"))
		{
			playSheetString = playSheetComboBox.getName();
		}
		return playSheetString;
	}
}
