/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.util.Hashtable;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.ui.helpers.PlaysheetOverlayRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;

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
		// get all the component
		// get the current panel showing - need to do the isVisible
		// currently assumes all queries are SPARQL, needs some filtering if
		// there are other types of queries
		// especially the ones that would use JGraph
		// get the query

		//initiate executeQueryProcessor

		JToggleButton btnCustomSparql = (JToggleButton) DIHelper.getInstance().getLocalProp(Constants.CUSTOMIZE_SPARQL);
		JToggleButton appendBtn = (JToggleButton) DIHelper.getInstance().getLocalProp(Constants.APPEND);

		//set custom and append variables to processor
		ExecuteQueryProcessor exQueryProcessor = new ExecuteQueryProcessor();
		exQueryProcessor.setAppendBoolean(appendBtn.isSelected());
		exQueryProcessor.setCustomBoolean(btnCustomSparql.isSelected());

		//setting the playsheet selectiondropdown as enabled when a graph is created
		((JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST)).setEnabled(true);

		// get the selected repository, in case someone selects multiple, it'll always use first one
		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		String engineName = (String)list.getSelectedValue();

		//enable the top right playsheet selector button
		JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
		btnShowPlaySheetsList.setEnabled(true);


		//setup playsheet depending whether its custom or not, exQueryProcessor will also take care of append or create
		//custom query 
		if (btnCustomSparql.isSelected()) {
			String query = ((JTextArea) DIHelper.getInstance().getLocalProp(Constants.SPARQL_AREA_FIELD)).getText();
			JComboBox playSheetComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.PLAYSHEET_COMBOBOXLIST);
			String playSheetString = playSheetComboBox.getSelectedItem()+"";
			if(playSheetString.startsWith("*"))
			{
				playSheetString = playSheetComboBox.getName();
			}
			exQueryProcessor.processCustomQuery(engineName, query, playSheetString);

		} 
		else 
		{
			String insightString = ((JComboBox) DIHelper.getInstance().getLocalProp(Constants.QUESTION_LIST_FIELD)).getSelectedItem() + "";
			String[] insightStringSplit = insightString.split("\\. ", 2);
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			Insight in = ((AbstractEngine)engine).getInsight2(insightString).get(0);
			if(((AbstractEngine)engine).getInsight2(insightString).get(0).getOutput().equals("Unknown")){
				insightString = insightStringSplit[1];
			}
			
			//get Swing UI and set ParamHash";
			JPanel panel = (JPanel) DIHelper.getInstance().getLocalProp(Constants.PARAM_PANEL_FIELD);
			DIHelper.getInstance().setLocalProperty(Constants.UNDO_BOOLEAN,	false);
			// get the currently visible panel
			Component[] comps = panel.getComponents();
			JComponent curPanel = null;
			for (int compIndex = 0; compIndex < comps.length
					&& curPanel == null; compIndex++)
				if (comps[compIndex].isVisible())
					curPanel = (JComponent) comps[compIndex];
			// get all the param field
			Component[] fields = curPanel.getComponents();
			Hashtable<String, Object> paramHash = new Hashtable<String, Object>();
			for (int compIndex = 0; compIndex < fields.length; compIndex++) {
				if (fields[compIndex] instanceof ParamComboBox) {
					String fieldName = ((ParamComboBox) fields[compIndex]).getParamName();
					String fieldValue = ((ParamComboBox) fields[compIndex]).getSelectedItem() + "";
					String uriFill = ((ParamComboBox) fields[compIndex]).getURI(fieldValue);
					if (uriFill == null)
						uriFill = fieldValue;
					paramHash.put(fieldName, uriFill);
				}
			}
			exQueryProcessor.processQuestionQuery(engineName, insightString, paramHash);
		}

		//getplaysheet
		//then figure out if its append or create and then call the right threadrunners
		Runnable playRunner = null;	
		IPlaySheet playSheet= exQueryProcessor.getPlaySheet();
		if (appendBtn.isSelected()) {
			logger.debug("Appending ");
			playRunner = new PlaysheetOverlayRunner(playSheet);
		} 
		else 
		{
			JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
			playSheet.setJDesktopPane(pane);
			playRunner = new PlaysheetCreateRunner(playSheet);
		}				
		Thread playThread = new Thread(playRunner);
		playThread.start();

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
}
