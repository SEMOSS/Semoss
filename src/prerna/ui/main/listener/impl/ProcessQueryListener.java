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

import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.ui.helpers.PlaysheetOverlayRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;
import prerna.util.QuestionPlaySheetStore;

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
	Logger logger = Logger.getLogger(getClass());
	JTextArea sparql = null;

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
		JToggleButton btnCustomSparql = (JToggleButton) DIHelper.getInstance().getLocalProp(Constants.CUSTOMIZE_SPARQL);
		JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(
				Constants.SHOW_PLAYSHEETS_LIST);
		btnShowPlaySheetsList.setEnabled(true);
		
		JList list = (JList) DIHelper.getInstance().getLocalProp(
				Constants.REPO_LIST);
		// get the selected repository
		Object[] repos = (Object[]) list.getSelectedValues();

		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(repos[0] + "");

		JComboBox questionList = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.QUESTION_LIST_FIELD);
		String id = DIHelper.getInstance().getIDForQuestion(questionList.getSelectedItem() + "");

		
		if (btnCustomSparql.isSelected()) {
			// perform user inputed sparql code
			JTextArea userInputSparql = (JTextArea) DIHelper.getInstance().getLocalProp(Constants.SPARQL_AREA_FIELD);
			String sparql = userInputSparql.getText();

			// determine user inputed playsheet to use
			// if playsheet is not in predefined combobox list get playsheet from current question	
			JComboBox playSheetComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.PLAYSHEET_COMBOBOXLIST);
			String userPlaySheet = (String) playSheetComboBox.getSelectedItem();
			String layoutValue;
			// what kind of absurdity is this ?
			if(userPlaySheet.startsWith("*"))
			{
				String keyToSearch = id + "_" + Constants.LAYOUT;
				layoutValue = DIHelper.getInstance().getProperty(keyToSearch);
			}
			else //get the playsheet from play sheet enum
			{
				layoutValue = PlaySheetEnum.getClassFromName(userPlaySheet);
			}

			logger.info("Layout value set to [" + layoutValue + "]");
			logger.info("Repository is " + repos);
			Runnable playRunner = null;
			
			// if user is inputing a new custom querry
			for (int repoIndex = 0; repoIndex < repos.length; repoIndex++) {
				//engine.setEngineName(repos[repoIndex] + "");
				logger.info("Selecting repository " + repos[repoIndex]);
				// use the layout to load the sheet later
				// see if the append is on
				JToggleButton append = (JToggleButton) DIHelper.getInstance().getLocalProp(Constants.APPEND);
				logger.debug("Toggle is selected");

				IPlaySheet playSheet = null;
				try {
					playSheet = (IPlaySheet) Class.forName(layoutValue).getConstructor(null).newInstance(null);
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.fatal(ex);
				}
				if (append.isSelected()) {
					logger.debug("Appending ");
					playSheet = QuestionPlaySheetStore.getInstance().getActiveSheet();
					playSheet.setRDFEngine((IEngine) engine);

					playRunner = new PlaysheetOverlayRunner(playSheet);
					playSheet.setQuery(sparql);
				} else {
					QuestionPlaySheetStore.getInstance().customcount++;
					playSheet.setTitle("Custom Query - "+QuestionPlaySheetStore.getInstance().getCustomCount());
					playSheet.setQuery(sparql);
					playSheet.setRDFEngine((IEngine) engine);
					playSheet.setQuestionID(QuestionPlaySheetStore.getInstance().getCount()+"custom");
					JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
					playSheet.setJDesktopPane(pane);
					playRunner = new PlaysheetCreateRunner(playSheet);					
				}
				QuestionPlaySheetStore.getInstance().put(QuestionPlaySheetStore.getInstance().getCount()+"custom", playSheet);
				System.out.println(QuestionPlaySheetStore.getInstance().getCustomCount());
				Thread playThread = new Thread(playRunner);
				playThread.start();
				
			}

		} else {
			// using default sparql queries
			// get the insight from this engine
			Insight in = engine.getInsight(questionList.getSelectedItem()+"");
			
			//String sparql = in.getSparql();
			String sparql = (String) DIHelper.getInstance().getLocalProp(Constants.BASE_QUERY);
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
			Hashtable paramHash = new Hashtable();
			String title = "";

			for (int compIndex = 0; compIndex < fields.length; compIndex++) {
				if (fields[compIndex] instanceof ParamComboBox) {
					String fieldName = ((ParamComboBox) fields[compIndex]).getParamName();
					String fieldValue = ((ParamComboBox) fields[compIndex]).getSelectedItem() + "";
					String uriFill = ((ParamComboBox) fields[compIndex]).getURI(fieldValue);
					if (uriFill == null)
						uriFill = fieldValue;
					paramHash.put(fieldName, uriFill);
					title = fieldValue + " - " + title;
				}
			}

			sparql = prerna.util.Utility.fillParam(sparql, paramHash);
			// Feed all of this information to the playsheet
			// get the layout class based on the query

			// Properties prop = DIHelper.getInstance().getEngineCoreProp();
			// uses pattern QUERY_Layout
			// need to get the key first here >>>>
			//String keyToSearch = id + "_" + Constants.LAYOUT;
			String layoutValue = in.getOutput(); //DIHelper.getInstance().getProperty(keyToSearch);

			// now just do class.forName for this layout Value and set it inside
			// playsheet
			// need to template this out and there has to be a directive to
			// identify
			// specifically what sheet we need to refer to


			logger.info("Layout value set to [" + layoutValue + "]");
			logger.info("Repository is " + repos);
			Runnable playRunner = null;
			// if user is inputing a new custom querry
			for (int repoIndex = 0; repoIndex < repos.length; repoIndex++) {
				//engine.setEngineName(repos[repoIndex] + "");
				logger.info("Selecting repository " + repos[repoIndex]);
				// use the layout to load the sheet later
				// see if the append is on
				JToggleButton append = (JToggleButton) DIHelper.getInstance().getLocalProp(Constants.APPEND);

				logger.debug("Toggle is selected");

				IPlaySheet playSheet = null;
				try {
					playSheet = (IPlaySheet) Class.forName(layoutValue).getConstructor(null).newInstance(null);
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.fatal(ex);
				}
				if (append.isSelected()) {
					logger.debug("Appending ");
					playSheet = QuestionPlaySheetStore.getInstance().getActiveSheet();
					playSheet.setRDFEngine((IEngine) engine);
					playRunner = new PlaysheetOverlayRunner(playSheet);
					playSheet.setQuery(sparql);
				} else {
					QuestionPlaySheetStore.getInstance().count++;
					String question = QuestionPlaySheetStore.getInstance().getCount()+". "+ id;
					String questionTitle = (String) questionList.getSelectedItem();
					String[] questionTitleArray = questionTitle.split("\\.");
					title = title+questionTitleArray[1].trim()+" ("+questionTitleArray[0]+")";
					playSheet.setTitle(title);
					playSheet.setQuery(sparql);
					playSheet.setRDFEngine((IEngine) engine);
					playSheet.setQuestionID(question);
					JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
					playSheet.setJDesktopPane(pane);
					playRunner = new PlaysheetCreateRunner(playSheet);
					QuestionPlaySheetStore.getInstance().put(question, playSheet);
				}				
				// thread
				Thread playThread = new Thread(playRunner);
				playThread.start();
			}
		}

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
