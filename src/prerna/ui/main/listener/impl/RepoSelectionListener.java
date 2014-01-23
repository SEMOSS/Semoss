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

import java.util.Collections;
import java.util.Vector;

import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.FactSheetReportComboBox;
import prerna.ui.components.specific.tap.ServiceSelectPanel;
import prerna.ui.components.specific.tap.SourceSelectPanel;
import prerna.ui.components.specific.tap.TransitionReportComboBox;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

/**
 * Controls the selection of a repository.
 */
public class RepoSelectionListener implements ListSelectionListener {

	static RepoSelectionListener instance = null;

	/**
	 * Method getInstance.  Gets the instance of the repo selection listener.	
	 * @return RepoSelectionListener */
	public static RepoSelectionListener getInstance()
	{
		if(instance == null)
			instance = new RepoSelectionListener();
		return instance;
	}

	/**
	 * Constructor for RepoSelectionListener.
	 */
	protected RepoSelectionListener()
	{
	}

	Logger logger = Logger.getLogger(getClass());
	// when the repo is selected, load the specific properties file
	// along with it load the database and the questions

	/**
	 * Method valueChanged.  Retrieves the repository information for a database when it is selected from the list.
	 * @param e ListSelectionEvent
	 */
	@Override
	public void valueChanged(ListSelectionEvent e) {
		logger.info("Repository Changed");
		JList list = (JList)e.getSource(); //DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		if (!list.isSelectionEmpty())
		{
			logger.info("Engine selected " + list.getSelectedValue()); //e.getFirstIndex();
			// now get the prop file
			// need to change this to local prop
			 //String qPropName = (String)DIHelper.getInstance().getCoreProp().get(list.getSelectedValue() + "_" + Constants.DREAMER);
			 //String ontologyPropName = (String)DIHelper.getInstance().getCoreProp().get(list.getSelectedValue() + "_" + Constants.ONTOLOGY);
			
			// logger.info("Question file to load " + qPropName);
			// logger.info("Ontology file to load " + qPropName);
			
			// and use the prop file to load the engine and perspectives
			//DIHelper.getInstance().loadEngineProp(list.getSelectedValue()+"", qPropName, ontologyPropName);
			// do this after
			// need a method in the DIHelper which loads a properties file first
			// and then loads perspectives etc.
			// once this is done.. keep the core properties pointed to it / need to modify the calls on process query listener etc.
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(list.getSelectedValue()+"");
			//Hashtable perspectiveHash = (Hashtable) DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE);
			//Vector<String> perspectives = Utility.convertEnumToStringVector(perspectiveHash.keys(), perspectiveHash.size());
			Vector<String> perspectives = engine.getPerspectives();
			Collections.sort(perspectives);
			//logger.info("Perspectives " + perspectiveHash);
			JComboBox box = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE_SELECTOR);
			box.removeAllItems();
			
			
			for(int itemIndex = 0;itemIndex < perspectives.size();((JComboBox)DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE_SELECTOR)).addItem(perspectives.get(itemIndex)), itemIndex++);

			//box.setSelectedIndex(0);
			//box.firePopupMenuWillBecomeVisible();
			//fill transition report combo box
			try{
				TransitionReportComboBox transCostReportcomboBox = (TransitionReportComboBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_REPORT_COMBO_BOX);
				transCostReportcomboBox.setEngine(list.getSelectedValue()+"");
				transCostReportcomboBox.run();
			}
			catch(Exception ex){
			}
			//Fill Fact Sheet Report Select System Combo Box
			try {
				FactSheetReportComboBox factSheetReportcomboBox = (FactSheetReportComboBox) DIHelper.getInstance().getLocalProp(Constants.FACT_SHEET_SYSTEM_SELECT_COMBO_BOX);
				factSheetReportcomboBox.setEngine(list.getSelectedValue()+"");
				factSheetReportcomboBox.run();
			}
			catch (Exception e1) {
				
			}
			//Fill tasker generation select system combo box
			try {
				FactSheetReportComboBox taskerGenerationReportComboBox = (FactSheetReportComboBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.TASKER_GENERATION_SYSTEM_COMBO_BOX);
				taskerGenerationReportComboBox.setEngine(list.getSelectedValue()+"");
				taskerGenerationReportComboBox.run();
			}
			catch (Exception e1) {
				
			}
			
			
			Object[] repos = new Object[1];
			try{
				ServiceSelectPanel transitionSerPanel = (ServiceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_SERVICE_PANEL);
				transitionSerPanel.engine=(IEngine)DIHelper.getInstance().getLocalProp(list.getSelectedValue()+"");
				transitionSerPanel.getServices();

			}
			catch(Exception ex){
			}
			try{
				SourceSelectPanel sourceSelPanel = (SourceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.SOURCE_SELECT_PANEL);
				sourceSelPanel.engine=(IEngine)DIHelper.getInstance().getLocalProp(list.getSelectedValue()+"");
				sourceSelPanel.getServices();
			}catch(Exception ex){}
		}
	}
}
