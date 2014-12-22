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

import java.util.Collections;
import java.util.List;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.DHMSMSystemSelectPanel;
import prerna.ui.components.specific.tap.FactSheetReportComboBox;
import prerna.ui.components.specific.tap.SelectRadioButtonPanel;
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

	static final Logger logger = LogManager.getLogger(RepoSelectionListener.class.getName());
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
		List selectedList = list.getSelectedValuesList();
		String selectedValue = selectedList.get(selectedList.size()-1).toString();
		if (!list.isSelectionEmpty() && !e.getValueIsAdjusting())
		{
			// now get the prop file
			// need to change this to local prop
			
			// do this after
			// need a method in the DIHelper which loads a properties file first
			// and then loads perspectives etc.
			// once this is done.. keep the core properties pointed to it / need to modify the calls on process query listener etc.
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(selectedValue);
			Vector<String> perspectives = engine.getPerspectives();
			Collections.sort(perspectives);
			
			JComboBox<String> box = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE_SELECTOR);
			box.removeAllItems();
			
			for(int itemIndex = 0;itemIndex < perspectives.size(); itemIndex++) {
				box.addItem(perspectives.get(itemIndex).toString());
			}
			
			//fill transition report combo box
			try{
				TransitionReportComboBox transCostReportcomboBox = (TransitionReportComboBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_REPORT_COMBO_BOX);
				transCostReportcomboBox.setEngine(selectedValue);
				transCostReportcomboBox.run();
			}
			catch(RuntimeException ex){
				logger.debug(ex);
			}
			//Fill Fact Sheet Report Select System Combo Box
			try {
				FactSheetReportComboBox factSheetReportcomboBox = (FactSheetReportComboBox) DIHelper.getInstance().getLocalProp(Constants.FACT_SHEET_SYSTEM_SELECT_COMBO_BOX);
				factSheetReportcomboBox.setEngine(selectedValue);
				factSheetReportcomboBox.run();
			}
			catch (RuntimeException e1) {
				logger.debug(e1);
			}
			//Fill tasker generation select system combo box
			try {
				FactSheetReportComboBox taskerGenerationReportComboBox = (FactSheetReportComboBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.TASKER_GENERATION_SYSTEM_COMBO_BOX);
				taskerGenerationReportComboBox.setEngine(selectedValue);
				taskerGenerationReportComboBox.run();
			}
			catch (RuntimeException e1) {
				logger.debug(e1);
			}
			
			//Fill Capability Fact Sheet Report Select Capability Combo Box
			try {
				FactSheetReportComboBox capabilityFactSheetReportComboBox = (FactSheetReportComboBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.CAPABILITY_FACT_SHEET_CAP_COMBO_BOX);
				capabilityFactSheetReportComboBox.setEngine(selectedValue);
				capabilityFactSheetReportComboBox.setKey("Capability");
				capabilityFactSheetReportComboBox.run();
			}
			catch (RuntimeException e1) {
				logger.debug(e1);
			}
			try{
				ServiceSelectPanel transitionSerPanel = (ServiceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_SERVICE_PANEL);
				transitionSerPanel.engine=(IEngine)DIHelper.getInstance().getLocalProp(selectedValue);
				transitionSerPanel.getServices();
			}
			catch(RuntimeException ex){
				logger.debug(ex);
			}
			try{
				SourceSelectPanel sourceSelPanel = (SourceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.SOURCE_SELECT_PANEL);
				sourceSelPanel.engine=(IEngine)DIHelper.getInstance().getLocalProp(selectedValue);
				sourceSelPanel.getCapabilities();
			}catch(Exception ex){
				logger.debug(ex);}
			
			try{
				SourceSelectPanel dhmsmCapSelPanel = (SourceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.DHMSM_CAPABILITY_SELECT_PANEL);
				dhmsmCapSelPanel.engine=(IEngine)DIHelper.getInstance().getLocalProp(selectedValue);
				dhmsmCapSelPanel.getCapabilities();
				
				SelectRadioButtonPanel radioSelPanel = (SelectRadioButtonPanel) DIHelper.getInstance().getLocalProp(Constants.SELECT_RADIO_PANEL);
				radioSelPanel.engine=dhmsmCapSelPanel.engine;
				radioSelPanel.clear();
			}catch(RuntimeException ex){
				logger.debug(ex);
			}			
			try{
				DHMSMSystemSelectPanel dhmsmSystemSelectPanel = (DHMSMSystemSelectPanel) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_SYSTEM_SELECT_PANEL);
				dhmsmSystemSelectPanel.addElements();
			}catch(RuntimeException ex){
				logger.debug(ex);
			}
		}
	}
}
