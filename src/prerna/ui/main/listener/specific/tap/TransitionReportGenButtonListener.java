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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.PfTapGenericServicesCalculator;
import prerna.ui.components.specific.tap.PfTapSystemSpecificCalculator;
import prerna.ui.components.specific.tap.TAPGenericServicesCalculator;
import prerna.ui.components.specific.tap.TAPSystemSpecificCalculator;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

//TODO: have query processsing in separate file

/**
 * Produces the transition cost report within SEMOSS based on user inputs
 * Runs when the user presses transitionReportGenButton
 */
public class TransitionReportGenButtonListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(TransitionReportGenButtonListener.class.getName());
	ArrayList queryArray = new ArrayList();
	
	/**
	 * Creates transition cost report based on the format, type, costs to view, and optionally a specific system the user selects
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		JComboBox reportFormat = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_REPORT_FORMAT_COMBO_BOX);
		String format = (String) reportFormat.getSelectedItem();
		JComboBox reportType = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_REPORT_TYPE_COMBO_BOX);
		String type = (String) reportType.getSelectedItem();
		String system = null;
		boolean dataFed = false;
		boolean dataConsumer = false;
		boolean bluProvider = false;
		boolean dataGeneric = false;
		boolean bluGeneric = false;
		
		//now I need to fill the types array so that I can set that before beginning the calculation
		String runTypes = null;
		boolean semantics = true;
		boolean serviceSpecific = false;
		JToggleButton serviceBtn = (JToggleButton) DIHelper.getInstance().getLocalProp(Constants.SERVICE_SELECTION_BUTTON);
		queryArray.clear();
		if(serviceBtn.isSelected())
		{
			serviceSpecific=true;
		}
		//if report type is specific, get datafed, consumer and system.  Otherwise, just go through the if statements.
		if(type.contains("Specific")){
			ParamComboBox systemComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_REPORT_COMBO_BOX);
			system = (String) systemComboBox.getSelectedItem();
			JCheckBox dataFedCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_FED);
			JCheckBox dataConsumerCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_CONSUMER);
			JCheckBox bluProviderCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_BLU_PROVIDER);

			dataFed = dataFedCheck.isSelected();
			dataConsumer = dataConsumerCheck.isSelected();
			bluProvider = bluProviderCheck.isSelected();
			
			//now for the enormous amount of if statements to put the exactly correct query on this bad boy

			if(dataFed)
			{
				queryArray.add(Constants.TRANSITION_DATA_FEDERATION);
			}
			if(bluProvider)
			{
				queryArray.add(Constants.TRANSITION_BLU_PROVIDER);
			}
			if(dataConsumer)
			{
				queryArray.add(Constants.TRANSITION_SPECIFIC_DATA_CONSUMER);
				queryArray.add(Constants.TRANSITION_SPECIFIC_SITE_CONSUMER);
			}
			
		}
		
		//if report type is specific, get datafed, consumer and system.  Otherwise, just go through the if statements.
		else if(type.contains("Generic")){
			ParamComboBox systemComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_REPORT_COMBO_BOX);
			system = (String) systemComboBox.getSelectedItem();
			JCheckBox dataGenericCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_GENERIC);
			JCheckBox bluGenericCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_BLU_GENERIC);
			dataGeneric = dataGenericCheck.isSelected();
			bluGeneric = bluGenericCheck.isSelected();
			
			//now to set the query type
			if(dataGeneric)
			{

				queryArray.add(Constants.TRANSITION_GENERIC_DATA);
			}
			else if (bluGeneric)
			{
				queryArray.add(Constants.TRANSITION_GENERIC_BLU);
			}
		}

		if(queryArray.isEmpty()) {
			Utility.showError("Please select a check box specifying which costs to view \nand regenerate the report");
			return;
		}
		
		//System Printouts
		//String[] sysList = {"AHLTA", "AHLTA-M", "AHLTA-T", "CCQAS", "CHCS", "CHDR", "CIS-Essentris", "BHIE", 
		//		"DMLSS(DCAM)", "DMLSS(JMAR)", "DMLSS", "EBMS-BDMS", "EBMS-BMBB-TS", "eForms", "ESSENCE", "FHIE", "HAIMS", "MEB", "MEDWEB", "MMM", 
		//		"MSAT", "NMIS", "SAMS", "TC2", "TEWLS", "TMDS", "TOL"};
		
		//DHSS Systems
		//String[] sysList = {"AHLTA", "AHLTA-M", "AHLTA-T", "BHIE", "CHCS", "CHDR", "CIS-Essentris",  "EBMS-BDMS", "EBMS-BMBB-TS", 
		//		"eForms", "FHIE", "HAIMS", "MEDWEB", "MMM", "MSAT", "NCAT", "SAMS", "TC2", "TMDS"};
		//DHIMS Systems
		//String[] sysList = {"CCE", "CCQAS", "DMHRSi", "DMHRSi(EWPD)", "DMLSS", "DMLSS(DCAM)", "DMLSS(JMAR)", "DOEHRS-IH", 
		//		"DOEHRS-HC", "EAS_IV", "ESSENCE", "iAS", "M2", "MDR", "MHS_Learn", 
		//		"NMIS", "PEPR", "PMITS", "PSR", "SNPMIS", "TED", "TEWLS", "TOL"};
		
		//ALL SYSTEMS
		//String[] sysList = {"AHLTA", "AHLTA-M", "AHLTA-T", "BHIE", "CHCS", "CHDR", "CIS-Essentris",  "EBMS-BDMS", "EBMS-BMBB-TS", 
		//		"eForms", "FHIE", "HAIMS", "MEDWEB", "MMM", "MSAT", "SAMS", "TC2", "TMDS",
		//		"CCE", "CCQAS", "DMHRSi", "DMLSS", "DMLSS(DCAM)", "DMLSS(JMAR)", "DOEHRS-IH", 
		//		"DOEHRS-HC", "EAS_IV", "ESSENCE", "iAS", "M2", "MDR", "MHS_Learn", 
		//	"NMIS", "PEPR", "PMITS", "PSR", "SNPMIS", "TED", "TEWLS", "TOL"};
		
		//Army Systems
		//String[] sysList = {"AWCTS", "AERO", "MODS", "WMSNi", "DoDTR_(JTTR)"};
		
		//Selected System
		String[] sysList = {system};
		
		//now lets see what was selected and go to the correct class
		if (format.contains("TAP") && type.contains("Specific"))
		{
			TAPSystemSpecificCalculator tapCalc = new TAPSystemSpecificCalculator();
			tapCalc.setSemantics(semantics);
			tapCalc.setServiceBoolean(serviceSpecific);
			tapCalc.setTypes(queryArray);
			tapCalc.processData(sysList);
		}
		else if (format.contains("TAP") && type.contains("Generic"))
		{
			TAPGenericServicesCalculator tapCalc = new TAPGenericServicesCalculator();
			tapCalc.setServiceBoolean(serviceSpecific);
			tapCalc.setTypes(queryArray);
			tapCalc.processData("Generic");
		}
		else if (format.contains("ProSite") && type.contains("Specific"))
		{
			PfTapSystemSpecificCalculator tapCalc = new PfTapSystemSpecificCalculator();
			tapCalc.setSemantics(semantics);
			tapCalc.setServiceBoolean(serviceSpecific);
			tapCalc.setTypes(queryArray);
			tapCalc.processData(sysList);
		}
		else if (format.contains("ProSite") && type.contains("Generic"))
		{
			PfTapGenericServicesCalculator tapCalc = new PfTapGenericServicesCalculator();
			tapCalc.setServiceBoolean(serviceSpecific);
			tapCalc.setTypes(queryArray);
			tapCalc.processData("Generic");
		}

		logger.info("Transition Report Generator Button Pushed");

	}

	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}
}
