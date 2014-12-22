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

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.ServicesAggregationProcessor;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AggregateTAPServicesIntoTAPCoreListener extends AbstractListener{

	static final Logger logger = LogManager.getLogger(AggregateTAPServicesIntoTAPCoreListener.class.getName());

	@Override
	public void actionPerformed(ActionEvent arg0) {
		//get selected values
		JComboBox<String> servicesDbComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_SERVICES_AGGREGATION_SERVICE_COMBO_BOX);
		String servicesDbName = servicesDbComboBox.getSelectedItem() + "";
		JComboBox<String> coreDbComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_SERVICES_AGGREGATION_CORE_COMBO_BOX);
		String coreDbName = coreDbComboBox.getSelectedItem() + "";

		//get associated engines
		IEngine servicesDB = (IEngine) DIHelper.getInstance().getLocalProp(servicesDbName);
		IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(coreDbName);

		//send to processor
		logger.info("Aggregating " + servicesDbName + " into " + coreDbName);
		ServicesAggregationProcessor sap = new ServicesAggregationProcessor(servicesDB, coreDB);

		boolean success = sap.runFullAggregation();
		if(success)
		{
			Utility.showMessage("Finished Aggregation!");
		}
		else
		{
			Utility.showError("Please Check Error Report for Possible Issues with Data Quality");
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub

	}

}
