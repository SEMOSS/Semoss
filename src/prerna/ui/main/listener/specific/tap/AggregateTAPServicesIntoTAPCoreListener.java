package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextField;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.ServicesAggregationProcessor;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

public class AggregateTAPServicesIntoTAPCoreListener extends AbstractListener{

	Logger logger = Logger.getLogger(getClass());
	
	@Override
	public void actionPerformed(ActionEvent arg0) {
		//get selected values
		JComboBox<String> servicesDbComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_SERVICES_AGGREGATION_SERVICE_COMBO_BOX);
		String servicesDbName = servicesDbComboBox.getSelectedItem() + "";
		JComboBox<String> coreDbComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_SERVICES_AGGREGATION_CORE_COMBO_BOX);
		String coreDbName = coreDbComboBox.getSelectedItem() + "";
		String baseURI = ((JTextField)DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_SERVICES_AGGREGATION_BASE_URI)).getText();
		
		//get associated engines
		IEngine servicesDB = (IEngine) DIHelper.getInstance().getLocalProp(servicesDbName);
		IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(coreDbName);
		
		//send to processor
		logger.info("Aggregating " + servicesDbName + " into " + coreDbName + "with base URI " + baseURI);
		ServicesAggregationProcessor sap = new ServicesAggregationProcessor(servicesDB, coreDB, baseURI);
		
		
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
