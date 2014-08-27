package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.CreateFutureStateDHMSMDatabase;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CreateFutureInterfaceDatabaseListener extends AbstractListener{

	static final Logger LOGGER = LogManager.getLogger(CreateFutureInterfaceDatabaseListener.class.getName());

	@Override
	public void actionPerformed(ActionEvent arg0) {
		//get selected values
		JComboBox<String> hrCoreDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.HR_CORE_FUTURE_INTERFACE_DATABASE_CORE_COMBO_BOX);
		String hrCoreDBName = hrCoreDBComboBox.getSelectedItem() + "";
		
		JComboBox<String> futureStateDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_FUTURE_INTERFACE_DATABASE_COMBO_BOX);
		String futureDBName = futureStateDBComboBox.getSelectedItem() + "";

		//get associated engines
		IEngine hrCoreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDBName);
		IEngine futureDB = (IEngine) DIHelper.getInstance().getLocalProp(futureDBName);

		//send to processor
		LOGGER.info("Creating " + futureDBName + " from " + hrCoreDBName);

		CreateFutureStateDHMSMDatabase futureStateCreator = new CreateFutureStateDHMSMDatabase(hrCoreDB, futureDB);
		try {
			futureStateCreator.generateData();
			futureStateCreator.createNewDB();
		
		} catch (EngineException e) {
			Utility.showError("Error with generting new DB.  Make sure DB's are properly defined.");
			e.printStackTrace();
		}
		
//		boolean success = sap.runFullAggregation();
//		if(success)
//		{
//			Utility.showMessage("Finished Aggregation!");
//		}
//		else
//		{
//			Utility.showError("Please Check Error Report for Possible Issues with Data Quality");
//		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub

	}

}
