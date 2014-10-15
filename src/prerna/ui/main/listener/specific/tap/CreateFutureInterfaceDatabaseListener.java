package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.CreateFutureStateDHMSMDatabase;
import prerna.ui.components.specific.tap.GLItemGeneratorSelfReportedFutureInterfaces;
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
		
		JComboBox<String> futureStateCostDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_FUTURE_COST_INTERFACE_DATABASE_COMBO_BOX);
		String futureCostDBName = futureStateCostDBComboBox.getSelectedItem() + "";

		//get associated engines
		IEngine hrCoreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDBName);
		IEngine futureDB = (IEngine) DIHelper.getInstance().getLocalProp(futureDBName);
		IEngine futureCostDB = (IEngine) DIHelper.getInstance().getLocalProp(futureCostDBName);
		
		//send to processor
		LOGGER.info("Creating " + futureDBName + " from " + hrCoreDBName);
		LOGGER.info("Creating " + futureCostDBName + " from " + hrCoreDBName);
		
		try {
			
			IEngine tapCost = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
			if(tapCost == null) {
				throw new EngineException("Cost Info Not Found");
			}
			
			GLItemGeneratorSelfReportedFutureInterfaces glGen = new GLItemGeneratorSelfReportedFutureInterfaces(hrCoreDB, futureDB, futureCostDB);
			glGen.genData();
			
			CreateFutureStateDHMSMDatabase futureStateCreator = new CreateFutureStateDHMSMDatabase(hrCoreDB, futureDB, futureCostDB, tapCost);
			futureStateCreator.addTriplesToExistingICDs();
			futureStateCreator.generateData();
			futureStateCreator.createDBs();
			Utility.showMessage("Finished adding triples to " + futureDBName + " and " + futureCostDBName);
		} catch (EngineException e) {
			Utility.showError("Error with generting new DB. Make sure DB's are properly defined.");
			e.printStackTrace();
		} 
		catch (RepositoryException e) {
			Utility.showError("Error with generting new DB");
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			Utility.showError("Error with generting new DB");
			e.printStackTrace();
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub

	}

}
