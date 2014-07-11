package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import org.apache.log4j.Logger;

import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.specific.tap.SORpropInsertProcessor;
import prerna.ui.components.specific.tap.SysBPCapInsertProcessor;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SORpropInsertListener  extends AbstractListener {

	Logger logger = Logger.getLogger(getClass());	
	
	@Override
	public void actionPerformed(ActionEvent arg0) {
		//get the selected engine name
		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		String engineName = (String)list.getSelectedValue();
		engineName = "TAP_Core_Data";
						
		//send to processor
		logger.info("Inserting SOR properties for System-Data into " + engineName + "...");
		boolean success = false;
		String errorMessage = "";
		String isCalculatedQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?o <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?s ?p ?o ;} BIND(<http://semoss.org/ontologies/Relation/Contains/Calculated> AS ?contains) {?p ?contains ?prop ;} {?p <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} }";
		BooleanProcessor proc = new BooleanProcessor();
		proc.setQuery(isCalculatedQuery);
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		boolean isCalculated = proc.processQuery();
				
		if(isCalculated){	
			Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store (" + engineName + ") already " +
					"contains calculated relationships.  Would you like to recalculate?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
			
			if (response == 1) {
				SORpropInsertProcessor insertProcessor = new SORpropInsertProcessor();
				insertProcessor.setInsertCoreDB(engineName);
				//insertProcessor.runDeleteQueries();
				success = insertProcessor.runCoreInsert();
				errorMessage = insertProcessor.getErrorMessage();
				if (!(errorMessage == "")) {
					Utility.showError(errorMessage);
				}
			}
			else return;
		}
		else {
			SORpropInsertProcessor insertProcessor = new SORpropInsertProcessor();
			insertProcessor.setInsertCoreDB(engineName);
			success = insertProcessor.runCoreInsert();
			errorMessage = insertProcessor.getErrorMessage();
			if (!(errorMessage == "")) {
				Utility.showError(errorMessage);
			}
		}
		
		if(success)	{
			logger.info("Completed Insert.");
			Utility.showMessage("Insert Completed!");			
		}
	}
	
	@Override
	public void setView(JComponent view) {
			
	}

}
