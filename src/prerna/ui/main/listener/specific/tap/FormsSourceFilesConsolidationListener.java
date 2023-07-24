package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.io.File;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextField;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabase;
import prerna.ui.components.specific.tap.forms.FormsDataProcessor;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class FormsSourceFilesConsolidationListener extends AbstractListener {

	public static final Logger LOGGER = LogManager.getLogger(FormsSourceFilesConsolidationListener.class.getName());
	
	@Override
	public void actionPerformed(ActionEvent arg0) {
		JComboBox<String> formsDbComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.FORMS_SOURCE_FILE_AGGREGATION_COMBO_BOX);
		String selectedFormsDb = (String) formsDbComboBox.getSelectedItem();
		
		JTextField formsSourceFolderTextField = (JTextField) DIHelper.getInstance().getLocalProp(ConstantsTAP.FORMS_SOURCE_FILE_DIRECTORY);
		String selectedFormsFolder = formsSourceFolderTextField.getText();
		
		File sourceFolder = new File(Utility.normalizePath(selectedFormsFolder));
		if(!sourceFolder.isDirectory()) {
			Utility.showError("Source folder entered is not valid!");
			return;
		}
		
		LOGGER.info("SELECTED DATABASE ::: " + selectedFormsDb);
		LOGGER.info("SELECTED FOLDER ::: " + Utility.cleanLogString(sourceFolder.toString()));

		IDatabase selectedEngine = Utility.getEngine(selectedFormsDb);
		
		FormsDataProcessor processor = new FormsDataProcessor();
		processor.processData(selectedEngine, sourceFolder);
		
		Utility.showMessage("Finished Consolidating Source Files");
	}

	@Override
	public void setView(JComponent view) {
		// do nothing
	}

}


