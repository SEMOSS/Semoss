package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.io.File;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
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
		
		File sourceFolder = new File(selectedFormsFolder);
		if(!sourceFolder.isDirectory()) {
			Utility.showError("Source folder entered is not valid!");
			return;
		}
		
		LOGGER.info("SELECTED DATABASE ::: " + selectedFormsDb);
		LOGGER.info("SELECTED FOLDER ::: " + sourceFolder);

		IEngine selectedEngine = Utility.getEngine(selectedFormsDb);
		
		// TODO: CREATE NEW CLASS THAT TAKES IN ENGINE AND SOURCE FOLDER THAT DOES PROCESSING
//		below is for example
//		make NewClass in package prerna.ui.components.specific.tap
//		NewClass x = new NewClass(selectedEngine, sourceFolder);
//		x.processSourceFiles();
//		String workbook1Path = sourceFolder + "/SourceFile1Name.xlsx";
		
		Utility.showMessage("Finished Consolidating Source Files");
	}

	@Override
	public void setView(JComponent view) {
		// do nothing
	}

}
