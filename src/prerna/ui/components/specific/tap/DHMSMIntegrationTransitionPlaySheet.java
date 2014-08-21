package prerna.ui.components.specific.tap;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class DHMSMIntegrationTransitionPlaySheet  extends BasicProcessingPlaySheet{

	@Override
	public void createView() {
		Utility.showMessage("Success!");
	}
	
	@Override
	public void createData() {
		String systemURI = this.query;
		DHMSMIntegrationTransitionCostWriter writer;
		try {
			writer = new DHMSMIntegrationTransitionCostWriter();
			writer.setSysURI(systemURI);
			writer.calculateValuesForReport();
			writer.writeToExcel();
		} catch (EngineException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		} catch (FileReaderException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		}
	}	
	
}
