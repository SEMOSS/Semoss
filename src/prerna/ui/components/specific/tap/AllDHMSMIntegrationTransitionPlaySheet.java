package prerna.ui.components.specific.tap;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class AllDHMSMIntegrationTransitionPlaySheet extends BasicProcessingPlaySheet{

	@Override
	public void createView() {
		Utility.showMessage("Success! Created all LPI reports.");
	}
	
	@Override
	public void createData() {
		AllDHMSMIntegrationTransitionCostProcessor writer;
		try {
			writer = new AllDHMSMIntegrationTransitionCostProcessor();
			writer.runAllReports();
		} catch (EngineException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		} catch (FileReaderException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		}
	}

}
