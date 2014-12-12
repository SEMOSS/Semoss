package prerna.ui.components.specific.tap;

import java.util.ArrayList;

import prerna.error.FileReaderException;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Utility;

public class DHMSMIntegrationSavingsBySystemFromWorksheetPerYearPlaySheet extends GridPlaySheet{
	@Override
	public void createData() {
		DHMSMIntegrationSavingsPerYearFromWkSht reader = new DHMSMIntegrationSavingsPerYearFromWkSht();
		DHMSMIntegrationSavingsPerFiscalYearProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		
		processor.runSupportQueries();
		
		try {
			reader.read();
			ArrayList<String> systems = reader.getSystems();
			processor.runMainQueryFromWorksheetList(systems);
			processor.processSystemData();
			list = processor.getList();
			names = processor.getNames();
		} catch (FileReaderException e) {
			Utility.showError(e.getMessage());
			e.printStackTrace();
		}
	}
}
