package prerna.ui.components.specific.tap;

import java.util.ArrayList;

import prerna.error.FileReaderException;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Utility;

public class DHMSMIntegrationSavingsPerYearFromWkShtPlaySheet extends GridPlaySheet{

	@Override
	public void createData() {
		DHMSMIntegrationSavingsPerYearFromWkSht reader = new DHMSMIntegrationSavingsPerYearFromWkSht();
		DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor();
		ArrayList<String> systems;
		
		processor.runSupportQueries();
		
		try {
			reader.read();
			systems = reader.getSystems();
			processor.runMainQueryFromWorksheetList(systems);
			processor.processData();
			list = processor.getList();
			names = processor.getNames();
		} catch (FileReaderException e) {
			Utility.showError(e.getMessage());
			e.printStackTrace();
		}		
	}
}
