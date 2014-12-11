package prerna.ui.components.specific.tap;

import prerna.ui.components.playsheets.GridPlaySheet;

public class DHMSMIntegrationSavingsBySelectedSystemPlaySheet extends GridPlaySheet{
	@Override
	public void createData(){
		DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor();
		processor.runSupportQueries();
		if(query.equalsIgnoreCase("None")) {
			processor.runMainQuery("");
		} else {
			processor.runMainQuery(query);
		}
		processor.processSystemData();
		list = processor.getList();
		names = processor.getNames();
	}
}