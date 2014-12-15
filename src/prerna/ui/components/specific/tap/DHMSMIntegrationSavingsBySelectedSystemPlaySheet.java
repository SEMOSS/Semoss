package prerna.ui.components.specific.tap;

import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Utility;

public class DHMSMIntegrationSavingsBySelectedSystemPlaySheet extends GridPlaySheet{
	@Override
	public void createData(){
		DHMSMIntegrationSavingsPerFiscalYearProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		processor.runSupportQueries();
		if(query.equalsIgnoreCase("None")) {
			processor.runMainQuery("");
		} else {
			processor.runMainQuery(query);
		}
		try {
			processor.processSystemData();
		} catch(ArrayIndexOutOfBoundsException e) {
			Utility.showMessage(e.getMessage());
			e.printStackTrace();
		}
		list = processor.getList();
		names = processor.getNames();
	}
}