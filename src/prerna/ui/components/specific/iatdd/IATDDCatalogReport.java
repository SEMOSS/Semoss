package prerna.ui.components.specific.iatdd;

import java.util.ArrayList;
import java.util.HashMap;

import prerna.error.EngineException;
import prerna.poi.specific.IATDDReportWriter;
import prerna.poi.specific.IndividualSystemTransitionReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.ui.components.specific.tap.LPInterfaceProcessor;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class IATDDCatalogReport extends BasicProcessingPlaySheet {

	private IEngine IATDD_DB;
	
	private String selectedParam = "";

	@Override
	public void createData() {		
		try {
			IATDD_DB = (IEngine) DIHelper.getInstance().getLocalProp("IATDD_DB");
			if (IATDD_DB == null)
				throw new EngineException("Database not found");
		} catch (EngineException e) {
			Utility.showError("Could not find necessary database: IATDD_DB. Cannot generate report.");
			return;
		}
		
		super.createData();
		//get the data from the query and send it to the writer
		HashMap<String,Object> catalogData = new HashMap<String,Object>();
		catalogData.put("data", list);
		boolean success = writeReport(catalogData, names);

		if (success) {
			Utility.showMessage("System Export Finished! File located in:\n" + IATDDReportWriter.getFileLoc());
		} else {
			Utility.showError("Error Creating Report!");
		}
	}

	private boolean writeReport(HashMap<String,Object> result, String[] headers) {
		IATDDReportWriter writer = new IATDDReportWriter();
		String templateFileName = "RD_Catalog_Template.xlsx";

		selectedParam = query.substring(query.indexOf("'") + 1, query.lastIndexOf("'"));
		writer.makeWorkbook(selectedParam, templateFileName);
		writer.writeCatalogSheet("Catalog", result, headers);

		return writer.writeWorkbook();
	}
	
	public void createView() {}

}
