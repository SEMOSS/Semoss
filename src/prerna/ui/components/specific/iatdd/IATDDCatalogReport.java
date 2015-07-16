/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.iatdd;

import java.util.HashMap;

import prerna.engine.api.IEngine;
import prerna.error.EngineException;
import prerna.poi.specific.IATDDReportWriter;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
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
		catalogData.put("data", dataFrame.getData());
		boolean success = writeReport(catalogData, dataFrame.getColumnHeaders());

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
