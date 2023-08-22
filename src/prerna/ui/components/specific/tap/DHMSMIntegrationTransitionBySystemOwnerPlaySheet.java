/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

package prerna.ui.components.specific.tap;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class DHMSMIntegrationTransitionBySystemOwnerPlaySheet extends TablePlaySheet {
	private static final Logger logger = LogManager.getLogger(DHMSMIntegrationTransitionBySystemOwnerPlaySheet.class);

	private static final String STACKTRACE = "StackTrace: ";
	private String lpiSysQuery = "SELECT DISTINCT ?sys WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?sys <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI'} {?sys <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?sys <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) BIND(<@SYS_OWNER@> AS ?owner) {?sys <http://semoss.org/ontologies/Relation/OwnedBy> ?owner} } ORDER BY ?sys";

	@Override
	public void createData() {
		int counter = 0;
		String sysOwner = this.query;
		lpiSysQuery = lpiSysQuery.replace("@SYS_OWNER@", sysOwner);
		IDatabaseEngine tapCoreData = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		if (tapCoreData == null) {
			Utility.showError(
					"Could not find TAP_Core_Data database.\nPlease load the appropriate database to produce report");
		}

		DHMSMIntegrationTransitionCostWriter generateData = null;
		try {
			generateData = new DHMSMIntegrationTransitionCostWriter();
		} catch (IOException e1) {
			logger.error(STACKTRACE, e1);
		}
		DHMSMIntegrationTransitionBySystemOwnerWriter writer = new DHMSMIntegrationTransitionBySystemOwnerWriter();

		ISelectWrapper sjsw = Utility.processQuery(tapCoreData, lpiSysQuery);
		String[] names = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String sysURI = sjss.getRawVar(names[0]).toString();
			try {
				if (generateData != null) {
					generateData.setSysURI(sysURI);
					generateData.calculateValuesForReport();
					writer.setDataSource(generateData);
					writer.write(counter);
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
				Utility.showError(e.getMessage());
			}
			counter++;
		}

		writer.writeFile(Utility.getInstanceName(sysOwner));
	}

	@Override
	public void createView() {
		Utility.showMessage("Success!");
	}
}
