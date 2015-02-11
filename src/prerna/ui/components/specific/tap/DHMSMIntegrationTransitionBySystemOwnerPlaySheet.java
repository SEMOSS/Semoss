/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package prerna.ui.components.specific.tap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class DHMSMIntegrationTransitionBySystemOwnerPlaySheet extends BasicProcessingPlaySheet {
	
	private static final Logger logger = LogManager.getLogger(DHMSMIntegrationTransitionBySystemOwnerPlaySheet.class.getName());

	private String lpiSysQuery = "SELECT DISTINCT ?sys WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?sys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?sys <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?sys <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' } {?sys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} BIND(<@SYS_OWNER@> AS ?owner) {?sys <http://semoss.org/ontologies/Relation/OwnedBy> ?owner} } ORDER BY ?sys BINDINGS ?Probability {('Medium')('Low')('Medium-High')}";
	
	@Override
	public void createData() {
		
		int counter = 0;
		String sysOwner = this.query;
		lpiSysQuery = lpiSysQuery.replace("@SYS_OWNER@", sysOwner);
		IEngine HR_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		if(HR_Core == null) {
			Utility.showError("Could not find HR_Core database.\nPlease load the appropriate database to produce report");
		}
		
		DHMSMIntegrationTransitionCostWriter generateData = null;
		try {
			generateData = new DHMSMIntegrationTransitionCostWriter();
		} catch (EngineException e1) {
			e1.printStackTrace();
		} 
		DHMSMIntegrationTransitionBySystemOwnerWriter writer = new DHMSMIntegrationTransitionBySystemOwnerWriter();
		
		ISelectWrapper sjsw = Utility.processQuery(HR_Core, lpiSysQuery);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String sysURI = sjss.getRawVar(names[0]).toString();
			try {
				generateData.setSysURI(sysURI);
				generateData.calculateValuesForReport();
				writer.setDataSource(generateData);
				writer.write(counter);
			} catch (EngineException e) {
				e.printStackTrace();
				Utility.showError(e.getMessage());
			} catch (FileReaderException e) {
				Utility.showError(e.getMessage());
				e.printStackTrace();
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
