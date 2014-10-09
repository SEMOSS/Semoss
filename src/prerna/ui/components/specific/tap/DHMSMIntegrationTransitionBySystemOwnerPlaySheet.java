/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package prerna.ui.components.specific.tap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class DHMSMIntegrationTransitionBySystemOwnerPlaySheet extends BasicProcessingPlaySheet {
	
	private static final Logger logger = LogManager.getLogger(DHMSMIntegrationTransitionBySystemOwnerPlaySheet.class.getName());

	@Override
	public void createData() {
		
		int counter = 1;
		String sysOwnerQuery = query;
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
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(HR_Core, sysOwnerQuery);
		String[] names = sjsw.getVariables();
		
		
		while(sjsw.hasNext()){
			SesameJenaSelectStatement sjss = sjsw.next();
			String sysURI = sjss.getRawVar(names[0]).toString();
			
			try {
				generateData.setSysURI(sysURI);
				generateData.calculateValuesForReport();
				writer.setDataSource(generateData);
				writer.setSysURI(sysURI);
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
		
		writer.writeFile("HERE");
	}
	
	@Override
	public void createView() {
		Utility.showMessage("Success!");
	}
}
