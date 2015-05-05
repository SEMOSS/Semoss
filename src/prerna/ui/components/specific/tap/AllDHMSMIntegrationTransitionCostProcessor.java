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
package prerna.ui.components.specific.tap;

import java.util.HashSet;

import prerna.engine.api.IEngine;
import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;

public class AllDHMSMIntegrationTransitionCostProcessor {

	public void runAllReports() throws EngineException, FileReaderException{
		IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		if(hrCore==null) {
				throw new EngineException("Database not found");
		}
		
		HashSet<String> lpiSystemList = DHMSMTransitionUtility.runRawVarListQuery(hrCore, DHMSMTransitionUtility.LPI_SYS_QUERY);
		DHMSMIntegrationTransitionCostWriter writer = new DHMSMIntegrationTransitionCostWriter();
		for(String sysURI: lpiSystemList) {
			writer.setSysURI(sysURI);
			writer.calculateValuesForReport();
			writer.writeToExcel();
		}
	}
}
