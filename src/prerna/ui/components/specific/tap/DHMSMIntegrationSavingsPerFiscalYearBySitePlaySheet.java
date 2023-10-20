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

import java.util.List;

import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Utility;

public class DHMSMIntegrationSavingsPerFiscalYearBySitePlaySheet extends GridPlaySheet {

	List<Object[]> list;
	String[] names;
	
	@Override
	public List<Object[]> getList() {
		return this.list;
	}
	
	@Override
	public String[] getNames() {
		return this.names;
	}
	
	@Override
	public void createData() {
		DHMSMIntegrationSavingsPerFiscalYearProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		boolean success = true;
		try {
			processor.runSupportQueries("TAP_Portfolio","TAP_Site_Data","TAP_Core_Data");
		} catch(NullPointerException ex) {
			success = false;
			Utility.showError(ex.getMessage());
		}
		
		if(success) {
			if(query.equalsIgnoreCase("None")) {
				processor.runMainQuery("");
			} else {
				processor.runMainQuery(query);
			}
			processor.generateSavingsData();
			processor.processSiteData();
			list = processor.getSiteOutputList();
			names = processor.getSiteNames();
		}
	}
	
}
