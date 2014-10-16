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

import prerna.ui.components.playsheets.GridPlaySheet;

public class DHMSMIntegrationSavingsPerFiscalYearBySitePlaySheet extends GridPlaySheet {

	
	@Override
	public void createData(){
		DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearBySiteProcessor();
		processor.runSupportQueries();
		if(query.equalsIgnoreCase("None")) {
			processor.runMainQuery("");
		} else {
			processor.runMainQuery(query);
		}
		processor.processData();
		list = processor.getList();
		names = processor.getNames();
	}
	
	}

