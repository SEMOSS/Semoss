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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.swing.JDesktopPane;

import prerna.engine.api.IEngine;
import prerna.error.EngineException;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class LPInterfaceReportGenerator extends GridPlaySheet {

	private LPInterfaceProcessor processor;
	private IEngine tapCostData;
	private IEngine hrCore;
	public void setProcessor(LPInterfaceProcessor processor) {
		this.processor = processor;
	}
	
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
	
	/**
	 * This is the function that is used to create the first view 
	 * of any play sheet.  It often uses a lot of the variables previously set on the play sheet, such as {@link #setQuery(String)},
	 * {@link #setJDesktopPane(JDesktopPane)}, {@link #setRDFEngine(IEngine)}, and {@link #setTitle(String)} so that the play 
	 * sheet is displayed correctly when the view is first created.  It generally creates the model for visualization from 
	 * the specified engine, then creates the visualization, and finally displays it on the specified desktop pane
	 * 
	 * <p>This is the function called by the PlaysheetCreateRunner.  PlaysheetCreateRunner is the runner used whenever a play 
	 * sheet is to first be created, most notably in ProcessQueryListener.
	 */
	@Override
	public void createData() {

		list = new ArrayList<Object[]>();
		
		if(processor == null) {
			processor = new LPInterfaceProcessor();
		}
		processor.setEngine(engine);
		
		list = processor.generateReport();
		names = processor.getNames();
		
	}
	
	// requires the cost information to already be created and set in the processor
	public HashMap<String, Object> getSysInterfaceWithCostData(String systemName, String reportType) throws EngineException {
		if(tapCostData == null) {
			tapCostData = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		}
		if(hrCore == null) {
			hrCore = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		}
		if(processor == null) {
			processor = new LPInterfaceProcessor();
		}
		
		systemName = systemName.replaceAll("\\(", "\\\\\\\\\\(").replaceAll("\\)", "\\\\\\\\\\)");
		processor.setDownstreamQuery(DHMSMTransitionUtility.lpSystemDownstreamInterfaceQuery.replace("@SYSTEMNAME@", systemName));
		processor.setUpstreamQuery(DHMSMTransitionUtility.lpSystemUpstreamInterfaceQuery.replace("@SYSTEMNAME@", systemName));
		
		processor.isGenerateCost(true);
		processor.setEngine(hrCore);
		processor.getCostInfo(tapCostData);
		ArrayList<Object[]> newData = processor.generateReport();
		
		HashMap<String, Object> dataHash = new HashMap<String, Object>();
		String[] oldHeaders = processor.getNames();
		String[] newHeaders = new String[oldHeaders.length + 3];
		for(int i = 0; i < oldHeaders.length; i++)
		{
			if(i < oldHeaders.length - 1) {
				newHeaders[i] = oldHeaders[i];
			} else {
				newHeaders[i] = "Services";
				newHeaders[i+1] = "Recommendation";
				newHeaders[i+2] = "Direct Cost";
				newHeaders[i+3] = "Indirect Cost";
			}
		}
		
		dataHash.put(DHMSMTransitionUtility.DATA_KEY, DHMSMTransitionUtility.removeSystemFromArrayList(newData));
		dataHash.put(DHMSMTransitionUtility.HEADER_KEY, DHMSMTransitionUtility.removeSystemFromStringArray(newHeaders));
		dataHash.put(DHMSMTransitionUtility.TOTAL_DIRECT_COST_KEY, processor.getTotalDirectCost());
		dataHash.put(DHMSMTransitionUtility.TOTAL_INDIRECT_COST_KEY, processor.getTotalIndirectCost());
		
		return dataHash;
	}
}
