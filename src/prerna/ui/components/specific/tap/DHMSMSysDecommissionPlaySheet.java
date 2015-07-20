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

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class DHMSMSysDecommissionPlaySheet extends BrowserPlaySheet{

	private DHMSMSysDecommissionReport data;
	
	List<Object[]> list;
	String[] names;
	
	@Override
	public List<Object[]> getTabularData() {
		return this.list;
	}
	
	@Override
	public String[] getNames() {
		return this.names;
	}
	
	//TODO: this class should not extend BrowserPlaySheet and should create new methods to send information to JS
	public void setData(DHMSMSysDecommissionReport data) {
		this.data = data;
		this.list = new ArrayList<Object[]>();
		this.list.add(new Object[1]);
		this.names = new String[1];
	}

	/**
	 * Constructor for DHMSMSysDecommissionPlaySheet.
	 */
	public DHMSMSysDecommissionPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/world-map-timeline.html";
	}
	
	@Override
	public void createData()
	{
		if(data == null || data.masterHash == null)
		{
			data = new DHMSMSysDecommissionReport();
			data.runCalculation();
		}
		
		DHMSMSysDecommissionDataProcessing dataProcessing = new DHMSMSysDecommissionDataProcessing();
		dataProcessing.setDataSource(data);
		dataProcessing.setAllData(data.masterHash);
		dataProcessing.decomposeData();
		Hashtable<Integer, Object> loeHash = dataProcessing.constructHash();
		
		Hashtable allHash = new Hashtable();
		allHash.put("data", loeHash);
		allHash.put("label", "cost");
		this.dataHash = allHash;
	}
}
