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
import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.logging.Level;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.specific.tap.CapabilityFactSheetListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.LoggerProvider;
import com.teamdev.jxbrowser.chromium.events.FinishLoadingEvent;
import com.teamdev.jxbrowser.chromium.events.LoadAdapter;

/**
 * This class creates the capability fact sheet.
 */
public class CapabilityFactSheet extends BrowserPlaySheet{

	Hashtable allHash = new Hashtable();
	Hashtable<String, ArrayList<String>> capabilityHash = new Hashtable<String, ArrayList<String>>();
	//keys are processed capabilities and values are semoss stored capabilities
	public Hashtable<String,String> capabilityProcessed = new Hashtable<String,String>();
	
	CapabilityFactSheetListener singleCapFactSheetCall = new CapabilityFactSheetListener();
	
	/**
	 * Constructor for CapabilityFactSheet. Generates the landing page for the capability fact sheets.
	 */
	public CapabilityFactSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		 LoggerProvider.getBrowserLogger().setLevel(Level.OFF);
		 LoggerProvider.getIPCLogger().setLevel(Level.OFF);
		 LoggerProvider.getChromiumProcessLogger().setLevel(Level.OFF);
	}

	/**
	 * Processes all capability Similarity queries and shows results in sysdupe.html format.
	 */
	@Override
	public void createView()
	{		
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		singleCapFactSheetCall.setCapabilityFactSheet(this);
		//browser.navigate("file://" + workingDir + "/html/MHS-FactSheets/Capability Fact Sheet.html");
		//singleCapFactSheetCall.invoke(null);
		browser.registerFunction("singleCapFactSheet",  singleCapFactSheetCall);
		browser.addLoadListener(new LoadAdapter() {

    	    public void onFinishLoadingFrame(FinishLoadingEvent event) {

	   	    	File file = new File(DIHelper.getInstance().getProperty("BaseFolder") + "/html/MHS-FactSheets/export.json");
	   			if(file.exists()) {
	   				file.delete();
	   				browser.executeJavaScript("window.location.reload(true);");
	   			}
    			callIt();
    	    }

    	});
	       
		browser.loadURL("file://" + workingDir + "/html/MHS-FactSheets/index.html");
		
		
	}

	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable Includes the data series.*/
	public Hashtable<String, ArrayList<String>> processQueryData()
	{
		addPanel();
		ArrayList<String> dataArrayList = new ArrayList<String>();
		String[] var = wrapper.getVariables();
		for (int i=0; i<list.size(); i++)
		{	
			Object[] listElement = list.get(i);
		//	for (int j = 0; j < var.length; j++) 
		//	{	
					String text = (String) listElement[0];
					String source = (String) listElement[1];
					String processedText = text.replaceAll("\\[", "(").replaceAll("\\]", ")").replaceAll(",", "").replaceAll("&", "").replaceAll("\'","").replaceAll("’", "")+"_("+source+")";
					capabilityProcessed.put(processedText,text);
					dataArrayList.add(processedText);
		//	}			
		}

		capabilityHash.put("dataSeries", dataArrayList);
		
		return capabilityHash;
	}
	
	public Hashtable processNewCapability(String capability)
	{		
		CapabilityFactSheetPerformer performer = new CapabilityFactSheetPerformer();
		Hashtable<String,Object> dataSeries = new Hashtable<String,Object>();

		updateProgressBar("10%...Processing Capability Similarity", 10);
		Hashtable<String, Object> capabilitySimSheetHash = performer.processCapabilitySimSheet(capability);
		dataSeries.put("CapabilitySimSheet", capabilitySimSheetHash);
		
		updateProgressBar("25%...Processing Data Objects", 25);
		Hashtable<String, Object> dataSheet = performer.processDataSheetQueries(capability);
		dataSeries.put("DataSheet", dataSheet);
		
		updateProgressBar("30%...Processing Systems", 30);
		Hashtable<String, Object> systemSheet = performer.processSystemQueries(capability);
		dataSeries.put("SystemSheet", systemSheet);
		
		updateProgressBar("60%...Processing Tasks and BPs", 60);
		Hashtable<String, Object> taskAndBPSheetHash = performer.processTaskandBPQueries(capability);
		dataSeries.put("TaskAndBPSheet", taskAndBPSheetHash);
		
		updateProgressBar("65%...Processing Requirements and Standards", 65);
		Hashtable<String, Object> reqAndStandardSheet = performer.processRequirementsAndStandardsQueries(capability);
		dataSeries.put("ReqAndStandardSheet", reqAndStandardSheet);
		
		updateProgressBar("70%...Processing BLUs", 70);
		Hashtable<String, Object> bluSheet = performer.processBLUSheetQueries(capability);
		dataSeries.put("BLUSheet", bluSheet);
		
		updateProgressBar("75%...Processing FunctionalGaps", 75);
		Hashtable<String, Object> funtionalGapSheet = performer.processFunctionalGapSheetQueries(capability);
		dataSeries.put("FunctionalGapSheet", funtionalGapSheet);
		
		updateProgressBar("80%...Processing Capability Overview", 80);
		Hashtable<String, Object> firstSheetHash = performer.processFirstSheetQueries(capability);
		dataSeries.put("CapabilityOverviewSheet", firstSheetHash);

		allHash.put("dataSeries", dataSeries);
		allHash.put("capability", capability);

		callItAllHash();
		updateProgressBar("100%...Capability Fact Sheet Generation Complete", 100);
		return allHash;
	}
	
	public void callIt()
	{
		System.err.println(">>> callIt");
		Gson gson = new Gson();
//		browser.executeScript("capabilityList('" + gson.toJson(capabilityHash) + "');");
//		String json = gson.toJson(capabilityHash);
		browser.executeJavaScript("start('" + gson.toJson(capabilityHash) + "');");
		System.out.println(gson.toJson(capabilityHash));
	}
	
	public void callItAllHash()
	{
		System.err.println(">>> callItAllHash");
		Gson gson = new Gson();
//		browser.executeScript("capabilityData('" + gson.toJson(allHash) + "');");
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		browser.loadURL("file://" + workingDir + "/html/MHS-FactSheets/index.html#/cap");
		browser.executeJavaScript("start('" + gson.toJson(allHash) + "');");
		System.out.println(gson.toJson(allHash));
	}
	
}


