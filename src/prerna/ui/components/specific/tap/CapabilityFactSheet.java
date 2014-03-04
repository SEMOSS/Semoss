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

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.events.NavigationEvent;
import com.teamdev.jxbrowser.events.NavigationFinishedEvent;
import com.teamdev.jxbrowser.events.NavigationListener;

import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.specific.tap.CapabilityFactSheetListener;
import prerna.ui.main.listener.specific.tap.SysDupeHealthGridListener;

/**
 * This class creates the capability fact sheet.
 */
public class CapabilityFactSheet extends BrowserPlaySheet{

	Hashtable allHash = new Hashtable();
	Hashtable capabilityHash = new Hashtable();
	
	CapabilityFactSheetListener singleCapFactSheetCall = new CapabilityFactSheetListener();
	
	/**
	 * Constructor for CapabilityFactSheet. Generates the landing page for the capability fact sheets.
	 */
	public CapabilityFactSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
	}

	/**
	 * Processes all Sys Dupe queries and shows results in sysdupe.html format.
	 */
	@Override
	public void createView()
	{
		String workingDir = System.getProperty("user.dir");
		
		singleCapFactSheetCall.setCapabilityFactSheet(this);
		browser.navigate("file://" + workingDir + "/html/MHS-FactSheets/Capability Fact Sheet.html");
		singleCapFactSheetCall.invoke(null);
		
		browser.addNavigationListener(new NavigationListener() {
    	    public void navigationStarted(NavigationEvent event) {
    	    	logger.info("event.getUrl() = " + event.getUrl());
    	    }

    	    public void navigationFinished(NavigationFinishedEvent event) {
   	    	browser.registerFunction("singleCapFactSheet",  singleCapFactSheetCall);
  //  			callIt();
    	    }
    	});
	       
//		browser.navigate("file://" + workingDir + "/html/MHS-FactSheets/Capability Fact Sheet.html");
		
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable Includes the data series.*/
	public Hashtable processQueryData()
	{
		addPanel();
		ArrayList dataArrayList = new ArrayList();
		String[] var = wrapper.getVariables(); 		
		for (int i=0; i<list.size(); i++)
		{	
			Object[] listElement = list.get(i);
			for (int j = 0; j < var.length; j++) 
			{	
					String text = (String) listElement[j];
					text = text.replaceAll("^\"|\"$", "");
					if (text.length() >= 30) {
					text = text.substring(0, Math.min(text.length(), 30));  //temporary
					text = text + "...";
					}
					dataArrayList.add(text);
			}			
		}

		capabilityHash.put("dataSeries", dataArrayList);
		
		return allHash;
	}
	
	public void processNewCapability(String capability)
	{		
		CapabilityFactSheetPerformer performer = new CapabilityFactSheetPerformer();
		Hashtable<String,Object> dataSeries = performer.processQueries(capability);
		allHash.put("dataSeries", dataSeries);
		allHash.put("capability", capability);

		callItAllHash();
	}
	
	public void callIt()
	{
		Gson gson = new Gson();
		browser.executeScript("capabilityList('" + gson.toJson(capabilityHash) + "');");
		browser.executeScript("start();");
	}
	
	public void callItAllHash()
	{
		Gson gson = new Gson();
//		browser.executeScript("capabilityData('" + gson.toJson(allHash) + "');");
		browser.executeScript("start('" + gson.toJson(allHash) + "');");
	}
	
}


