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
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

import javax.swing.JDesktopPane;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * The Play Sheet for creating a Sankey diagram using nodes and relationships.  
 */
public class SankeyPlaySheet extends BrowserPlaySheet {

	/**
	 * Constructor for SankeyPlaySheet.
	 */
	public SankeyPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/sankey.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	 * Sankey query structure: ?source ?target ?value ?target2 ?value2 ?target3 ?value3...etc
	 * note: ?target is the source for ?target2 and ?target2 is the source for ?target3...etc
	
	 * @return Hashtable - Consists of all the nodes and links included in the Sankey diagram.*/
	public Hashtable processQueryData() {
		HashSet links = new HashSet();
		HashSet nodes = new HashSet();
		String[] var = wrapper.getVariables();
		int value = 1;
		
		for (int i=0; i<list.size(); i++) {
			LinkedHashMap elementLinks = new LinkedHashMap();
			LinkedHashMap elementSource = new LinkedHashMap();
			LinkedHashMap elementTarget = new LinkedHashMap();
			
			Object[] listElement = list.get(i);
			elementLinks.put("source", listElement[0]);
			elementLinks.put("target", listElement[1]);
			elementSource.put("name", listElement[0]);
			elementTarget.put("name", listElement[1]);
			if (listElement[2].toString().equals("")) {
				elementLinks.put("value", value);	
			}
			else { 
				elementLinks.put("value", listElement[2]); 
			}
			
			links.add(elementLinks);
			nodes.add(elementSource);
			nodes.add(elementTarget);
			
			if (var.length > 3) { 
				for (int j = 1; j < (var.length - 2); j = j + 2) {	
					LinkedHashMap newelementLinks = new LinkedHashMap();
					LinkedHashMap newElementSource = new LinkedHashMap();
					LinkedHashMap newElementTarget = new LinkedHashMap();
					
					newelementLinks.put("source", listElement[j]);
					newelementLinks.put("target", listElement[j+2]);
					newElementSource.put("name", listElement[j]);
					newElementTarget.put("name", listElement[j+2]);					
					if (listElement[j+3].toString().equals("")) {
						newelementLinks.put("value", value);	
					}
					else { 
						newelementLinks.put("value", listElement[j+3]); 
					}	
					links.add(newelementLinks);
					nodes.add(newElementSource);
					nodes.add(newElementTarget);					
				}				
			}		
		}
		
		Hashtable allHash = new Hashtable();
		allHash.put("nodes", nodes);
		allHash.put("links", links);
				
		return allHash;
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
	public void createView()
	{
		super.createView();
		//BrowserServices.getInstance().setPromptService(new SilentPromptService());
		Hashtable dataHash = new Hashtable();
		dataHash = processQueryData();
		browser.loadURL(fileName);
		while (browser.isLoading()) {
		    try {
				TimeUnit.MILLISECONDS.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
		
		callIt(dataHash);
	}

}
