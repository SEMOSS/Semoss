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
import java.util.Enumeration;
import java.util.Hashtable;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

/**
 * The Play Sheet for creating a heat map diagram.  
 */
public class VendorCapabilityTaskPlaysheet extends BrowserPlaySheet {
	
	/**
	 * Constructor for HeatMapPlaySheet.
	 */
	public VendorCapabilityTaskPlaysheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/heatmap.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Consists of the x-value, y-value, x- and y-axis titles, and the title of the map.*/
	public Hashtable processQueryData()
	{
		Hashtable dataHash = new Hashtable();
		Hashtable dataSeries = new Hashtable();
		
		String[] var = wrapper.getVariables();
		String venVar = var[0];
		String capVar = var[1];
		String taskVar = var[2];
		String supVar = var[3];
		
		String venName = null;
		
		// most of the logic would go in here
		// I am getting the vendor and the capability in x and y
		// now I need to find a way to get to the various tasks in the capability
		// I have to have a running counter to assign the scores
		// for each tasks's support level
		// add all of this and we are ready to go
		Hashtable<String,Integer> options = new Hashtable<String,Integer>();
		options.put("Out_of_Box", Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_1)));
		options.put("Out_of_box", Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_1)));
		options.put("Out_of_Box_with_Configuration",  Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_2)));
		options.put("Out_of_box_with_Configuration",  Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_2)));
		options.put("Out_of_the_box_with_configuration",  Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_2)));
		options.put("Out_of_Box_with_Customization", Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_3)));
		options.put("Customization", Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_3)));
		options.put("Out_of_box_with_Customization", Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_3)));
		options.put("Out_of_the_box_with_customization", Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_3)));
		options.put("Does_Not_Support", Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_4)));
		options.put("Does_not_support", Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_4)));

		// what I am getting is of the form
		// vendor capability task fullfillment level
		Hashtable <String,Hashtable> vendorHash = new Hashtable<String, Hashtable>();
		
		for (int i=0;i<list.size();i++)
		{
			
			Object[] listElement = list.get(i);
			venName = (String) listElement[0];

			Hashtable <String, Integer> capHash = new Hashtable<String, Integer>();
			
			if(vendorHash.containsKey(venName))
				capHash = vendorHash.get(venName);
			
			// since I know all of this is just one vendor, I am not going to worry much here
			String capName = (String) listElement[1];
			String supLevel = (String)listElement[3];
			
			Integer score = new Integer(0);
			if(capHash.containsKey(capName))
				score = (Integer)capHash.get(capName);
			System.err.println("Support is "+ venName+ " <>" + supLevel);
			score += options.get(supLevel);
			
			capHash.put(capName, score);
			vendorHash.put(venName, capHash);
		}
		
		
		Enumeration <String> vendors = vendorHash.keys();
		Hashtable <String, Integer> capHash = new Hashtable<String, Integer>();

		
		while(vendors.hasMoreElements())
		{
			String curVendor = vendors.nextElement();
			// now run the regular routine
			capHash = vendorHash.get(curVendor);
			Enumeration<String> en = capHash.keys();

			// now enumerate the elements
			curVendor = curVendor.replaceAll("\"", "");
			
			while(en.hasMoreElements())
			{
				String capName = en.nextElement();
				capName = capName.replaceAll("\"", "");
				Integer score = capHash.get(capName);
				
				Hashtable elementHash = new Hashtable();
				elementHash.put(venVar, curVendor);
				elementHash.put(capVar, capName);
				elementHash.put(supVar, score);
				
				dataHash.put(curVendor+"-"+capName, elementHash);
			}
		}
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		allHash.put("title",  "All Vendors " + " vs Capabilities ");
		allHash.put("xAxisTitle", capVar);
		allHash.put("yAxisTitle", venVar);
		allHash.put("value", supVar);
		
		System.err.println("> >>>>>>>>>>>>>>>>>>>>>>>>>>> " + allHash);
		
		return allHash;
	}
	

}
