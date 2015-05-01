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
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.HeatMapPlaySheet;
import prerna.ui.main.listener.impl.BrowserZoomKeyListener;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

/**
 * Vendor selection-specific heat map playsheet.
 */
@SuppressWarnings("serial")
public class VendorHeatMapSheet extends HeatMapPlaySheet {

	private static final Logger logger = LogManager.getLogger(VendorHeatMapSheet.class.getName());
	Hashtable allHash;
	
	/**
	 * Constructor for VendorHeatMapSheet.
	 */
	public VendorHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800, 600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir+"/html/MHS-RDFSemossCharts/app/capability.html";
	}
	
	/**
	 * Overrides BrowserPlaySheet createView(). Executes processing/data gathering and loads proper file (capability.html).
	 */
	@Override
	public void createData() {
		addPanel();
		
		updateProgressBar("0%...Generating Queries", 0);
		//get queries from question sheet
		//each query will pull task, bus or tech requirement
		ArrayList<String> queryArray= new ArrayList<String>();
		int queryCount = 1;
		String query=DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_HEAT_MAP_REQUIREMENTS_QUERY + "_"+queryCount);
		while(query!=null)
		{
			queryArray.add(query);
			queryCount++;
			query=DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_HEAT_MAP_REQUIREMENTS_QUERY + "_"+queryCount);
		}

		//hashtable to hold scoring values
		Hashtable<String,Integer> options = new Hashtable<String,Integer>();
		options.put("Supports_out_of_box", Integer.parseInt(this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_1)));
		options.put("Supports_with_configuration",  Integer.parseInt(this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_2)));
		options.put("Supports_with_customization", Integer.parseInt(this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_3)));
		options.put("Does_not_support", Integer.parseInt(this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_4)));
		
		Hashtable<String,Object> capabilities = new Hashtable<String, Object>();
		ArrayList<String> techReqWithStandard = new ArrayList<String>();
		for(int i=0;i<queryArray.size();i++)
		{
			updateProgressBar((i+1)+"0%...Processing Queries", (i+1)*10);
			wrapper = WrapperManager.getInstance().getSWrapper(engine, query); //new SesameJenaSelectWrapper();
			/*if(engine!= null && rs == null){
				wrapper.setQuery(queryArray.get(i));
				wrapper.setEngine(engine);
				wrapper.executeQuery();
			}
			else if (engine==null && rs!=null){
				wrapper.setResultSet(rs);
				wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
			}*/
			
			// get the bindings from it
			String[] names = wrapper.getVariables();

			// now get the bindings and generate the data
			try {
				while(wrapper.hasNext())
				{
					ISelectStatement sjss = wrapper.next();
					String vendor = (String)sjss.getVar(names[0]);
					String capability = (String)sjss.getVar(names[1]);
					//TODO: the variable underneath is not used?
					//String task = (String)sjss.getVar(names[2]);
					String requirement = ((String)sjss.getVar(names[3]));
					String requirementCategory = names[3];
					String fulfill = (String)sjss.getVar(names[4]);
					double score=options.get(fulfill); //score based on vendor direct response
					
					Hashtable<String, Object> requirementCategories;
					Hashtable<String, Object> requirementAndWeight;
					Hashtable<String, Hashtable<String,Object>> requirements;
					Hashtable<String,Object> values;

					if(!requirementCategory.contains("TechRequirement")||!techReqWithStandard.contains(requirement))
					{
					
						if(capabilities.containsKey(capability))
						{
							requirementCategories=(Hashtable<String,Object>)capabilities.get(capability);
							if(requirementCategories.containsKey(requirementCategory))
							{
								requirementAndWeight=(Hashtable<String,Object>)requirementCategories.get(requirementCategory);
								requirements=(Hashtable<String, Hashtable<String,Object>>)requirementAndWeight.get("requirement");
								if(requirements.containsKey(requirement+"-"+vendor))
									values=requirements.get(requirement+"-"+vendor);
								else
								{
									values=new Hashtable<String,Object>();
									values.put("value", 10.0);
									requirements.put(requirement+"-"+vendor,values);
								}
							}
							else
							{
								values=new Hashtable<String,Object>();
								values.put("value", 10.0);
								requirements=new Hashtable<String,Hashtable<String,Object>>();
								requirements.put(requirement+"-"+vendor,values);
								requirementAndWeight = new Hashtable<String,Object>();
								requirementAndWeight.put("requirement",requirements);
								requirementCategories.put(requirementCategory, requirementAndWeight);
							}
						}
						else
						{
							values=new Hashtable<String,Object>();
							values.put("value", 10.0);
							requirements= new Hashtable<String,Hashtable<String,Object>>();
							requirements.put(requirement+"-"+vendor,values);
							requirementAndWeight = new Hashtable<String,Object>();
							requirementCategories=new Hashtable<String,Object>();
							requirementAndWeight.put("requirement",requirements);
							requirementCategories.put(requirementCategory, requirementAndWeight);
							capabilities.put(capability, requirementCategories);
						}

						values.put("Requirement",requirement);
						values.put("Vendor",vendor);
						if(score<(Double)values.get("value"))
							values.put("value",score);
                   	}
					if(requirementCategory.contains("TechStandard")&&!techReqWithStandard.contains(requirement))
						techReqWithStandard.add((String)sjss.getVar(names[5]));
				}
			} catch (RuntimeException e) {
				logger.fatal(e);
			}
		}

		for(String cap: capabilities.keySet())
		{
			Hashtable<String,Object> reqCategories = (Hashtable<String,Object>)capabilities.get(cap);
			for(String reqCategory:reqCategories.keySet())
			{
				Hashtable<String,Object> requirementAndWeight = (Hashtable<String,Object>)reqCategories.get(reqCategory);
				requirementAndWeight.put("weight",1);
				reqCategories.put(reqCategory, requirementAndWeight);
				
				Hashtable<String, Hashtable<String,Object>> requirements;
				Hashtable<String,Object> values;
				
				requirements=(Hashtable<String, Hashtable<String,Object>>)requirementAndWeight.get("requirement");
				for(String requirementVendor : requirements.keySet())
				{
					values = requirements.get(requirementVendor);
					if(!values.containsKey("Vendor"))
						logger.warn("'Vendor' key does not exist.");
				}
			}
		}
		
		updateProgressBar("80%...Generating Heat Map from Data", 80);
		browser.loadURL(fileName);
		while (browser.isLoading()) {
		    try {
				TimeUnit.MILLISECONDS.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		allHash = new Hashtable();

		allHash.put("dataSeries",capabilities);
		allHash.put("title", "Requirements vs. Vendors");
		allHash.put("xAxisTitle","Requirement");
		allHash.put("yAxisTitle","Vendor");
		allHash.put("requirement","requirement");
		allHash.put("weight","weight");
		allHash.put("value", "value");
	}
	
	@Override
	public void createView()
	{
		browserView.addKeyListener(new BrowserZoomKeyListener(browser));
		browser.loadURL(fileName);
		while (browser.isLoading()) {
		    try {
				TimeUnit.MILLISECONDS.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		callIt(allHash);
		updateProgressBar("100%...Heat Map Generation Complete", 100);
	}
}
