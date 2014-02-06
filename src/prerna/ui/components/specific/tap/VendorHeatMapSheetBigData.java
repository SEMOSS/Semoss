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
import java.util.HashSet;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.HeatMapPlaySheet;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

/**
 * Vendor selection-specific heat map playsheet.
 */
public class VendorHeatMapSheetBigData extends HeatMapPlaySheet {

	Hashtable allHash;
	
	/**
	 * Constructor for VendorHeatMapSheet.
	 */
	public VendorHeatMapSheetBigData() {
		super();
		this.setPreferredSize(new Dimension(800, 600));
		String workingDir = System.getProperty("user.dir");
		fileName = "file://" + workingDir+"/html/MHS-RDFSemossCharts/app/capabilitybigdata.html";
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
		options.put("Supports_out_of_box", Integer.parseInt(DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_1)));
		options.put("Supports_with_configuration",  Integer.parseInt(DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_2)));
		options.put("Supports_with_customization", Integer.parseInt(DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_3)));
		options.put("Does_not_support", Integer.parseInt(DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_4)));
		
		Hashtable<String,Object> capabilities = new Hashtable<String, Object>();
		ArrayList<String> techReqWithStandard = new ArrayList<String>();
		for(int i=0;i<queryArray.size();i++)
		{
			updateProgressBar((i+1)+"0%...Processing Queries", (i+1)*10);
			wrapper = new SesameJenaSelectWrapper();
			if(engine!= null && rs == null){
				wrapper.setQuery(queryArray.get(i));
				wrapper.setEngine(engine);
				wrapper.executeQuery();
			}
			else if (engine==null && rs!=null){
				wrapper.setResultSet(rs);
				wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
			}
			
			// get the bindings from it
			String[] names = wrapper.getVariables();

			// now get the bindings and generate the data
			try {
				while(wrapper.hasNext())
				{
					SesameJenaSelectStatement sjss = wrapper.next();
					String vendor = (String)sjss.getVar(names[0]);
					String capability = (String)sjss.getVar(names[1]);
					String requirement = ((String)sjss.getVar(names[2]));
					String requirementCategory = names[2];
					String fulfill = (String)sjss.getVar(names[3]);
					double score=options.get(fulfill); //score based on vendor direct response
					
//					Hashtable<String, Object> requirementCategories;
//					Hashtable<String, Object> requirementAndWeight;
//					Hashtable<String, Hashtable<String,Object>> requirements;
//					Hashtable<String,Object> values;

//					if(!requirementCategory.contains("TechRequirement")||!techReqWithStandard.contains(requirement))
//					{
//					
//						if(capabilities.containsKey(capability))
//						{
//							requirementCategories=(Hashtable<String,Object>)capabilities.get(capability);
//							if(requirementCategories.containsKey(requirementCategory))
//							{
//								requirementAndWeight=(Hashtable<String,Object>)requirementCategories.get(requirementCategory);
//								requirements=(Hashtable<String, Hashtable<String,Object>>)requirementAndWeight.get("requirement");
//								if(requirements.containsKey(requirement+"-"+vendor))
//									values=requirements.get(requirement+"-"+vendor);
//								else
//								{
//									values=new Hashtable<String,Object>();
//									values.put("value", 10.0);
//									requirements.put(requirement+"-"+vendor,values);
//								}
//							}
//							else
//							{
//								values=new Hashtable<String,Object>();
//								values.put("value", 10.0);
//								requirements=new Hashtable<String,Hashtable<String,Object>>();
//								requirements.put(requirement+"-"+vendor,values);
//								requirementAndWeight = new Hashtable<String,Object>();
//								requirementAndWeight.put("requirement",requirements);
//								requirementCategories.put(requirementCategory, requirementAndWeight);
//							}
//						}
//						else
//						{
//							values=new Hashtable<String,Object>();
//							values.put("value", 10.0);
//							requirements= new Hashtable<String,Hashtable<String,Object>>();
//							requirements.put(requirement+"-"+vendor,values);
//							requirementAndWeight = new Hashtable<String,Object>();
//							requirementCategories=new Hashtable<String,Object>();
//							requirementAndWeight.put("requirement",requirements);
//							requirementCategories.put(requirementCategory, requirementAndWeight);
//							capabilities.put(capability, requirementCategories);
//						}
//
//						values.put("Requirement",requirement);
//						values.put("Vendor",vendor);
//						if(score<(Double)values.get("value"))
//							values.put("value",score);
//                   	}
//					if(requirementCategory.contains("TechStandard")&&!techReqWithStandard.contains(requirement))
//						techReqWithStandard.add((String)sjss.getVar(names[5]));
					
					Hashtable<String, Object> reqCategoriesAndVendors;
					Hashtable<String, Object> requirements;
					Hashtable<String, Object> children;
					Hashtable<String,Object> values;

					
					
					if(!requirementCategory.contains("TechRequirement")||!techReqWithStandard.contains(requirement))
					{
						if(capabilities.containsKey(capability))
						{
							reqCategoriesAndVendors = (Hashtable<String,Object>)capabilities.get(capability);
							if(reqCategoriesAndVendors.containsKey(requirementCategory+"-"+vendor))
							{
								requirements =(Hashtable<String,Object>)reqCategoriesAndVendors.get(requirementCategory+"-"+vendor);
								children = (Hashtable<String,Object>)requirements.get("Children");
								if(children.containsKey(requirement+"-"+vendor))
								{
									values=(Hashtable<String,Object>)children.get(requirement+"-"+vendor);
									double oldValue = (Double)values.get("Value");
									if(oldValue>score)
									{
										values.put("Value", score);
										requirements.put("Score", (Double)requirements.get("Score")+score-oldValue);
									}
								}
								else
								{
									values = new Hashtable<String, Object>();
									values.put("Requirement", requirement);
									values.put("Vendor", vendor);
									values.put("Value", score);
									children.put(requirement+"-"+vendor,values);
									requirements.put("Score", (Double)requirements.get("Score")+score);
								}
							}
							else
							{
								requirements = new Hashtable<String,Object>();
								requirements.put("Vendor",vendor);
								requirements.put("Criteria",requirementCategory);
								children = new Hashtable<String,Object>();
								values = new Hashtable<String, Object>();
								values.put("Requirement", requirement);
								values.put("Vendor", vendor);
								values.put("Value", score);
								children.put(requirement+"-"+vendor, values);
								requirements.put("Children",children);
								requirements.put("Score", score);
								reqCategoriesAndVendors.put(requirementCategory+"-"+vendor,requirements);
							}
						}
						else
						{
							reqCategoriesAndVendors = new Hashtable<String, Object>();
							requirements = new Hashtable<String,Object>();
							requirements.put("Vendor",vendor);
							requirements.put("Criteria",requirementCategory);
							children = new Hashtable<String,Object>();
							values = new Hashtable<String, Object>();
							values.put("Requirement", requirement);
							values.put("Vendor", vendor);
							values.put("Value", score);
							children.put(requirement+"-"+vendor, values);
							requirements.put("Children",children);
							requirements.put("Score", score);
							reqCategoriesAndVendors.put(requirementCategory+"-"+vendor,requirements);
							capabilities.put(capability,reqCategoriesAndVendors);
						}
					}
					if(requirementCategory.contains("TechStandard"))
					{
						String techrequirement = (String)sjss.getVar(names[4]);
						if(!techReqWithStandard.contains(techrequirement))
							techReqWithStandard.add(techrequirement);
					}
					
				}
			} catch (Exception e) {
				logger.fatal(e);
			}
		}

		for(String cap: capabilities.keySet())
		{
			Hashtable<String,Object> reqCategoriesAndVendors = (Hashtable<String,Object>)capabilities.get(cap);
			for(String reqCategoryAndVendor:reqCategoriesAndVendors.keySet())
			{
				
				Hashtable<String,Object> requirements = (Hashtable<String,Object>)reqCategoriesAndVendors.get(reqCategoryAndVendor);
				Hashtable<String,Object> children = (Hashtable<String,Object>)requirements.get("Children");
				int numRequirements = children.size();
				requirements.put("Score",(Double)requirements.get("Score")/numRequirements);
			}
		}
		
		
//		for(String cap: capabilities.keySet())
//		{
//			Hashtable<String,Object> reqCategories = (Hashtable<String,Object>)capabilities.get(cap);
//			for(String reqCategory:reqCategories.keySet())
//			{
//				Hashtable<String,Object> requirementAndWeight = (Hashtable<String,Object>)reqCategories.get(reqCategory);
//				requirementAndWeight.put("weight",1);
//				reqCategories.put(reqCategory, requirementAndWeight);
//				
//				Hashtable<String, Hashtable<String,Object>> requirements;
//				Hashtable<String,Object> values;
//				
//				requirements=(Hashtable<String, Hashtable<String,Object>>)requirementAndWeight.get("requirement");
//				for(String requirementVendor : requirements.keySet())
//				{
//					values = requirements.get(requirementVendor);
//					if(!values.containsKey("Vendor"))
//						logger.warn("'Vendor' key does not exist.");
//				}
//			}
//		}
		
		updateProgressBar("80%...Generating Heat Map from Data", 80);
		browser.navigate(fileName);
		browser.waitReady();
		
		allHash = new Hashtable();

		allHash.put("dataSeries",capabilities);
		allHash.put("title", "Criteria vs. Vendors");
		allHash.put("xAxisTitle","Criteria");
		allHash.put("yAxisTitle","Vendor");
		allHash.put("childxAxisTitle","Requirement");
		allHash.put("childyAxisTitle","Vendor");
		allHash.put("weight","weight");
		allHash.put("value", "Score");
		allHash.put("childvalue","Value");
	}
	
	@Override
	public void createView()
	{
		browser.navigate(fileName);
		browser.waitReady();
		
		callIt(allHash);
		updateProgressBar("100%...Heat Map Generation Complete", 100);
	}
}
