/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.components.playsheets.HeatMapPlaySheet;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

/**
 * Vendor selection-specific heat map playsheet.
 */
@SuppressWarnings("serial")
public class VendorHeatMapSheetBigData extends HeatMapPlaySheet {

	private static final Logger logger = LogManager.getLogger(VendorHeatMapSheetBigData.class.getName());
	Hashtable allHash;
	
	/**
	 * Constructor for VendorHeatMapSheet.
	 */
	public VendorHeatMapSheetBigData() {
		super();
		this.setPreferredSize(new Dimension(800, 600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir+"/html/MHS-RDFSemossCharts/app/capabilitybigdata.html";
	}
	
	/**
	 * Overrides BrowserPlaySheet createData(). Executes processing/data gathering and loads proper file (capability.html).
	 */
	@Override
	public void createData() {
	//	addPanel();
		String rfi = "";
		wrapper = new SesameJenaSelectWrapper();
		if(engine!= null && rs == null){
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
		}
		else if (engine==null && rs!=null){
			wrapper.setResultSet(rs);
			wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
		}
		names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			rfi = (String) sjss.getVar(names[0]);
		}
		updateProgressBar("0%...Generating Queries", 0);
		//get queries from question sheet - each query will pull task, business or tech requirement
		ArrayList<String> queryArray= new ArrayList<String>();
		int queryCount = 1;
		String query=""+this.engine.getProperty(ConstantsTAP.VENDOR_HEAT_MAP_REQUIREMENTS_QUERY + "_"+queryCount);
		query = query.replace("*Selected_RFI*", rfi);
		queryArray.add(query);
//		queryCount++;
//		while(query!="null")
//		{
//			queryArray.add(query);
//			queryCount++;
//			query=""+this.engine.getProperty(ConstantsTAP.VENDOR_HEAT_MAP_REQUIREMENTS_QUERY + "_"+queryCount);
//			if(query!="null")
//				query = query.replace("*Selected_RFI*", rfi);
//		}

		//hashtable to hold scoring values
		Hashtable<String,Integer> options = new Hashtable<String,Integer>();
		options.put("out_of_box".toLowerCase(), Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_1)));
		options.put("out_of_box_with_configuration".toLowerCase(),  Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_2)));
		options.put("out_of_box_with_customization".toLowerCase(), Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_3)));
		options.put("does_not_support".toLowerCase(), Integer.parseInt(""+this.engine.getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_4)));
		
		Hashtable<String,Object> capabilities = new Hashtable<String, Object>();
		ArrayList<String> techReqWithStandard = new ArrayList<String>();
		
		list = new ArrayList<Object[]>();
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
			names = wrapper.getVariables();

			// now get the bindings and generate the data
			try {
				while(wrapper.hasNext())
				{
					SesameJenaSelectStatement sjss = wrapper.next();
					String vendor = (String)sjss.getVar(names[0]);
					String capability = (String)sjss.getVar(names[1]);
					String requirement = ((String)sjss.getVar(names[2]));
					String requirementCategory = capability;//names[2];
					String fulfill = (String)sjss.getVar(names[3]);
					fulfill = fulfill.toLowerCase();
					
					Object[] listElement = new Object[4];
					listElement[0] = vendor;
					listElement[1] = capability;
					listElement[2] = requirement;
					listElement[3] = fulfill;
					list.add(listElement);
					double score=0.0;
					Object scoreObj = options.get(fulfill); //score based on vendor direct response
					if(scoreObj!=null&&scoreObj instanceof Double)
						score = (Double)scoreObj;
					else if(scoreObj!=null&&scoreObj instanceof Integer)
					{
						int scoreInt = (Integer) scoreObj;
						score = scoreInt*1.0;
					}
					
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
			} catch (RuntimeException e) {
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
		
		updateProgressBar("80%...Generating Heat Map from Data", 80);
		
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
		
		dataHash = allHash;
	}
	@Override
	public Hashtable processQueryData()
	{
		return allHash;
	}

}
