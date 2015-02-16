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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;


/**
 */
public class SourceReportTaskWeightButtonListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(SourceReportTaskWeightButtonListener.class.getName());
	ArrayList<String> outputArray = new ArrayList<String>();
	String repo="";
	IEngine engine;
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		//get database to calculate task weights from and database of vendors to add task weights to		
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object [] repos = (Object [])list.getSelectedValues();
		repo = repos[0] +"";
		engine = (IEngine)DIHelper.getInstance().getLocalProp(repo);
		if(!repo.toLowerCase().contains("vendor"))
		{
			Utility.showError("The database does not contain the required elements");
			return;
		}
		

		ArrayList<String> tasks = getVendorTasks();//get a list of all tasks used in the vendor database
		if(tasks==null)
			return;
		Hashtable<String,Double> taskAndWeights =calculateTaskWeights(tasks);
		if(taskAndWeights==null)
			return;
		replaceTaskWeights(taskAndWeights);	
		
		//hashtable to hold scoring values
		Hashtable<String,Integer> options = new Hashtable<String,Integer>();
		options.put("Supports_out_of_box", Integer.parseInt(DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_1)));
		options.put("Supports_with_configuration",  Integer.parseInt(DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_2)));
		options.put("Supports_with_customization", Integer.parseInt(DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_3)));
		options.put("Does_not_support", Integer.parseInt(DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_FULFILL_LEVEL_4)));
		
		Hashtable<String, Hashtable<String, Hashtable<String,Object>>> vendors = createVendorHash(options);
		Hashtable<String,Double> vendorsCustomCost = createVendorCustomCostHash();
		Hashtable<String,Double> vendorsHWSWCost = createVendorHWSWCostHash();
		replaceBVTVCost(vendors,vendorsCustomCost,vendorsHWSWCost);
		
		logger.info("Source Report Generator Button Pushed");

	}
	
	/**
	 * Method getVendorTasks.
	 * @return ArrayList<String> 
	 */
	public ArrayList<String> getVendorTasks(){
		String vendorTasksQuery = "SELECT DISTINCT ?Task WHERE {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}";


		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(vendorTasksQuery);
		wrapper.setEngine(engine);
		wrapper.executeQuery();*/
		engine = (IEngine)DIHelper.getInstance().getLocalProp(repo);

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, vendorTasksQuery);

		ArrayList<String> tasks = new ArrayList<String>();
		String[] names = wrapper.getVariables();
		try {
			if(!wrapper.hasNext())
			{
				Utility.showError("The database does not contain the required elements");
				return null;
			}
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				tasks.add((String)sjss.getVar(names[0]));
			}
		} 
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		return tasks;
	}
	
	/**
	 * Method calculateTaskWeights.
	 * @param tasks ArrayList<String>
	 * @return Hashtable<String,Double> 
	 */
	public Hashtable<String,Double> calculateTaskWeights(ArrayList<String> tasks)
	{
		//JComboBox<String> changedTaskWeightComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.CHANGED_TASK_WEIGHT_COMBOBOX);
		//String changedDB = (String) changedTaskWeightComboBox.getSelectedItem();
		String changedDB = "HR_Core";
		String query1 = "SELECT DISTINCT ?Task ?BP ?taskCount ?criticality ?transNum WHERE { {?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Task ?needs ?BP.} {?BP <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {SELECT ?BP (COUNT(DISTINCT(?Task)) AS ?taskCount) WHERE { {?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Task ?needs ?BP.} {?BP <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} } GROUP BY ?BP}  {?BP <http://semoss.org/ontologies/Relation/Contains/Wartime_Criticality> ?criticality} {?BP <http://semoss.org/ontologies/Relation/Contains/Transactions_Num> ?transNum}}";
		String query2 = "SELECT DISTINCT ?Capability ?Task (SUM(COALESCE(?dataWeight,0) * COALESCE(?Error_Percent,0) * .583 + COALESCE(?bluWeight,0) * COALESCE(?Error_Percent1,0) * .23) AS ?Task_Effect) WHERE {{SELECT DISTINCT ?Capability ?Task ?Data_Object ?FError ?Error_Percent ?dataWeight WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Data_Object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}  {?FError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?Attribute <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Attribute> ;} {?FError_Needs_Data_Object <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Assigned <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned> ;}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Task <http://semoss.org/ontologies/Relation/Needs> ?Data_Object}{ ?Task <http://semoss.org/ontologies/Relation/Has> ?Attribute.}{?FError <http://semoss.org/ontologies/Relation/Supports> ?Attribute}{?FError ?FError_Needs_Data_Object ?Data_Object}{?FError_Needs_Data_Object <http://semoss.org/ontologies/Relation/Contains/weight> ?dataWeight} { ?Activity ?Assigned ?FError.}{?Assigned <http://semoss.org/ontologies/Relation/Contains/weight> ?Error_Percent}}  }UNION{SELECT DISTINCT ?Capability ?Task ?BLU ?FError ?Error_Percent1 ?bluWeight WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?FError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?Attribute <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Attribute> ;}{?FError_Needs_Data_Object <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Assigned <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned> ;}{ ?Task <http://semoss.org/ontologies/Relation/Has> ?Attribute.}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.}{?Task <http://semoss.org/ontologies/Relation/Needs> ?BLU}{?FError <http://semoss.org/ontologies/Relation/Supports> ?Attribute}{?FError ?FError_Needs_Data_Object ?BLU}{?FError_Needs_Data_Object <http://semoss.org/ontologies/Relation/Contains/weight> ?bluWeight}{ ?Activity ?Assigned ?FError.}{?Assigned <http://semoss.org/ontologies/Relation/Contains/weight> ?Error_Percent1} } }} GROUP BY ?Capability ?Task ORDER BY ?Task_Effect";

		engine = (IEngine)DIHelper.getInstance().getLocalProp(changedDB);
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query1);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query1);
		wrapper.setEngine(engine);
		wrapper.executeQuery();*/

		Hashtable<String, Double> taskHash = new Hashtable<String, Double>();
		String[] names = wrapper.getVariables();
		try {
			if(!wrapper.hasNext())
			{
				Utility.showError("The database does not contain the required elements");
				return null;
			}
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				Double valToAdd=0.0;
				if((Double)sjss.getVar(names[4])!=0.0)
						valToAdd=((Double)(sjss.getVar(names[3])) *Math.log(((Double)sjss.getVar(names[4]))))/((Double)sjss.getVar(names[2]));
				if(taskHash.containsKey((String)sjss.getVar(names[0])))
						taskHash.put((String)sjss.getVar(names[0]),taskHash.get((String)sjss.getVar(names[0]))+valToAdd);
				else
					taskHash.put((String)sjss.getVar(names[0]),valToAdd);
			}
		} 
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		
			wrapper = WrapperManager.getInstance().getSWrapper(engine, query2);

			/*wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query2);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
			*/
			
			//all tasks and their associated weights are stored in taskAndWeights
			Hashtable<String,Double> taskAndWeights = new Hashtable<String,Double>();
			String[] names2 = wrapper.getVariables();
			try {
				while(wrapper.hasNext()) {
					ISelectStatement sjss = wrapper.next();
					//Object [] values = new Object[2];
					String task=(String)sjss.getVar(names2[1]);
					if(tasks.contains(task))
					{
					Double weight=0.0;
					if(taskHash.containsKey(task))
						weight=((Double)(sjss.getVar(names2[2])))*taskHash.get(task);
					taskAndWeights.put(task, weight);
					}
				}
			} 
			catch (RuntimeException e) {
				e.printStackTrace();
			}
			
			for(String task : tasks)
			{
				if(!taskAndWeights.containsKey(task))
					taskAndWeights.put(task, 0.0);
			}
			return taskAndWeights;
	}

	/**
	 * Method replaceTaskWeights.
	 * @param taskAndWeights Hashtable<String,Double>
	 */
	public void replaceTaskWeights(Hashtable<String,Double> taskAndWeights)
	{
		//delete old properties
		String deleteQuery="DELETE {?Task ?rel ?Weight.} WHERE{BIND(<http://semoss.org/ontologies/Relation/Contains/Weight> AS ?rel){?Task ?rel ?Weight ;}}";	
		Update up;
		engine = (IEngine)DIHelper.getInstance().getLocalProp(repo);
		try {
			up = ((BigDataEngine)engine).rc.prepareUpdate(QueryLanguage.SPARQL, deleteQuery);
			((BigDataEngine)engine).rc.setAutoCommit(false);
			up.execute();
			((BigDataEngine)engine).rc.commit();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (UpdateExecutionException e) {
			e.printStackTrace();
		}
		
		String predUri = "<http://semoss.org/ontologies/Relation/Contains/Weight>";
			//start with type triple
		String insertQuery = "INSERT DATA { " +predUri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://semoss.org/ontologies/Relation/Contains>. ";
		//add other type triple
		insertQuery = insertQuery + predUri +" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>. ";
		//add sub property triple -->>>>>>> should probably look into this.... very strange how other properties are set up
		insertQuery = insertQuery + predUri +" <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
				predUri + ". ";
		
		for(String task : taskAndWeights.keySet())
			{
			String subjectUri = "<http://health.mil/ontologies/Concept/Task/"+task+">";
			String objectUri = "\"" +taskAndWeights.get(task)+"\""+"^^<http://www.w3.org/2001/XMLSchema#double>";
			insertQuery = insertQuery+subjectUri+" "+predUri+" "+objectUri+". ";
			}
		insertQuery = insertQuery + "}";

		try {
			up = ((BigDataEngine)engine).rc.prepareUpdate(QueryLanguage.SPARQL, insertQuery);
			((BigDataEngine)engine).rc.setAutoCommit(false);
			up.execute();
			((BigDataEngine)engine).rc.commit();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (UpdateExecutionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Method createVendorHash.
	 * @param options Hashtable<String,Integer>
	 * @return Hashtable<String,Hashtable<String,Hashtable<String,Object>>> 
	 */
	public Hashtable<String, Hashtable<String, Hashtable<String,Object>>> createVendorHash(Hashtable<String,Integer> options){
		ArrayList<String> queryArray= new ArrayList<String>();
		int queryCount = 1;
		String query=(String)DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_BV_TV_QUERY + "_"+queryCount);
		while(query!=null)
		{
			queryArray.add(query);
			queryCount++;
			query=(String)DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_BV_TV_QUERY + "_"+queryCount);
		}

		Hashtable<String, Hashtable<String, Hashtable<String,Object>>> vendors = new Hashtable<String, Hashtable<String, Hashtable<String,Object>>>();

		//SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		ISelectWrapper wrapper;
		String[] names;
		for(int i=0;i<queryArray.size();i++)
		{
			/*wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(queryArray.get(i));
			wrapper.setEngine(engine);
			wrapper.executeQuery();*/

			wrapper = WrapperManager.getInstance().getSWrapper(engine, queryArray.get(i));

			// get the bindings from it
			names = wrapper.getVariables();
			try {
				while(wrapper.hasNext())
				{
					ISelectStatement sjss = wrapper.next();
					String vendor = (String)sjss.getVar(names[0]);
					String task = (String)sjss.getVar(names[1]);
					String requirementCategory = (String)names[2];
					String fulfill = (String)sjss.getVar(names[3]);
					Double weight = (Double)sjss.getVar(names[4]);
					double score=options.get(fulfill).doubleValue(); 
					Hashtable<String, Hashtable<String,Object>> tasksHash;
					Hashtable<String,Object> values;
					if(vendors.containsKey(vendor))
					{
						tasksHash=vendors.get(vendor);
						if(tasksHash.containsKey(task))
							values=tasksHash.get(task);
						else
						{
							values=new Hashtable<String,Object>();
							values.put("BusScore", 0.0);
							values.put("TechScore", 0.0);
							values.put("BusNum", 0.0);
							values.put("TechNum", 0.0);
							values.put("Weight",weight);
						}
					}
					else
					{
						values=new Hashtable<String,Object>();
						values.put("BusScore", 0.0);
						values.put("TechScore", 0.0);
						values.put("BusNum", 0.0);
						values.put("TechNum", 0.0);
						values.put("Weight",weight);
						tasksHash=new Hashtable<String,Hashtable<String,Object>>();
					}
					if(requirementCategory.contains("Bus"))
					{
						values.put("BusScore",(Double)values.get("BusScore")+score);
						values.put("BusNum", (Double)values.get("BusNum")+1.0);
					}
					if(requirementCategory.contains("Tech"))
					{
						values.put("TechScore",(Double)values.get("TechScore")+score);
						values.put("TechNum", (Double)values.get("TechNum")+1.0);
					}
					tasksHash.put(task,  values);
					vendors.put(vendor, tasksHash);
				}
			} catch (RuntimeException e) {
				logger.fatal(e);
			}
		}
		return vendors;
	}
	
	/**
	 * Method createVendorCustomCostHash.
	 * @return Hashtable<String,Double> 
	 */
	public Hashtable<String,Double> createVendorCustomCostHash(){
		ArrayList<String> queryArray = new ArrayList<String>();
		int queryCount = 1;
		String query=(String)DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_CUSTOM_COST_QUERY + "_"+queryCount);
		while(query!=null)
		{
			queryArray.add(query);
			queryCount++;
			query=(String)DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_CUSTOM_COST_QUERY + "_"+queryCount);
		}
	
		
		Hashtable<String,Double> vendorsCost = new Hashtable<String, Double>();
		Hashtable<String,ArrayList<String>> vendorsRequirements = new Hashtable<String,ArrayList<String>>();
		//SesameJenaSelectWrapper wrapper;
		ISelectWrapper wrapper;

		for(int i=0;i<queryArray.size();i++)
		{
			/*wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(queryArray.get(i));
			wrapper.setEngine(engine);
			wrapper.executeQuery();
			*/
			wrapper = WrapperManager.getInstance().getSWrapper(engine, queryArray.get(i));

			
			// get the bindings from it
			String[] names = wrapper.getVariables();
			try {
				while(wrapper.hasNext())
				{
					ISelectStatement sjss = wrapper.next();
					String vendor = (String)sjss.getVar(names[0]);
					String requirement = (String)sjss.getVar(names[1]);
					String fulfill = (String)sjss.getVar(names[2]);
					Double effort = 0.0;
					if(fulfill.contains("with"))
						effort=Double.parseDouble((String)sjss.getVar(names[3]));
					ArrayList<String> requirementsList;
					if(vendorsRequirements.containsKey(vendor))
						requirementsList = vendorsRequirements.get(vendor);
					else
					{
						requirementsList = new ArrayList<String>();
						vendorsCost.put(vendor,0.0);
					}
					if(!requirementsList.contains(requirement))
					{
						requirementsList.add(requirement);
						vendorsRequirements.put(vendor,requirementsList);
						vendorsCost.put(vendor,vendorsCost.get(vendor)+effort);
					}
				}
			} catch (RuntimeException e) {
				logger.fatal(e);
			}
		}
		return vendorsCost;
	}

	/**
	 * Method createVendorHWSWCostHash.
	 * @return Hashtable<String,Double> 
	 */
	public Hashtable<String,Double> createVendorHWSWCostHash(){
		ArrayList<String> queryArray = new ArrayList<String>();
		queryArray.add((String)DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_HWSW_COST_QUERY + "_1"));
		queryArray.add((String)DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_HWSW_COST_QUERY + "_2"));
				
		Hashtable<String,Double> vendorsHWSWCost = new Hashtable<String, Double>();
		
		//SesameJenaSelectWrapper wrapper;
		ISelectWrapper wrapper;

		for(int i=0;i<queryArray.size();i++)
		{
			/*wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(queryArray.get(i));
			wrapper.setEngine(engine);
			wrapper.executeQuery();
			*/
			
			wrapper = WrapperManager.getInstance().getSWrapper(engine, queryArray.get(i));

			
			// get the bindings from it
			String[] names = wrapper.getVariables();
			try {
				while(wrapper.hasNext())
				{
					ISelectStatement sjss = wrapper.next();
					String vendor = (String)sjss.getVar(names[0]);
					Double cost = (Double)sjss.getVar(names[1]);
					if(vendorsHWSWCost.containsKey(vendor))
						cost+=vendorsHWSWCost.get(vendor);
					vendorsHWSWCost.put(vendor, cost);
				}
			} catch (RuntimeException e) {
				logger.fatal(e);
			}
		}
		return vendorsHWSWCost;
	}
	
	/**
	 * Method replaceBVTVCost.
	 * @param vendors Hashtable<String,Hashtable<String,Hashtable<String,Object>>>
	 * @param vendorsCost Hashtable<String,Double>
	 * @param vendorsHWSWCost Hashtable<String,Double>
	 */
	public void replaceBVTVCost(Hashtable<String, Hashtable<String, Hashtable<String,Object>>> vendors,Hashtable<String,Double> vendorsCost,Hashtable<String,Double> vendorsHWSWCost){
		//delete old properties
		ArrayList<String> deleteQueries = new ArrayList<String>();
		deleteQueries.add("DELETE {?Vendor ?hasBS ?bs.} WHERE{BIND(<http://semoss.org/ontologies/Relation/Contains/BusinessScore> AS ?hasBS){?Vendor ?hasBS ?bs ;}}");	
		deleteQueries.add("DELETE {?Vendor ?hasTS ?ts.} WHERE{BIND(<http://semoss.org/ontologies/Relation/Contains/TechScore> AS ?hasTS){?Vendor ?hasTS ?ts ;}}");
		deleteQueries.add("DELETE {?Vendor ?hasCost ?cost.} WHERE{BIND(<http://semoss.org/ontologies/Relation/Contains/CustomizationCost> AS ?hasCost){?Vendor ?hasCost ?cost ;}}");
		deleteQueries.add("DELETE {?Vendor ?hasCost ?totalCost.} WHERE{BIND(<http://semoss.org/ontologies/Relation/Contains/TotalCost> AS ?hasCost){?Vendor ?hasCost ?totalCost ;}}");
		
		for(int i=0;i<deleteQueries.size();i++)
		{
			Update up;
			try {
				up = ((BigDataEngine)engine).rc.prepareUpdate(QueryLanguage.SPARQL, deleteQueries.get(i));
				((BigDataEngine)engine).rc.setAutoCommit(false);
				up.execute();
				((BigDataEngine)engine).rc.commit();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (UpdateExecutionException e) {
				e.printStackTrace();
			}
		}

			ArrayList<String> predUris = new ArrayList<String>();
			predUris.add("<http://semoss.org/ontologies/Relation/Contains/BusinessScore>");
			predUris.add("<http://semoss.org/ontologies/Relation/Contains/TechScore>");
			predUris.add("<http://semoss.org/ontologies/Relation/Contains/CustomizationCost>");
			predUris.add("<http://semoss.org/ontologies/Relation/Contains/TotalCost>");
				//start with type triple
			String insertQuery = "INSERT DATA { ";
			for(String predUri : predUris)
			{
				insertQuery+=predUri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://semoss.org/ontologies/Relation/Contains>. ";			
			//add other type triple
			insertQuery = insertQuery + predUri +" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>. ";
			//add sub property triple -->>>>>>> should probably look into this.... very strange how other properties are set up
			insertQuery = insertQuery + predUri +" <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
					predUri + ". ";	
			}
			for(String vendor : vendors.keySet())
			{
				Hashtable<String, Hashtable<String,Object>> tasksHash= vendors.get(vendor);
				double venBusScore=0.0;
				double venTechScore=0.0;
				double weightBusSum=0.0;
				double weightTechSum=0.0;
				for(String task : tasksHash.keySet())
				{
					Hashtable<String,Object> values = tasksHash.get(task);
					if(((Double)values.get("BusNum"))>0)
					{
						venBusScore+=((Double)values.get("BusScore"))/((Double)values.get("BusNum"))*((Double)values.get("Weight"));
						weightBusSum+=(Double)values.get("Weight");
					}
					if(((Double)values.get("TechNum"))>0)
					{
						venTechScore+=((Double)values.get("TechScore"))/((Double)values.get("TechNum"))*((Double)values.get("Weight"));
						weightTechSum+=(Double)values.get("Weight");
					}
				}
				double totalCost=vendorsCost.get(vendor)+vendorsHWSWCost.get(vendor);
				String subjectUri = "<http://health.mil/ontologies/Concept/Vendor/"+vendor+">";
				ArrayList<String> objectUris = new ArrayList<String>();
				objectUris.add("\"" +venBusScore/weightBusSum+"\""+"^^<http://www.w3.org/2001/XMLSchema#double>");
				objectUris.add("\"" +venTechScore/weightTechSum+"\""+"^^<http://www.w3.org/2001/XMLSchema#double>");
				objectUris.add("\"" +vendorsCost.get(vendor)+"\""+"^^<http://www.w3.org/2001/XMLSchema#double>");
				objectUris.add("\"" +totalCost+"\""+"^^<http://www.w3.org/2001/XMLSchema#double>");
				for(int i=0;i<predUris.size();i++)
				{
					insertQuery = insertQuery+subjectUri+" "+predUris.get(i)+" "+objectUris.get(i)+". ";
				}
			}
			insertQuery = insertQuery + "}";
			Update up;
			try {
				up = ((BigDataEngine)engine).rc.prepareUpdate(QueryLanguage.SPARQL, insertQuery);
				((BigDataEngine)engine).rc.setAutoCommit(false);
				up.execute();
				((BigDataEngine)engine).rc.commit();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (UpdateExecutionException e) {
				e.printStackTrace();
			}
	}
	
	/**
	 * Override method from ICharkaListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}
}
