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

import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.JDesktopPane;
import javax.swing.JList;
import javax.swing.JScrollPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.VertexFilterData;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

/**
 * This class implements functionalities used for SOA analysis.
 */
public class SOAAnalysisPerformer implements Runnable {
	//IPlaySheet playSheet = null;
	static final Logger logger = LogManager.getLogger(SOAAnalysisPerformer.class.getName());
	GraphPlaySheet newPlaySheet;
	Hashtable retValues = new Hashtable();
	GraphPlaySheet oldPlaySheet = null;
	String sysName="";
	ArrayList <Object[]> totalList = new ArrayList<Object[]>();
	SOATransitionCalculator soaCalc = new SOATransitionCalculator();
	Hashtable<String,double[]> graphValueHash = new Hashtable<String,double[]>();
	Double hourlyRate = 150.0;
	//variables for passing to analysis sheet
	Vector dataFedListVector = new Vector();
	double dataFedTotalLOE = 0.0;
	Vector dataConListVector = new Vector();
	double dataConTotalLOE = 0.0;
	
	/**
	 * Constructor for SOAAnalysisPerformer.
	 * @param playSheet GraphPlaySheet
	 */
	public SOAAnalysisPerformer (GraphPlaySheet playSheet)
	{
		this.oldPlaySheet = playSheet;
	}
	/**
	 * Runs the SOA analysis by processing through old information about services. 
	 * Gets all estimates from the financial database and add playsheets for transition analysis.
	 */
	public void run() {
		recreateOldSheet();
		nodeListProcess();
		extendServices();
		removeICDs();
		//recreateNewSheet();
		
	//	get all estimates from financial database
		newPlaySheet.updateProgressBar("80%...Performing Financial Analysis", 80);
		getDataEstimates();
		getBLUEstimates();
		getGenericEstimates();
//		
//		
		double [] soaICDMaintenanceCost = soaCalc.processSOAICDMaintenance(oldPlaySheet.filterData,newPlaySheet.filterData);
		double [] icdMaintenanceCost = soaCalc.processICDMaintenance(oldPlaySheet.filterData);
		double [] soaCost = soaCalc.processEstimates(totalList);
		graphValueHash.put("soaICDMainCost",  soaICDMaintenanceCost );
		graphValueHash.put("icdMainCost",  icdMaintenanceCost);
		graphValueHash.put("soaBuildCost",  soaCost);
		newPlaySheet.updateProgressBar("90%...Creating Tables and Charts", 90);
		addTransitionSheet();
		addTransitionAnalysisSheet();
		
		newPlaySheet.updateProgressBar("100%...SOA Transition and Analysis Completed", 100);
	}
	
	/**
	 * Recreates the old playsheet used for SOA analysis.
	 */
	public void recreateOldSheet()
	{
		//oldGSheet.setVisible(false);
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object [] repos = (Object [])list.getSelectedValues();

		QuestionPlaySheetStore.getInstance().idCount++;
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+". "+ "SOATransition";
		DIHelper.getInstance().setLocalProperty(Constants.UNDO_BOOLEAN, false);
		newPlaySheet = new GraphPlaySheet();
		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
			
			//newPlaySheet = oldPlaySheet;
			newPlaySheet.setTitle("SOA Transition All: "+oldPlaySheet.getTitle());
			//newPlaySheet.setQuery(query);
			newPlaySheet.setRDFEngine((IEngine)engine);
			newPlaySheet.setQuestionID(insightID);
			newPlaySheet.setGraphData(oldPlaySheet.getGraphData());
			//newPlaySheet.setJenaModel(jenaModel);
			JDesktopPane pane = (JDesktopPane)DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
			QuestionPlaySheetStore.getInstance().put(insightID, newPlaySheet);
			newPlaySheet.setJDesktopPane(pane);
			newPlaySheet.createView();
		}
		
		newPlaySheet.updateProgressBar("100%...Recreate Old Graph playsheet", 100);
	}
	
	
	/**
	 * Extends services by performing an extend query on a selected repository.
	 */
	public void extendServices()
	{
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object [] repos = (Object [])list.getSelectedValues();
		newPlaySheet.updateProgressBar("20%...Extending Services", 20);
		logger.info("Repository is " + repos);
		
		String extendQuery = "CONSTRUCT {?system1 ?build ?service.?build ?subprop ?relation. ?system2 ?build2 ?service.?build2 ?subprop ?relation.  ?system3 ?build3 ?service2. ?build3 ?subprop ?relation. ?service ?exposes ?data. ?service2 ?exposes2 ?blu} WHERE {BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subprop) BIND(<http://semoss.org/ontologies/Relation> AS ?relation){{?service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;}{?system1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?system2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type><http://semoss.org/ontologies/Concept/System>;} BIND(URI(CONCAT(\"http://health.mil/ontologies/Relation/\", SUBSTR(STR(?system1), 45), \":\", SUBSTR(STR(?service), 46))) AS ?build) BIND(URI(CONCAT(\"http://health.mil/ontologies/Relation/\", SUBSTR(STR(?system2), 45), \":\", SUBSTR(STR(?service), 46))) AS ?build2){?icd <http://semoss.org/ontologies/Relation/Payload> ?data;}{?system1 <http://semoss.org/ontologies/Relation/Provide> ?icd;}{?icd <http://semoss.org/ontologies/Relation/Consume> ?system2;}{?service ?exposes ?data;} {?data ?label ?name1;}Filter (?name1 in (FILTER_VALUES))}UNION{{?system3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?service2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?exposes2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf><http://semoss.org/ontologies/Relation/Exposes>;}BIND(URI(CONCAT(\"http://health.mil/ontologies/Relation/\", SUBSTR(STR(?system3), 45), \":\", SUBSTR(STR(?service2), 46))) AS ?build3){?system3 <http://semoss.org/ontologies/Relation/Provide> ?blu}{?service2 ?exposes2 ?blu}.BIND( <http://health.mil/ontologies/Concept/System/> AS ?system3)}}BINDINGS ?icd {BINDINGS_VALUE}";
		
		//replacing alot of params into the query
		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{
			// use the layout to load the sheet later
			// see if the append is on
			VertexFilterData vfd = newPlaySheet.getFilterData();
			Vector  nodeList = (Vector) retValues.get("InterfaceControlDocument");
			Vector  nodeList2 = (Vector) retValues.get("DataObject");
			Vector  nodeList3 = (Vector) retValues.get("BusinessLogicUnit");
			// convert this into a filter list
			// just get the filter string completely
			String filter = "";
			for(int nodeIndex = 0;nodeIndex < nodeList.size();nodeIndex++)
			{
				filter = filter + "(<http://health.mil/ontologies/Concept/InterfaceControlDocument/" + nodeList.get(nodeIndex) + ">)";
					
			}
			extendQuery = extendQuery.replace("BINDINGS_VALUE", filter);
			filter = "";
			for(int nodeIndex = 0;nodeIndex < nodeList2.size();nodeIndex++)
			{
				filter = filter + "\"" + nodeList2.get(nodeIndex)+"\"";
				if(nodeIndex + 1 < nodeList2.size())
					filter = filter + ",";
			}
			extendQuery = extendQuery.replace("FILTER_VALUES", filter);
			filter = "";
			for(int nodeIndex = 0;nodeIndex < nodeList3.size();nodeIndex++)
			{
				filter = filter + "\"" + nodeList3.get(nodeIndex)+"\"";
				if(nodeIndex + 1 < nodeList3.size())
					filter = filter + ",";
			}
			extendQuery = extendQuery.replace("FILTER2_VALUES", filter);
			extendQuery = extendQuery.replace("@System@", sysName);
			
			//extend query set, create data, then overlay
			newPlaySheet.setQuery(extendQuery);
			newPlaySheet.createData();
			newPlaySheet.overlayView();
		}
		
		newPlaySheet.updateProgressBar("100%...Extending Services Done", 100);
	}
	
	/**
	 * Removes ICDs from the selected repository.
	 */
	public void removeICDs() {
		//get database listed
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object [] repos = (Object [])list.getSelectedValues();
		newPlaySheet.updateProgressBar("20%...Removing SOA Replaced Interfaces", 20);
		String removeQuery = "CONSTRUCT {?system1 ?upstream ?icd. ?icd ?downstream ?system2. ?icd ?carries ?data. ?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>.?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>.} WHERE {{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;}{?system1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}{?downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?system2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?system1 ?upstream ?icd;}{?icd ?downstream ?system2;}{?icd ?carries ?data;} }BINDINGS ?icd {FILTER_VALUES}";
		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{

			Vector nodeList = (Vector) retValues.get("InterfaceControlDocument");
			String filter = "";
			for(int nodeIndex = 0;nodeIndex < nodeList.size();nodeIndex++)
			{
				filter = filter + "(<http://health.mil/ontologies/Concept/InterfaceControlDocument/" + nodeList.get(nodeIndex)+">)";
					
			}
			removeQuery = removeQuery.replace("FILTER_VALUES", filter);
			
			newPlaySheet.setQuery(removeQuery);
			// thread
			newPlaySheet.removeView();
		}
		oldPlaySheet.setVisible(true);
		try {
			newPlaySheet.setSelected(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		
		newPlaySheet.updateProgressBar("100%...SOA Interface Removal Complete", 100);
	}
	
	
	/**
	 * Processes through the list of nodes about system, ICDs, and data objects, and BLUs.
	 * Puts values into the retValue hash.
	 */
	public void nodeListProcess()
	{

		VertexFilterData vfd = oldPlaySheet.getFilterData();
		Vector <SEMOSSVertex> nodeList = vfd.getNodes("System");
		Vector <SEMOSSVertex> nodeList1 = vfd.getNodes("InterfaceControlDocument");
		Vector <SEMOSSVertex> nodeList2 = vfd.getNodes("DataObject");
		
		try
		{
			Vector <SEMOSSVertex> nodeList3 = vfd.getNodes("BusinessLogicUnit");
			Vector bluNameVec = new Vector();
			for(int nodeIndex = 0;nodeIndex < nodeList3.size();nodeIndex++)
			{
				bluNameVec.addElement(nodeList3.elementAt(nodeIndex).getProperty(Constants.VERTEX_NAME));
			}
			retValues.put("BusinessLogicUnit", bluNameVec );
		}
		catch (RuntimeException e)
		{
			retValues.put("BusinessLogicUnit", new Vector() );
		}
		Vector <SEMOSSEdge> edgeList = vfd.getEdges("Provide");
		for (int i = 0; i<edgeList.size();i++)
		{
			SEMOSSVertex objV = edgeList.get(i).inVertex;
			String type = (String) objV.getProperty(Constants.VERTEX_TYPE);
			if (type.equals("BusinessLogicUnit"))
			{
				SEMOSSVertex subV = edgeList.get(i).outVertex;
				sysName=(String) subV.getProperty(Constants.VERTEX_NAME);
				break;
			}
		}
		
		Vector sysNameVec = new Vector();
		Vector icdNameVec = new Vector();
		Vector dataNameVec = new Vector();
		
		// convert this into a filter list
		// just get the filter string completely
		String filter = "";
		for(int nodeIndex = 0;nodeIndex < nodeList.size();nodeIndex++)
		{
			sysNameVec.addElement(nodeList.elementAt(nodeIndex).getProperty(Constants.VERTEX_NAME));			
		}
		retValues.put("System", sysNameVec);
		
		for(int nodeIndex = 0;nodeIndex < nodeList1.size();nodeIndex++)
		{
			icdNameVec.addElement(nodeList1.elementAt(nodeIndex).getProperty(Constants.VERTEX_NAME));
				
		}
		retValues.put("InterfaceControlDocument", icdNameVec);
		
		for(int nodeIndex = 0;nodeIndex < nodeList2.size();nodeIndex++)
		{
			dataNameVec.addElement(nodeList2.elementAt(nodeIndex).getProperty(Constants.VERTEX_NAME));
		}
		retValues.put("DataObject", dataNameVec);
		

	}
	
	/**
	 * Processes through the list of services.
	 * Puts names of BLUs, systems, ICDs, and data objects into the retValue hashtable.
	 */
	public void serListProcess()
	{

		VertexFilterData vfd = oldPlaySheet.getFilterData();
		Vector<SEMOSSEdge> serEdgeList = vfd.getEdges("Expose");
		Vector <SEMOSSVertex> nodeList = vfd.getNodes("System");
		Vector <SEMOSSVertex> nodeList1 = vfd.getNodes("InterfaceControlDocument");
		Vector <SEMOSSVertex> nodeList2 = vfd.getNodes("DataObject");
		
		try
		{
			Vector <SEMOSSVertex> nodeList3 = vfd.getNodes("BusinessLogicUnit");
			Vector bluNameVec = new Vector();
			for(int nodeIndex = 0;nodeIndex < nodeList3.size();nodeIndex++)
			{
				bluNameVec.addElement(nodeList3.elementAt(nodeIndex).getProperty(Constants.VERTEX_NAME));
			}
			retValues.put("BusinessLogicUnit", bluNameVec );
		}
		catch (RuntimeException e)
		{
			retValues.put("BusinessLogicUnit", new Vector() );
		}
		Vector <SEMOSSEdge> edgeList = vfd.getEdges("Provide");
		for (int i = 0; i<edgeList.size();i++)
		{
			SEMOSSVertex objV = edgeList.get(i).inVertex;
			String type = (String) objV.getProperty(Constants.VERTEX_TYPE);
			if (type.equals("BusinessLogicUnit"))
			{
				SEMOSSVertex subV = edgeList.get(i).outVertex;
				sysName=(String) subV.getProperty(Constants.VERTEX_NAME);
				break;
			}
		}
		Vector sysNameVec = new Vector();
		Vector icdNameVec = new Vector();
		Vector dataNameVec = new Vector();
		// convert this into a filter list
		// just get the filter string completely
		String filter = "";
		for(int nodeIndex = 0;nodeIndex < nodeList.size();nodeIndex++)
		{
			sysNameVec.addElement(nodeList.elementAt(nodeIndex).getProperty(Constants.VERTEX_NAME));			
		}
		retValues.put("System", sysNameVec);
		
		for(int nodeIndex = 0;nodeIndex < nodeList1.size();nodeIndex++)
		{
			icdNameVec.addElement(nodeList1.elementAt(nodeIndex).getProperty(Constants.VERTEX_NAME));
				
		}
		retValues.put("InterfaceControlDocument", icdNameVec);
		
		for(int nodeIndex = 0;nodeIndex < nodeList2.size();nodeIndex++)
		{
			dataNameVec.addElement(nodeList2.elementAt(nodeIndex).getProperty(Constants.VERTEX_NAME));
		}
		retValues.put("DataObject", dataNameVec);

	}
	
	/**
	 * Gets BLU estimates.
	 */
	public void getBLUEstimates()
	{
		Hashtable paramHash = new Hashtable();
		String query = DIHelper.getInstance().getProperty(Constants.SOA_TRANSITION_ALL_BLU_QUERY);
		paramHash.put("System",  sysName);
		query = Utility.fillParam(query, paramHash);
		ArrayList <Object[]> list = retListFromQuery(query);
		totalList.addAll(list);
		addToValueHashForBLU("bluProvider", list);
	}
	
	/**
	 * Gets generic estimates.
	 */
	public void getGenericEstimates()
	{
		Hashtable paramHash = new Hashtable();
		String query = DIHelper.getInstance().getProperty(Constants.SOA_TRANSITION_ALL_GENERIC_DATA_QUERY);
	    String valueFill="";
	    GraphPlaySheet newSheet = (GraphPlaySheet) newPlaySheet;
		Vector<SEMOSSVertex> serV = (Vector<SEMOSSVertex>) newSheet.filterData.getNodes("Service");
		for (int i=0; i<serV.size();i++)
		{
			valueFill= valueFill + "(\"" + serV.get(i).getProperty(Constants.VERTEX_NAME) + "\")";
		}
		paramHash.put("FILTER_VALUES", valueFill);
		query = Utility.fillParam(query, paramHash);
		ArrayList <Object[]> list = retListFromQuery(query);
		totalList.addAll(list);
		addToValueHashForGeneric("dataGeneric", list);
		
		query = DIHelper.getInstance().getProperty(Constants.SOA_TRANSITION_ALL_GENERIC_BLU_QUERY);
		query = Utility.fillParam(query, paramHash);
		list = retListFromQuery(query);
		totalList.addAll(list);
		addToValueHashForGeneric("bluGeneric", list);
	}
	
	/**
	 * Gets data estimates.
	 */
	public void getDataEstimates()
	{
		Hashtable sysDataHash = getSysDataMappingFromICD();
		Hashtable paramHash = new Hashtable();
		Enumeration<String> enumKey = sysDataHash.keys();
		retValues.put("dataFed", new Vector());
		retValues.put("dataCon", new Vector());
		retValues.put("dataFedCost", 0.0);
		retValues.put("dataConCost", 0.0);
		while(enumKey.hasMoreElements()) {
			
			String query = DIHelper.getInstance().getProperty(Constants.SOA_TRANSITION_ALL_DATA_QUERY);
			System.err.println("Query is " + query);
		    String key = enumKey.nextElement();
		    String valueFill="";
			Vector dataV = (Vector) sysDataHash.get(key);
			for (int i=0; i<dataV.size();i++)
			{
				valueFill= valueFill + "(\"" + dataV.get(i) + "\")";
			}
			paramHash.put("FILTER_VALUES", valueFill);
			paramHash.put("System",  key);
			query = Utility.fillParam(query, paramHash);
			ArrayList <Object[]> list = retListFromQuery(query);
			totalList.addAll(list);
			//in this case, both provider and consumer are in this list, so the next function will separate that
			addToValueHashForData("data", list);
		}
		

	}

	/**
	 * Updates retValues hashtable by factoring in hourly rate into the LOE for generic costs.
	 * @param key 	String used to get original values from the hashtable. 
	 * @param list 	ArrayList of systems and related cost information.
	 */
	public void addToValueHashForGeneric(String key, ArrayList <Object[]> list)
	{
		Vector listVector = new Vector();
		double totalLOE = 0.0;
		int inputIdx = 1;
		int sysIdx = 2;
		int serIdx = 3;
		int loeIdx = 4;
		for (int i = 0; i<list.size();i++)
		{
			Object[] listArray = list.get(i);
			String item = (String)listArray[serIdx] + ":"+(String)listArray[inputIdx];
			if (!listVector.contains(item))
			{
				listVector.addElement(item);
				totalLOE=totalLOE+(Double)listArray[loeIdx];
			}
			else
			{
				totalLOE=totalLOE+(Double)listArray[loeIdx];
			}
		}
		retValues.put(key, listVector);
		retValues.put(key+"Cost", totalLOE*hourlyRate);
	}
	
	/**
	 * Updates retValues hashtable by factoring in hourly rate into the LOE for BLUs.
	 * @param key 	String used to get original values from the hashtable. 
	 * @param list 	ArrayList of systems and related cost information.
	 */
	public void addToValueHashForBLU(String key, ArrayList <Object[]> list)
	{
		Vector listVector = new Vector();
		double totalLOE = 0.0;
		int inputIdx = 1;
		int sysIdx = 2;
		int serIdx = 3;
		int loeIdx = 4;
		for (int i = 0; i<list.size();i++)
		{
			Object[] listArray = list.get(i);
			String item = (String)listArray[sysIdx] + ":"+(String)listArray[serIdx] + ":"+(String)listArray[inputIdx];
			if (!listVector.contains(item))
			{
				listVector.addElement(item);
				totalLOE=totalLOE+(Double)listArray[loeIdx];
			}
			else
			{
				totalLOE=totalLOE+(Double)listArray[loeIdx];
			}
		}
		retValues.put(key, listVector);
		retValues.put(key+"Cost", totalLOE*hourlyRate);
	}
	
	/**
	 * Updates retValues hashtable by factoring in hourly rate into the LOE.
	 * @param key 	String used to get original values from the hashtable. 
	 * @param list 	ArrayList of systems and related cost information.
	 */
	public void addToValueHashForData(String key, ArrayList <Object[]> list)
	{

		dataFedListVector = (Vector) retValues.get(key+"Fed");
		dataFedTotalLOE = ((Double)(retValues.get(key+"FedCost"))).doubleValue();
		dataConListVector = (Vector) retValues.get(key+"Con");
		dataConTotalLOE = ((Double)(retValues.get(key+"ConCost"))).doubleValue();
		
		int inputIdx = 1;
		int sysIdx = 2;
		int serIdx = 3;
		int loeIdx = 4;
		int glTagIdx = 6;
		for (int i = 0; i<list.size();i++)
		{
			Object[] listArray = list.get(i);
			String type = (String) listArray[glTagIdx];
			String item = (String)listArray[sysIdx] + ":"+(String)listArray[serIdx] + ":"+(String)listArray[inputIdx];
			if (type.equals("Provider")&&!dataFedListVector.contains(item))
			{
				dataFedListVector.addElement(item);
				dataFedTotalLOE = dataFedTotalLOE+(Double)listArray[loeIdx]*hourlyRate;
			}
			else if (type.equals("Provider"))
			{
				dataFedTotalLOE = dataFedTotalLOE+(Double)listArray[loeIdx]*hourlyRate;
			}
			if (type.equals("Consumer")&&!dataConListVector.contains(item))
			{
				dataConListVector.addElement(item);
				dataConTotalLOE = dataConTotalLOE+(Double)listArray[loeIdx]*hourlyRate;
			}
			else if (type.equals("Consumer"))
			{
				dataConTotalLOE = dataConTotalLOE+(Double)listArray[loeIdx]*hourlyRate;
			}
		}
		retValues.put(key+"Fed", dataFedListVector);
		retValues.put(key+"Con", dataConListVector);
		retValues.put(key+"FedCost", dataFedTotalLOE);
		retValues.put(key+"ConCost", dataConTotalLOE);
	}
	
	/**
	 * Gets the system-data mapping specified in an ICD using the upstream and downstream relationships.
	
	 * @return Hashtable	Contains information about system-data mapping. */
	public Hashtable getSysDataMappingFromICD ()
	{
		Hashtable retHash = new Hashtable();
		
		Vector <SEMOSSEdge> upstreamList = oldPlaySheet.filterData.getEdges("Provide");
		Vector <SEMOSSEdge> downstreamList = oldPlaySheet.filterData.getEdges("Consume");
		for (int i = 0; i<upstreamList.size(); i++)
		{
			
			String edgeName = (String) upstreamList.get(i).getProperty(Constants.EDGE_NAME);
			String[] edgeNameSplit = edgeName.split(":");
			String sysName = edgeNameSplit[0];
			if (retHash.containsKey(sysName))
			{
				Vector v = (Vector) retHash.get(sysName);
				String icdName = edgeNameSplit[1];
				String[] icdSplit = icdName.split("-");
				String data = icdSplit[icdSplit.length-1];
				if (!v.contains(data))
				{
					v.addElement(data);
					retHash.put(sysName,  v);
				}
			}
			else
			{
				Vector v = new Vector();
				String icdName = edgeNameSplit[1];
				String[] icdSplit = icdName.split("-");
				String data = icdSplit[icdSplit.length-1];
				v.addElement(data);
				retHash.put(sysName,  v);
			}
		}
		
		for (int i = 0; i<downstreamList.size(); i++)
		{
			
			String edgeName = (String) downstreamList.get(i).getProperty(Constants.EDGE_NAME);
			String[] edgeNameSplit = edgeName.split(":");
			String sysName = edgeNameSplit[1];
			if (retHash.containsKey(sysName))
			{
				Vector v = (Vector) retHash.get(sysName);
				String icdName = edgeNameSplit[0];
				String[] icdSplit = icdName.split("-");
				String data = icdSplit[icdSplit.length-1];
				if (!v.contains(data))
				{
					v.addElement(data);
					retHash.put(sysName,  v);
				}
			}
			else
			{
				Vector v = new Vector();
				String icdName = edgeNameSplit[0];
				String[] icdSplit = icdName.split("-");
				String data = icdSplit[icdSplit.length-1];
				v.addElement(data);
				retHash.put(sysName,  v);
			}
		}
		
		return retHash;
	}
	
	/**
	 * Given a query, returns an array list of results.
	 * @param query String
	
	 * @return ArrayList */
	public ArrayList retListFromQuery (String query)
	{
		ArrayList <Object []> list = new ArrayList();
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		
		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();*/
		// get the bindings from it

		int count = 0;
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				
				Object [] values = new Object[names.length];
				boolean filledData = true;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(sjss.getVar(names[colIndex]) != null)
					{
						values[colIndex] = sjss.getVar(names[colIndex]);
					}
					else {
						filledData = false;
						break;
					}
				}
				if(filledData)
					list.add(count, values);
				count++;
			}
		} 
		catch (RuntimeException e) {
			logger.debug(e);
		}
		
		return list;
	}
		
		
		
	/**
	 * Adds a new playsheet for transition analysis using the returned values hashtable.
	 */
	public void addTransitionSheet()
	{
		TransitionSheet sheet = new TransitionSheet(retValues);
		JScrollPane jsPane = new JScrollPane(sheet);
		newPlaySheet.jTab.add("Transition Analysis Sheet", jsPane);
	}
	/**
	 * Adds a new playsheet for transition analysis based on the graph value hash.
	 */
	public void addTransitionAnalysisSheet()
	{
		TransitionAnalyticsSheet sheet = new TransitionAnalyticsSheet(graphValueHash);
		JScrollPane jsPane = new JScrollPane(sheet);
		newPlaySheet.jTab.add("Transition Analytics Sheet",  jsPane);
	}
			
		
		


}
