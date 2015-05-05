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
package prerna.ui.components.specific.cbp;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;

/**
 * Sets parameters for the graph playsheet specifically working with time down.
 */
public class GraphPlaySheetTimeDownParameters extends GraphPlaySheet {

	private static final Logger logger = LogManager.getLogger(GraphPlaySheetTimeDownParameters.class.getName());
	public int timeDown1;
	public int timeDown2=-1;
	public int timeDown3=-1;
	public int timeDown4=-1;
	
	/**
	 * Constructor for GraphPlaySheetTimeDownParameters.
	 */
	public GraphPlaySheetTimeDownParameters() {
		super();
	}

	/**
	 * Creates the forest based on information about time down.
	 * Gets information about arrival time and resolved time and stores information about these nodes and edges.
	 */
	@Override
	public void createForest() {

		super.createForest();
//		Hashtable<String, SEMOSSVertex> myVertStore = this.getGraphData().getVertStore();
//		Hashtable<String, SEMOSSEdge> myEdgeStore = this.getGraphData().getEdgeStore();

		Hashtable<String, SEMOSSVertex> myVertStore = gdm.getVertStore();
		Hashtable<String, SEMOSSEdge> myEdgeStore = gdm.getEdgeStore();
		
		Enumeration keyList = myVertStore.keys();
		if(myEdgeStore.keys().hasMoreElements())
		{
			
		while(keyList.hasMoreElements()) {
			String currKey = (String) keyList.nextElement();
			SEMOSSVertex vert1 = myVertStore.get(currKey);
			SEMOSSVertex vert2 = null;
			SEMOSSEdge edge = null;
			String TimeDownType = "";
			String predicate = "http://semoss.org/ontologies/Relation/Contains/";	
			DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Calendar tempDate = Calendar.getInstance();
			
			if(vert1.getProperty(Constants.VERTEX_TYPE).toString().equals("CaseID"))
			{
				if(vert1.getProperty("ArrivalTime")!=null&&vert1.getProperty("ResolvedTime")!=null)
				{
					String start=vert1.getProperty("ArrivalTime").toString();
					String end=vert1.getProperty("ResolvedTime").toString();
					logger.info("StartDate: "+start);
					Date startDate = null;
					try {
						startDate = dfm.parse(start.substring(1,11)+" "+start.substring(12,20));
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Date endDate = null;
					try {
						endDate = dfm.parse(end.substring(1,11)+" "+end.substring(12,20));
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
						
					tempDate.setTime(startDate);
					tempDate.add(Calendar.HOUR,timeDown1);

					if(tempDate.getTime().after(endDate))
						TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Under_"+timeDown1+"_Hours";
					else
					{
						tempDate.add(Calendar.HOUR, timeDown2-timeDown1);
						if(timeDown2==-1)
							TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Over_"+timeDown1+"_Hours";
						if(tempDate.getTime().after(endDate))
							TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/"+timeDown1+"_to_"+timeDown2+"_Hours";
						else
						{
							tempDate.add(Calendar.HOUR, timeDown3-timeDown2);
							if(timeDown3==-1)
								TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Over_"+timeDown2+"_Hours";
							else if(tempDate.getTime().after(endDate))
								TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/"+timeDown2+"_to_"+timeDown3+"_Hours";
							else
							{
								tempDate.add(Calendar.HOUR, timeDown4-timeDown3);
								if(timeDown4==-1)
									TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Over_"+timeDown3+"_Hours";
								else if(tempDate.getTime().after(endDate))
									TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/"+timeDown3+"_to_"+timeDown4+"_Hours";
								else
									TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Over_"+timeDown4+"_Hours";
							}

						}
							
					}
				}
					else
						TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Indeterminable";

					vert2=myVertStore.get(TimeDownType+"");
					if(vert2==null)
					{
						vert2 = new SEMOSSVertex(TimeDownType);
						processControlData(vert2);
						filterData.addVertex(vert2);
						forest.addVertex(vert2);
						graph.addVertex(vert2);
					}
					
					myVertStore.put(TimeDownType, vert2);
					predicate=predicate+vert1.getProperty(Constants.VERTEX_NAME)+":"+vert2.getProperty(Constants.VERTEX_NAME);
					edge = new SEMOSSEdge(vert1, vert2, predicate);
					myEdgeStore.put(predicate, edge);
					forest.addEdge(edge,vert1,vert2);
					processControlData(edge);
					filterData.addEdge(edge);
					predData.addPredicateAvailable(edge.getURI());
					predData.addConceptAvailable(vert1.getURI());
					predData.addConceptAvailable(vert2.getURI());
					
					//add to simple graph
					graph.addVertex(vert1);
					graph.addVertex(vert2);
					if(vert2 != vert1) // loops not allowed in simple graph... can we get rid of this simple grpah entirely?
						graph.addEdge(vert1, vert2, edge);

//					genControlData(vert2);
//					genControlData(edge);

					genAllData();
			}
			
		} 
		}
//		genBaseConcepts();
//		genBaseGraph();
//		genAllData();	
	}
	/**
	 * Sets the query given information about the time down.
	 * @param query 	Query, in string form.
	 */
	@Override
	public void setQuery(String query) {
		logger.info("New Query " + query);
		int semi1=query.indexOf(";");
		int semi2=query.indexOf(";",semi1+1);
		int semi3=query.indexOf(";",semi2+1);
		int semi4=query.indexOf(";",semi3+1);
		if(!query.substring(0,9).equals("CONSTRUCT")&&!query.substring(0,6).equals("SELECT"))
		{
		timeDown1= Integer.parseInt(query.substring(0,semi1));
		if(!query.substring(semi1+1,semi2).equals("none"))
			timeDown2= Integer.parseInt(query.substring(semi1+1,semi2));
		if(!query.substring(semi2+1,semi3).equals("none"))
			timeDown3= Integer.parseInt(query.substring(semi2+1,semi3));
		if(!query.substring(semi3+1,semi4).equals("none"))
			timeDown4= Integer.parseInt(query.substring(semi3+1,semi4));
		this.query = query.substring(semi4+1);
		}
		else
			this.query=query;
	}

}
