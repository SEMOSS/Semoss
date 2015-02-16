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

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;

/**
 * Creates a graph playsheet specifically for time down.
 */
public class GraphPlaySheetTimeDown extends GraphPlaySheet {

	public int year;
	public int month;
	
	/**
	 * Constructor for GraphPlaySheetTimeDown.
	 */
	public GraphPlaySheetTimeDown() {
		super();
	}

	/**
	 * Creates the forest based on information about time down.
	 * This includes getting properties about arrival time and resolved time and comparing the amount of time down.
	 */
	@Override
	public void createForest() {

		super.createForest();
		Hashtable<String, SEMOSSVertex> myVertStore = this.getGraphData().getVertStore();
		Hashtable<String, SEMOSSEdge> myEdgeStore = this.getGraphData().getEdgeStore();
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
					tempDate.add(Calendar.HOUR,1);

					if(tempDate.getTime().after(endDate))
						TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Under_One_Hour";
					else
					{
						tempDate.add(Calendar.HOUR, 3);
						if(tempDate.getTime().after(endDate))
							TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/One_to_Four_Hours";
						else
						{
							tempDate.add(Calendar.HOUR, 9);
							if(tempDate.getTime().after(endDate))
								TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Four_to_Twelve_Hours";
							else
								TimeDownType="http://semoss.org/ontologies/Concept/TimeDown/Over_Twelve_Hours";

						}
							
					}
				
				vert2=myVertStore.get(TimeDownType+"");
				if(vert2==null)
				{
					vert2 = new SEMOSSVertex(TimeDownType);
					filterData.addVertex(vert2);
				}
				
				myVertStore.put(TimeDownType, vert2);
				predicate=predicate+vert1.getProperty(Constants.VERTEX_NAME)+":"+vert2.getProperty(Constants.VERTEX_NAME);
				edge = new SEMOSSEdge(vert1, vert2, predicate);
				myEdgeStore.put(predicate, edge);
				this.forest.addEdge(edge,vert1,vert2);
//				genControlData(vert2);
//				genControlData(edge);
				}
			}
			
		} 
		}
//		genBaseConcepts();
//		genBaseGraph();
//		genAllData();	
	}
	

}
