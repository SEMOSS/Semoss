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
package prerna.ui.components.specific.cbp;

import java.text.DateFormat;
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
 * Sets parameters for the graph playsheet specifically working with time down.
 */
public class GraphPlaySheetTimeDownParameters extends GraphPlaySheet {

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
	public void createForest() throws Exception {

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
					logger.info("StartDate: "+start);
					Date startDate = dfm.parse(start.substring(1,11)+" "+start.substring(12,20));
					Date endDate = dfm.parse(end.substring(1,11)+" "+end.substring(12,20));
						
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
						filterData.addVertex(vert2);
					}
					
					myVertStore.put(TimeDownType, vert2);
					predicate=predicate+vert1.getProperty(Constants.VERTEX_NAME)+":"+vert2.getProperty(Constants.VERTEX_NAME);
					edge = new SEMOSSEdge(vert1, vert2, predicate);
					myEdgeStore.put(predicate, edge);
					this.forest.addEdge(edge,vert1,vert2);
//					genControlData(vert2);
//					genControlData(edge);


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
