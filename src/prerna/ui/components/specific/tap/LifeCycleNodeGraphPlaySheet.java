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

import java.text.DateFormatSymbols;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Hashtable;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;

/**
 * This class creates the graph playsheet for lifecycle nodes.
 */
public class LifeCycleNodeGraphPlaySheet extends GraphPlaySheet {

	public int year;
	public int month;
	
	/**
	 * Constructor for LifeCycleNodeGraphPlaySheet.
	 */
	public LifeCycleNodeGraphPlaySheet() {
		super();
	}

	/**
	 * Creates the forest for the life cycle types.
	 * These options include retired, sunset, supported, or generally available.
	 */
	@Override
	public void createForest() throws Exception {

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
			String lifeCycleType ="http://health.mil/ontologies/Concept/LifeCycle/";
			String predicate = "http://health.mil/ontologies/Relation/Contains/";
			int currYear=year;
			int currMonth= month;

			if(vert1.getProperty(Constants.VERTEX_TYPE).toString().equals("HardwareVersion")||vert1.getProperty(Constants.VERTEX_TYPE).toString().equals("SoftwareVersion"))
			{
				if(vert1.getProperty("EOL")!=null)
				{
					String date=vert1.getProperty("EOL").toString();
					int year=Integer.parseInt(date.substring(1,5));
					int month=Integer.parseInt(date.substring(6,8));
					int day=Integer.parseInt(date.substring(9,11));
					

					if((year<currYear)||(year==currYear && month<=currMonth+6)||(year==currYear+1&&month<=currMonth+6-12))
						lifeCycleType+="Retired_(Not_Supported)";
					else if(year<=currYear||(year==currYear+1&&month<=currMonth))
						lifeCycleType+="Sunset_(End_of_Life)";
					else if(year<=currYear+2||(year==currYear+3&&month<=currMonth))
						lifeCycleType+="Supported";
					else
						lifeCycleType+="GA_(Generally_Available)";

				}
				else
					lifeCycleType+="TBD";
				vert2=myVertStore.get(lifeCycleType+"");
				if(vert2==null)
					vert2 = new SEMOSSVertex(lifeCycleType);
				
				predicate=predicate+vert1.getProperty(Constants.VERTEX_NAME)+":"+vert2.getProperty(Constants.VERTEX_NAME);
				edge = new SEMOSSEdge(vert1, vert2, predicate);

				myVertStore.put(vert2.getProperty(Constants.URI) + "",vert2);
				myEdgeStore.put(edge.getProperty(Constants.URI)+"",edge);
				
				}
			}
		}
		super.createForest();

	}
	
	/**
	 * Sets the string version of the SPARQL query on the playsheet. 
	 * @param query String
	 */
	@Override
	public void setQuery(String query) {
		if(query.startsWith("SELECT")||query.startsWith("CONSTRUCT"))
			this.query=query;
		else{
		logger.info("New Query " + query);
		int semi=query.indexOf(";");
		int semi2=query.indexOf(";",semi+1);
		Calendar now = Calendar.getInstance();
		year=now.get(Calendar.YEAR); //replace with current year
		if(!query.substring(0,semi).equals("Today"))
			year = Integer.parseInt(query.substring(0,semi));
		month = getIntForMonth(query.substring(semi+1,semi2));
		if(month==-1)
			month=now.get(Calendar.MONTH);
		this.query = query.substring(semi2+1);
		}
	}
	
	/**
	 * Reads in a date format symbol and gets the months.
	 * The string form of the month is checked against the name of months and returns the integer form.
	 * @param m 		Month in string form.
	
	 * @return int		Month in integer form. */
	public int getIntForMonth(String m) {
	    DateFormatSymbols dfs = new DateFormatSymbols();
	    String[] months = dfs.getMonths();
	    int i=0;
	    while(i<12)
	    {
	    	if(m.equals(months[i]))
	    		return i;
	    	i++;
	    }
    	return -1;
	}


}
