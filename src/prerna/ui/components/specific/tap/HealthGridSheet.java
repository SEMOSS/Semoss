/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import java.util.Iterator;

import prerna.ds.BTreeDataFrame;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This class creates the application health grid.
 */
public class HealthGridSheet extends BrowserPlaySheet{

	private Boolean highlight=false;
	private String sysToHighlight="";
	Hashtable<String,Object> allHash;
	/**
	 * Constructor for HealthGridSheet.
	 */
	public HealthGridSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		//String htmlFileName = "/html/MHS-RDFSemossCharts/app/grid.html";
		//fileName = "file://" + workingDir + htmlFileName;
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/scatterplot.html";
	}
	/**
	 * Determines whether a system should be highlighted.
	 * @param shouldHighlight 	True if the system should be highlighted.
	 */
	public void setSystemHighlight(Boolean shouldHighlight){
		highlight =shouldHighlight;
	}
	/**
	 * Sets the system to be highlighted.
	 * @param system 	Name of system (string).
	 */
	public void setSystemToHighlight(String system){
		sysToHighlight = system;
	}

	/**
	 * Processes the query data to display information about the system and its associated business value, costs, and technical maturity.
	
	 * @return Hashtable<String,Object> */
	@Override
	public void processQueryData(){
		if(dataFrame == null || dataFrame.isEmpty()) {
			return;
		}
		
		//TODO: remove this logic once dataFrame.isEmpty() is implemented
		try {
			Iterator<Object[]> it = dataFrame.iterator(true);
		} catch(NullPointerException e) {
			return;
		}
		
		//list will be of the form: system/vendor, xname, yname1, yname2 size of circle,lifecycle
		ArrayList<Hashtable<String,Object>> allData = new ArrayList<Hashtable<String,Object>>();
		String[] names = this.dataFrame.getColumnHeaders(); 
		String xName = names[1];//BusinessValue
		String yName = names[2] + names[3] + names[4] + names[5];//Technical Maturity components
		String zName = names[6];//Cost

		double maxXAxis = 0.0;

		Iterator<Object[]> it = dataFrame.iterator(true);
		int counter = 0;
		while(it.hasNext()) {
			Hashtable<String,Object> elementHash = new Hashtable<String,Object>();
			Object[] listElement = it.next();
			
			elementHash.put("label", listElement[0]);//system
			elementHash.put("x", listElement[1]);//xvalue business value
			elementHash.put("y-external stability", listElement[2]);//yvalue1 
			elementHash.put("y-architectural complexity", listElement[3]);//yvalue2
			elementHash.put("y-information assurance", listElement[4]);//yvalue3
			elementHash.put("y-nonfunctional requirements", listElement[5]);//yvalue4
			elementHash.put("z", listElement[6]);//size cost
			elementHash.put("series", (listElement[7]).toString().replaceAll("\"", ""));//lifecycle
			allData.add(elementHash);

			if((Double)listElement[1]>maxXAxis)
				maxXAxis=(Double)listElement[1];
			
			if(counter==0&&listElement.length>8)
			{
				setSystemHighlight(true);
				setSystemToHighlight(listElement[8].toString());
			}
			counter++;
		}
		

		allHash = new Hashtable<String,Object>();
		allHash.put("dataSeries", allData);
		allHash.put("title", "Health Grid");
		allHash.put("xAxisTitle", xName);
		allHash.put("yAxisTitle",  "Technical Maturity");
		allHash.put("zAxisTitle",zName);
		
		if(highlight)
		{
			allHash.put("highlight", sysToHighlight);
		}
		
		allHash.put("xAxisMin", 0.01);
		
		//not sure if this is used?
		if(xName.toLowerCase().contains("score")||yName.toLowerCase().contains("score"))
		{
			allHash.put("xAxisMax", 10);
			allHash.put("xLineVal", 5);
		
		}
		else
		{
			allHash.put("xAxisMax",(int)maxXAxis + 1);
			allHash.put("xLineVal",maxXAxis/2);
		}
		allHash.put("yAxisMin", 0.01);
		allHash.put("yAxisMax", 10);
		allHash.put("yLineVal", 5);
		
		// We need to send a flag to HTML to hide Z values when the Z is sustainment budget and sustainment budget is a hidden property
		boolean showZ = checkIfShowZ();
		allHash.put("showZTooltip", showZ);
		
		this.dataHash = allHash;
	}
	
	/**
	 * Check if sustainment budget is part of the query and if type triple exists for the sustainment budget property.
	
	 * @return boolean	True if this is not a hidden property, show Z. */
	private boolean checkIfShowZ(){
		boolean retBool = true;
		
		// first need to determine if sustainment budget is part of the query-- we are only going to hide the z value if it is sustainment budget
		if(query.contains("Relation/Contains/SustainmentBudget")){
			// Next need to see if the type triple exists for the sustainment budget property (AKA <SustainmentBudget> <type> <Contains>)
			// If it does not exist, that means it is a hidden property and that we should not show Z
			String typeTripleCheckQuery = "SELECT ?s WHERE {" +
					"BIND(<http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> AS ?s)" +
					"BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?p) " +
					"BIND(<http://semoss.org/ontologies/Relation/Contains> AS ?o)" +
					"{?s ?p ?o}}";
			try{
				ISelectWrapper wrap = runQuery(typeTripleCheckQuery);
				if(wrap.hasNext())
					retBool = true;
				else
					retBool = false;
			}catch(RuntimeException e){
				retBool = false;
			}
		}
		
		return retBool;
	}

	/**
	 * Runs a query on a specified engine.
	 * @param query String
	
	 * @return SesameJenaSelectWrapper */
	private ISelectWrapper runQuery(String query){
	
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		wrapper.getVariables();
		*/
		return wrapper;
	}
	
	/**
	 * Method callIt.  Converts a given Hashtable to a Json and passes it to the browser.
	 * @param table Hashtable - the correctly formatted data from the SPARQL query results.
	 */
	public void clearTables()
	{
		output.clear();
		allHash.clear();
		dataHash.clear();
		dataFrame = new BTreeDataFrame(dataFrame.getColumnHeaders()); //TODO: is this the best way to clear the data-frame???
	}
	
	
	@Override
	public Hashtable<String, String> getDataTableAlign() {
		Hashtable<String, String> alignHash = new Hashtable<String, String>();
		String[] names = dataFrame.getColumnHeaders();
		alignHash.put("label", names[0]);
		for(int namesIdx = 1; namesIdx<names.length; namesIdx++){
			alignHash.put("value " + namesIdx, names[namesIdx]);
		}
		return alignHash;
	}
}
