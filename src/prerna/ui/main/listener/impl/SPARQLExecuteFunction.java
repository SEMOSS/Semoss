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
package prerna.ui.main.listener.impl;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.impl.SesameJenaUpdateWrapper;
import prerna.ui.components.playsheets.GraphPlaySheet;

import com.google.gson.Gson;

/**
 */
public class SPARQLExecuteFunction extends AbstractBrowserSPARQLFunction {
	
	GraphPlaySheet gps;
	
	/**
	 * Method invoke.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public Object invoke(Object... arg0) {
		Hashtable retHash = new Hashtable();
		ArrayList ret = new ArrayList();
		boolean success = true;
		logger.info("Arguments are " + arg0);
		
		//get the query from the args
		String query = ((String) arg0[0]).trim(); 
		
		//run the query against the set repository
		try {
			logger.info("Using repository " + engine);
			
			if(query.toUpperCase().startsWith("INSERT") || query.toUpperCase().startsWith("DELETE")){
				logger.info("running update : " + query);
				success = processUpdate(query, engine);
				
				// if a playsheet has been set to this function, run this update against the playsheet so that we can refresh the graph
				if(gps!=null)
					gps.getGraphData().updateAllModels(query);
			}

			else if(query.startsWith("SELECT") ){
				logger.info("running select : " + query);
				ret = processSelect(query, engine);
			}
			
			else { 
				System.err.println("UNKNOWN QUERY TYPE SENT TO JAVA FOR PROCESSING");
			}
		} catch (Exception e) {
			logger.fatal(e);
			success = false;
		}

		Object[] retArray = ret.toArray();
		retHash.put("results", retArray);
		retHash.put("success", success);

		Gson gson = new Gson();
        
		return gson.toJson(retHash);
	}
	
	/**
	 * Method processUpdate.
	 * @param query String
	 * @param selectedEngine IEngine
	
	 * @return boolean */
	private boolean processUpdate(String query, IEngine selectedEngine){
		//create the update wrapper, set the variables, and let it run
		SesameJenaUpdateWrapper wrapper = new SesameJenaUpdateWrapper();
		wrapper.setEngine(selectedEngine);
		wrapper.setQuery(query);
		boolean success = wrapper.execute();
		return success;
	}
	/**
	 * Method processSelect.
	 * @param query String
	 * @param selectedEngine IEngine
	
	 * @return ArrayList<Object[]> */
	private ArrayList<Object[]> processSelect(String query, IEngine selectedEngine){
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		
		//create the update wrapper, set the variables, and let it run
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setEngine(selectedEngine);
		wrapper.setQuery(query);
		wrapper.executeQuery();

		// get the bindings from it
		String [] names = wrapper.getVariables();
		int count = 0;
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			
			Object [] values = new Object[names.length];
			for(int colIndex = 0;colIndex < names.length;colIndex++)
			{
				values[colIndex] = sjss.getRawVar(names[colIndex])+"";
			}
			list.add(count, values);
			count++;
		}
		return list;
	}
	
	/**
	 * Method setGps.
	 * @param gps GraphPlaySheet
	 */
	public void setGps(GraphPlaySheet gps){
		this.gps = gps;
	}
}
