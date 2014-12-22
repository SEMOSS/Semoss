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
package prerna.ui.main.listener.impl;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.impl.SesameJenaUpdateWrapper;
import prerna.ui.components.playsheets.GraphPlaySheet;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.JSValue;

/**
 */
public class SPARQLExecuteFunction extends AbstractBrowserSPARQLFunction {
	
	GraphPlaySheet gps;
	
	/**
	 * Method invoke.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public JSValue invoke(JSValue... arg0) {
		Hashtable retHash = new Hashtable();
		ArrayList ret = new ArrayList();
		boolean success = true;
		logger.info("Arguments are " + arg0);
		
		//get the query from the args
		String query = (arg0[0].getString()).trim(); 
		
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
		} catch (RuntimeException e) {
			logger.fatal(e);
			success = false;
		}

		Object[] retArray = ret.toArray();
		retHash.put("results", retArray);
		retHash.put("success", success);

		Gson gson = new Gson();
        
		return JSValue.create(gson.toJson(retHash));
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
