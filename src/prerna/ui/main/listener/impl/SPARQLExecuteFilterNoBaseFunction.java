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
import prerna.ui.components.playsheets.GraphPlaySheet;

import com.google.gson.Gson;

/**
 */
public class SPARQLExecuteFilterNoBaseFunction extends AbstractBrowserSPARQLFunction {

	Hashtable filterHash;
	static final String concept = "http://semoss.org/ontologies/Concept";
	static final String relation = "http://semoss.org/ontologies/Relation";
	
	/**
	 * Method invoke.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public Object invoke(Object... arg0) {

		logger.info("Arguments are " + arg0);
		
		//get the query from the args
		String query = ((String) arg0[0]).trim(); 
		
		Hashtable retHash = process(query);

		Gson gson = new Gson();
        
		return gson.toJson(retHash);
	}
	
	public Hashtable process(String query){
		Hashtable retHash = new Hashtable();
		ArrayList ret = new ArrayList();
		boolean success = true;

		//run the query against the set repository
		try {
			logger.info("Using repository " + engine);
			
			if(query.toUpperCase().startsWith("INSERT") || query.toUpperCase().startsWith("DELETE")){
				logger.info("Incorrectly passed update query to FilterNoBaseFunction");
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
		
		return retHash;
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
			//return has to be ::::::::::::::  ?subjectType ?subject ?verb ?verbType ?objectType ?object
			Object [] values = new Object[names.length];
			boolean addRow = true;
			for(int colIndex = 0;colIndex < names.length;colIndex++)
			{
				String var = sjss.getRawVar(names[colIndex])+"";
				//just check subject verb and object because obviously everything else will be contained in the filterhash
				if(((colIndex == 1 || colIndex == 2 || colIndex == 5) && filterHash.containsKey(var)) || 
						((colIndex == 0 || colIndex == 3 || colIndex == 4) && !filterHash.containsKey(var)) || 
						((colIndex == 0 || colIndex == 4) && concept.equals(var)) || 
						(colIndex == 3 && relation.equals(var)))
				{
					addRow = false;
					break;
				}
				else
				{
					values[colIndex] = var;
				}
					
			}
			if(addRow)
			{
				list.add(count, values);
				count++;
			}
		}
		return list;
	}
	
	/**
	 * Method setFilterHash.
	 * @param filterHash Hashtable
	 */
	public void setFilterHash(Hashtable filterHash) {
		this.filterHash = filterHash;
	}

}
