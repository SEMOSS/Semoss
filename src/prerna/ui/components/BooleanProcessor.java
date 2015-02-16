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
package prerna.ui.components;

import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaBooleanWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This class is used in various listeners to determine whether or not a query provides results.
 */
public class BooleanProcessor {

	static final Logger logger = LogManager.getLogger(BooleanProcessor.class.getName());
	String query;
	IEngine engine;
	
	/**
	 * Constructor for BooleanProcessor.
	 */
	public BooleanProcessor(){
		
	}
	
	//if an engine has been set, it will run the query on that engine
	//if an engine has not been set, it will run it on all selected engines
	
	/**
	 * Processes the query on a specific engine.
	
	 * @return ret	True if the query is returned. */
	public boolean processQuery(){
		boolean ret = false;

		if(engine==null){
			//get the selected repositories
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			Object [] repos = (Object [])list.getSelectedValues();
			
			//for each selected repository, run the query
			for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
			{
				//get specific engine
				IEngine selectedEngine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
				logger.info("Selecting repository " + repos[repoIndex]);
				
				//create the update wrapper, set the variables, and let it run
				SesameJenaBooleanWrapper wrapper = new SesameJenaBooleanWrapper();
				wrapper.setEngine(selectedEngine);
				wrapper.setQuery(query);
				ret = wrapper.execute();
				
			}
		}
		else {
			//create the update wrapper, set the variables, and let it run
			SesameJenaBooleanWrapper wrapper = new SesameJenaBooleanWrapper();
			wrapper.setEngine(engine);
			wrapper.setQuery(query);
			ret = wrapper.execute();
		}
		
		return ret;
	}
	
	/**
	 * Sets the engine for query to be executed upon.
	 * @param e	Engine.
	 */
	public void setEngine(IEngine e){
		engine = e;
	}
	
	/**
	 * Sets the query.
	 * @param q 	Query.
	 */
	public void setQuery(String q){
		query = q;
	}
	
}
