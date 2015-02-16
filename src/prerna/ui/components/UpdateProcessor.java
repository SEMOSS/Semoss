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
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaUpdateWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * The update processor class is used to process a query on a specific engine.
 */
public class UpdateProcessor {

	static final Logger logger = LogManager.getLogger(UpdateProcessor.class.getName());
	String query;
	IEngine engine;
	
	/**
	 * Constructor for UpdateProcessor.
	 */
	public UpdateProcessor(){
		
	}
	
	/**
	 * Runs the query on a specific engine.
	 */
	public void processQuery(){
		//if the engine has been set, it will run the query only on that engine
		//if the engine has not been set, it will run the query on all selected engines

		if(engine == null){
			//get the selected repositories
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			Object [] repos = (Object [])list.getSelectedValues();
			
			//for each selected repository, run the query
			for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
			{
				//get specific engine
				IEngine selectedEngine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
				logger.info("Selecting repository " + repos[repoIndex]);
				
				if(selectedEngine instanceof BigDataEngine) {
					BigDataEngine selectedEngineBigData = (BigDataEngine) selectedEngine;
					selectedEngineBigData.infer();
				}
				
				//create the update wrapper, set the variables, and let it run
				SesameJenaUpdateWrapper wrapper = new SesameJenaUpdateWrapper();
				wrapper.setEngine(selectedEngine);
				wrapper.setQuery(query);
				wrapper.execute();
				selectedEngine.commit();
				
			}
		}
		else {
			if(engine instanceof BigDataEngine) {
				BigDataEngine selectedEngineBigData = (BigDataEngine) engine;
				selectedEngineBigData.infer();
			}
			
			//create the update wrapper, set the variables, and let it run
			SesameJenaUpdateWrapper wrapper = new SesameJenaUpdateWrapper();
			wrapper.setEngine(engine);
			wrapper.setQuery(query);
			wrapper.execute();
			engine.commit();
		}
		
	}
	
	/**
	 * Sets the query.
	 * @param q 	Query, in string form.
	 */
	public void setQuery(String q){
		query = q;
	}
	
	/**
	 * Sets the engine.
	 * @param e 	Engine to be set.
	 */
	public void setEngine(IEngine e){
		engine = e;
	}
	
}
