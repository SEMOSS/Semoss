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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabase;
import prerna.ui.components.ParamComboBox;
import prerna.ui.helpers.EntityFiller;
import prerna.util.DIHelper;

/**
 * Used to set a list of systems as combo box.
 */
public class SystemListComboBox extends ParamComboBox implements Runnable {
	
	static final Logger logger = LogManager.getLogger(SystemListComboBox.class.getName());
	Object[] repos = new Object[1];

	/**
	 * Constructor for SystemListComboBox.
	 * @param array String[]
	 */
	public SystemListComboBox(String[] array) {
		super(array);
	}

	/**
	 * Runs the fillParam method and starts the thread.
	 */
	@Override
	public void run() {
		fillParam();
	}

	/**
	 * Sets the engine.
	 * @param repo 	Repository to be set.
	 */
	public void setEngine(String repo){
		repos[0] = repo;
	}


	/**
	 * Sets the name of the parameter to system.
	 * Gets access to the engine and runs a query given certain parameters.
	 */
	public void fillParam(){
		String key = "System";
		String entityType = "http://semoss.org/ontologies/Concept/"+key;
		setParamName(key);
		
		setEditable(false);

		logger.info("Repository is " + repos);
		
		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{
			IDatabase engine = (IDatabase)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
			logger.info("Repository is " + repos[repoIndex]);
			
			EntityFiller filler = new EntityFiller();
			filler.engineName = repos[repoIndex]+"";
			filler.engine = engine;
			filler.box = this;
			filler.type = entityType;
			Thread aThread = new Thread(filler);
			aThread.start();
		}
	}
	

}
