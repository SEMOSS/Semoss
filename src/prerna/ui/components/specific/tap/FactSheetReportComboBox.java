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

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ParamComboBox;
import prerna.ui.helpers.EntityFiller;
import prerna.util.DIHelper;

/**
 * Used to pick parameters for the fact sheet report.
 */
public class FactSheetReportComboBox extends ParamComboBox implements Runnable {
	
	Logger logger = Logger.getLogger(getClass());
	String key = "System";
	
	Object[] repos = new Object[1];

	/**
	 * Constructor for FactSheetReportComboBox.
	 * @param array String[]
	 */
	public FactSheetReportComboBox(String[] array) {
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
	 * Sets the key to look for.
	 * @param key 	Key to be set.
	 */
	public void setKey(String key){
		this.key = key;
	}
	/**
	 * Sets the name of the parameter to system.
	 * Gets access to the engine and runs a query given certain parameters.
	 */
	public void fillParam(){
//		String key = "System";
		// execute the logic for filling the information here
//		String entityType = "http://semoss.org/ontologies/Concept/System";
		String entityType = "http://semoss.org/ontologies/Concept/"+key;
		setParamName(key);
		
		setEditable(false);
		//PlayTextField pField = new PlayTextField(field);
		
		// get the selected repository
		/*Object [] repos = new Object[1];
		repos[0] = "TAP_Cost_Data";
		*/
		logger.info("Repository is " + repos);
		
		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
			logger.info("Repository is " + repos[repoIndex]);
			
			EntityFiller filler = new EntityFiller();
			filler.engineName = repos[repoIndex]+"";
			filler.engine = engine;
			filler.box = this;
			filler.type = entityType;
			Thread aThread = new Thread(filler);
			aThread.run();
		}
	}
	

}
