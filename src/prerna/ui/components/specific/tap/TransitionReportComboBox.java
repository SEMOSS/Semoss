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
 * Combo box specific to the Transition Report.
 */
public class TransitionReportComboBox extends ParamComboBox implements Runnable {
	
	Logger logger = Logger.getLogger(getClass());
	
	Object[] repos = new Object[1];
	
	/**
	 * Constructor for TransitionReportComboBox.
	 * 
	 * @param array String[]	Items populating the combobox
	 */
	public TransitionReportComboBox(String[] array) {
		super(array);
	}

	/**
	 * Inherited method - calls fillParam()
	 */
	public void run(){
		fillParam();
	}
	
	/**
	 * Sets engine as specified.
	 * 
	 * @param repo String	Name of repo to be set
	 */
	public void setEngine(String repo){
		repos[0] = repo;
	}
	
	/**
	 * Fills combobox with Systems using EntityFiller
	 */
	public void fillParam(){
		String key = "System";
		// execute the logic for filling the information here
		String entityType = "http://semoss.org/ontologies/Concept/System";
		setParamName(key);
		
		setEditable(false);
		//PlayTextField pField = new PlayTextField(field);
		
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
