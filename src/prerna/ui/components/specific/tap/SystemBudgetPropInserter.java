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

import java.util.ArrayList;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.UpdateProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * 
 */
public class SystemBudgetPropInserter {

	Logger logger = Logger.getLogger(getClass());
	String query;
	String propName = "SustainmentBudget";
	SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
	String tapEngineName = "TAP_Core_Data";
	Boolean hideProp = true;
	
	//this function will run the query
	//step through the results of the query and put it into an insert query
	//run the insert query
	/**
	 * Executes insert queries in order to insert system sustainment budget values.
	 */
	public void runInsert(){
		//Show popup to determine whether the property will be hidden or not
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		Object[] buttons = {"Hide Property", "Do Not Hide"};
		int response = JOptionPane.showOptionDialog(playPane, "Would you like to insert this as a hidden property?\nHidden properties will only show up if specifically queried for", 
				"Hidden Property", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE, null, buttons, buttons[1]);
		if (response == 1) hideProp = false;
		logger.info("Beginning sustainment budget insert functionality");
		runQuery();
		logger.info("Got budget numbers");
		logger.info("Begin to put together insert query");
		String insertQuery = prepareInsertQuery();
		logger.info("Got insert query");
		logger.info("Begin to run insert");
		
		//run the insert
		UpdateProcessor proc = new UpdateProcessor();
		proc.setEngine((IEngine) DIHelper.getInstance().getLocalProp(tapEngineName));
		proc.setQuery(insertQuery);
		proc.processQuery();
		logger.info("Insert complete");
	}
	
	/**
	 * Prepares insert query to be executed.
	 * 
	 * @return String	Prepared query
	 */
	private String prepareInsertQuery(){
		//Set up the insert query
		//this is the pred URI that will be used to relate each system to its budget
		//First need to set up the pred URI as a property etc.
		String predUri = "<http://semoss.org/ontologies/Relation/Contains/"+propName+">";
		//start with type triple
		String insertQuery = "INSERT DATA { ";
		if(!hideProp){
			insertQuery = insertQuery + predUri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://semoss.org/ontologies/Relation/Contains>. ";
		}
		//add other type triple
		insertQuery = insertQuery + predUri +" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>. ";
		//add sub property triple -->>>>>>> should probably look into this.... very strange how other properties are set up
		insertQuery = insertQuery + predUri +" <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
				predUri + ". ";

		// get the bindings from it
		String [] names = wrapper.getVariables();
		int count = 0;
		ArrayList <Object []> list = new ArrayList();
		
		while(wrapper.hasNext()){

			SesameJenaSelectStatement sjss = wrapper.next();
			
			Object [] values = new Object[names.length];
			for(int colIndex = 0;colIndex < names.length;colIndex++)
			{
				values[colIndex] = sjss.getVar(names[colIndex]);
				logger.debug("Binding Name " + names[colIndex]);
				logger.debug("Binding Value " + values[colIndex]);
			}
			
			//add to insert query
			String subjectUri = "<http://health.mil/ontologies/Concept/System/"+values[0] +">";
			String objectUri = "\"" + values[1] + "\"" + "^^<http://www.w3.org/2001/XMLSchema#double>";
			int unk = -1;
			if (values[1]==null)
				objectUri = "\"" + unk + "\"" + "^^<http://www.w3.org/2001/XMLSchema#double>";
			insertQuery = insertQuery + subjectUri + " " + predUri + " " + objectUri + ". ";
			
			logger.debug("Creating new Value " + values);
			list.add(count, values);
			count++;
		}
		//close the insert query
		insertQuery = insertQuery + "}";
		
		return insertQuery;
	}
	
	/**
	 * Executes class level query var
	 */
	private void runQuery(){
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
			wrapper.setEngine(selectedEngine);
			wrapper.setQuery(query);
			wrapper.executeQuery();
		}
	}
	
	/**
	 * Sets class level var query
	 * 
	 * @param q String	Value of query to be set
	 */
	public void setQuery(String q){
		query = q;
	}
}
