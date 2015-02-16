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
package prerna.ui.helpers;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ParamComboBox;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This gets access to the engine and runs a query with parameters, and helps appropriately process the results.
 */
public class EntityFiller implements Runnable {
	
	static final Logger logger = LogManager.getLogger(EntityFiller.class.getName());

	public JComboBox box;
	public String type;
	public IEngine engine;
	public String engineName;
	public Vector<String> names;
	public String extQuery;
	public Vector<String> nameVector;
	public SEMOSSParam param;
	
	/**
	 * Method run. Gets access to engine, gets the type query based on the type of engine, fills query parameters, and runs the query.
	 */
	@Override
	public void run() {
//		logger.setLevel(Level.WARN);
		logger.debug(" Engine Name is  " + engineName);
		engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		names = new Vector<String>();
		
		if (box != null && type != null) {
			synchronized(box) {
				// if options for the parameter have been explicitly defined
				// use just those
				if(param!=null && !param.getOptions().isEmpty()){
					Vector<String> options = param.getOptions();
					for(String option : options) {
						names.addElement(option);
					}
					
					DefaultComboBoxModel model = new DefaultComboBoxModel(names);

					box.setModel(model);
					box.setEditable(false);
				}
				/*if(DIHelper.getInstance().getProperty(type + "_" + Constants.OPTION) != null) {
					// try to pick this from DBCM Properties table
					// this will typically be of the format
					String options = DIHelper.getInstance().getProperty(
							type + "_" + Constants.OPTION);
					// this is a string with ; delimited values
					StringTokenizer tokens = new StringTokenizer(options, ";");

					// sorry for the cryptic crap below
					int tknIndex = 0;
					for (; tokens.hasMoreTokens(); names.addElement(tokens
							.nextToken()), tknIndex++)
						;

					DefaultComboBoxModel model = new DefaultComboBoxModel(names);

					box.setModel(model);
					box.setEditable(false);
					//DIHelper.getInstance().setLocalProperty(type, names);
				}*/
				// the the param options have not been explicitly defined and the combo box has not been cached
				// time for the main processing
				else {//if (DIHelper.getInstance().getLocalProp(type) == null) {
					//check if URI is used in param filler
					/*if(!type.contains("http://")) {
						names.addElement("Incorrect Param Fill");
						DefaultComboBoxModel model = new DefaultComboBoxModel(names);
						box.setModel(model);
						box.notify();
						return;
					}*/
					// use the type query defined on RDF Map unless external query has been defined
					String sparqlQuery = DIHelper.getInstance().getProperty(
							"TYPE" + "_" + Constants.QUERY);

					Hashtable paramTable = new Hashtable();
					paramTable.put(Constants.ENTITY, type);
					if (extQuery!=null) {
						sparqlQuery=extQuery;
					} else if (param!=null) {
						sparqlQuery = param.getQuery();
						sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);	
					}
					else {
						sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);	
					}

					// get back all of the URIs that are of that type
					names = engine.getEntityOfType(sparqlQuery);				
					// try to query for the label
					logger.debug("Names " + names);
					Hashtable paramHash = Utility.getInstanceNameViaQuery(names);
					if (paramHash.isEmpty()) {
						names.addElement("Concept Doesn't Exist in DB");
						DefaultComboBoxModel model = new DefaultComboBoxModel(names);
						box.setModel(model);
						box.notify();
						return;
					}
					//keys are the labels, objects are the URIs
					Set nameC = paramHash.keySet();
					nameVector = new Vector(nameC);
					Collections.sort(nameVector);

					// if it is a paramcombobox, set the whole hashtable--will need to look up the URI for selected label later
					if(box instanceof ParamComboBox) {
						((ParamComboBox)box).setData(paramHash, nameVector);
					} else {
						// else just set the model on the box with the list
						DefaultComboBoxModel model = new DefaultComboBoxModel(nameVector);
						box.setModel(model);
					}

					box.setEditable(false);
				} /*else {
					names.addElement("Unknown");			
				}*/
				box.notify();
			}
		}
		// if the type is not null but their is no box to fill
		// fills the names array with all URIs of set type
		else if (type !=null) {
			//if (DIHelper.getInstance().getLocalProp(type) == null) {
				String sparqlQuery = DIHelper.getInstance().getProperty(
						"TYPE" + "_" + Constants.QUERY);
				Hashtable paramTable = new Hashtable();
				paramTable.put(Constants.ENTITY, type);
				if (extQuery!=null) {
					sparqlQuery=extQuery;
				} else {
					sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);	
				}

				names = engine.getEntityOfType(sparqlQuery);
				Collections.sort(names);
				Hashtable paramHash = Utility.getInstanceNameViaQuery(names);
				if (paramHash.isEmpty()) {
					names.addElement("Concept Doesn't Exist in DB");
					DefaultComboBoxModel model = new DefaultComboBoxModel(names);
					box.setModel(model);
					return;
				}
				//keys are the labels, objects are the URIs
				Set nameC = paramHash.keySet();
				nameVector = new Vector(nameC);
				Collections.sort(nameVector);
			//}
		}
	}

	/**
	 * Method setExternalQuery.  Sets the external query to the given SPARQL query.
	 * @param query String - The SPARQL query in string form that this external query is set to.
	 */
	public void setExternalQuery(String query)
	{
		this.extQuery = query;
	}
}
