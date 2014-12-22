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
package prerna.ui.helpers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.ui.components.ParamComboBox;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This gets access to the engine and runs a query with parameters for a subclass, and helps appropriately process the results.
 */
public class EntityFillerForSubClass implements Runnable {
	public ArrayList<JComboBox> boxes;
	public String parent;
	public IEngine engine;
	public String engineName;
	String sparqlQuery = "SELECT ?entity WHERE {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <"; 
	
	/**
	  * Method run. Gets access to engine, gets the type query based on the type of engine, fills query parameters, and runs the query.
	 */
	@Override
	public void run() {
		Vector<String> names = new Vector<String>();

		String entityNS = DIHelper.getInstance().getProperty(parent);
		if (entityNS != null) {
			sparqlQuery = sparqlQuery + entityNS + "/" + parent + "> ;}";
			names = engine.getEntityOfType(sparqlQuery);
			if(engine instanceof AbstractEngine){
				Vector<String> baseNames = ((AbstractEngine)engine).getBaseDataEngine().getEntityOfType(sparqlQuery);
				for(String name: baseNames) 
					if(!names.contains(name)) 
						names.addAll(baseNames);
			}
			Collections.sort(names);
		}
		
		Hashtable paramHash = Utility.getInstanceNameViaQuery(names);
		//keys are the labels, objects are the URIs
		Set nameC = paramHash.keySet();
		Vector nameVector = new Vector(nameC);
		Collections.sort(nameVector);
		nameVector.add(0, "");
		
		for(JComboBox box : boxes) {
			if (box != null) {
				// if it is a paramcombobox, set the whole hashtable--will need to look up the URI for selected label later
				if(box instanceof ParamComboBox)
					((ParamComboBox)box).setData(paramHash, nameVector);
				// else just set the model on the box with the list
				else
				{
					DefaultComboBoxModel model = new DefaultComboBoxModel(nameVector);
					box.setModel(model);
				}
			}
		}
	}
}
