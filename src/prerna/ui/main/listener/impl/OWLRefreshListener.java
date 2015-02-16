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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import prerna.ui.components.PropertySpecData;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * Controls the refreshing of the OWL.
 */
public class OWLRefreshListener implements IChakraListener {

	// quite simple
	// when refresh is pressed
	// get the active sheet and call the repaint

	static final Logger logger = LogManager.getLogger(OWLRefreshListener.class.getName());
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param actionevent ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		
		// puts the parent and the child predicate
		// it can be like Parent - BusinessProcess and Child - BusinessProcess/Anaesthesia
		// When the user selects this to be a property
		// I will remove the type-of relationship from the jenamodel
		// and insert child subproperty-of to contains - not the parent
		// because if I did to parent everything else would become a property too
		// however this is not attached to anything
		// 

		GraphPlaySheet ps = (GraphPlaySheet)QuestionPlaySheetStore.getInstance().getActiveSheet();

		// Step 1 : Establish Concept Model
		// converting something into a concept is fairly simple and straight forward
		// get all of the conceptHash
		// run through each one of them
		// if you dont see it in the prop Hash - remove it from the jenaModel
		// aggregate all the new concepts
		PropertySpecData data = ps.getGraphData().getPredicateData();
				
		// finish the subjects first
		logger.warn("Removing Subjects " + data.subject2bRemoved);
		ps.removeExistingConcepts(createTriples2Remove(data.subject2bRemoved.toString(), "<http://semoss.org/ontologies/Concept>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));

		logger.warn("Adding Subjects " + data.subject2bAdded);
		ps.addNewConcepts(data.subject2bAdded.toString(), "http://semoss.org/ontologies/Concept", RDF.TYPE + "");
		
		// finish the predicates next
		// this wont work since I am not giving it the hierarchy
		logger.warn("Removing Predicates " + data.pred2bRemoved);
		ps.removeExistingConcepts(createTriples2Remove(data.pred2bRemoved.toString(), "<http://semoss.org/ontologies/Relation>", "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>"));
		
		logger.warn("Adding Predicates " + data.pred2bAdded);
		String listOfChilds = ps.addNewConcepts(data.pred2bAdded.toString(), "http://semoss.org/ontologies/Relation", RDFS.subPropertyOf+"");
		
		logger.warn("List of childs " + listOfChilds);
		
		// do the property magic next
		//if(listOfChilds != null)
		//	ps.convertPropToConcept(listOfChilds);
		// finish the properties
		// this is interesting because the properties has to be contains relation
		logger.warn("Removing Properties " + data.prop2bRemoved);
		ps.removeExistingConcepts(createTriples2Remove(data.prop2bRemoved.toString(), "<http://semoss.org/ontologies/Relation/Contains>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));
		
		logger.warn("Adding Properties " + data.prop2bAdded);
		ps.addNewConcepts(data.prop2bAdded.toString(), "http://semoss.org/ontologies/Relation/Contains", RDF.TYPE+"");
		
		// this function will later be tied to save button
		//saveIt();
		
		// Step 2 - Converting a Relation to a Property
		// to Convert a relationship to a property
		// 1. query everything associate with the child element
		// 2. query for the base types
		// for each
		// 3. blow take it away
		// 4. insert with contains relationship
		// done for
		// 4. For each relations from step 1
		// 5. Remove it 
		// 6. Convert it into the appropriate contains relationship // ah just make it contains
		// 7. The base graph would now be all set
		
		// To Convert Property to a relationship
		// Seems like exact opposite of the previous

		// 
		
		// set the values from here just in case the user has edited it
		
		
		
		JTextField field = (JTextField)DIHelper.getInstance().getLocalProp(Constants.OBJECT_PROP_STRING);
		DIHelper.getInstance().putProperty(Constants.PREDICATE_URI, field.getText());

		field = (JTextField)DIHelper.getInstance().getLocalProp(Constants.DATA_PROP_STRING);
		DIHelper.getInstance().putProperty(Constants.PROP_URI, field.getText());

		JToggleButton overlay = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.APPEND);

		// need to see if there is a neater way to do this
		if(overlay.isSelected())
			QuestionPlaySheetStore.getInstance().getActiveSheet().overlayView();			
		else{
			((GraphPlaySheet)QuestionPlaySheetStore.getInstance().getActiveSheet()).clearStores();
			QuestionPlaySheetStore.getInstance().getActiveSheet().refineView();
		}
	}
	
	/**
	 * Method createTriples2Remove.  Creates the triples to remove.
	 * @param subjects String
	 * @param parentObj String
	 * @param predicate String
	
	 * @return Vector<String> */
	private Vector<String> createTriples2Remove(String subjects, String parentObj, String predicate)
	{
		Vector <String> subVector = new Vector<String>();
		
		String deleteQuery = "DELETE  DATA {" +
			"<child> " + predicate + " <parent>; " +
		"}";

		String deleteQuery2 = "DELETE  DATA {" +
		"<child> " + predicate + "  " + parentObj + "; " +
	"}";
		
		StringTokenizer tokenz = new StringTokenizer(subjects, ";");
		
		while(tokenz.hasMoreTokens())
		{
			String deleter = tokenz.nextToken();
			
			String parent = deleter.substring(0,deleter.indexOf("@@"));
			String child = deleter.substring(deleter.indexOf("@@") + 2);
			
			String q = deleteQuery.replace("child", child);
			q = q.replace("parent", parent);
		
			subVector.addElement(q);
			logger.info(" Query....  "+ q);
			q = deleteQuery2.replace("child", child);
			subVector.add(q);
		}
		return subVector;
	}	
	
	// temporary placeholder
	// once complete, this will be written into the prop file back
	/**
	 * Method saveIt.  Saves the configuration of an engine given a certain graph play sheet.
	 */
	public void saveIt()
	{
		GraphPlaySheet ps = (GraphPlaySheet)QuestionPlaySheetStore.getInstance().getActiveSheet();

		String engineName = ps.engine.getEngineName();
		// get the core properties
		ps.exportDB();
		ps.engine.saveConfiguration();
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {

	}
}
