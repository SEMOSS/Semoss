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
import java.util.ArrayList;
import java.util.HashSet;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls the selection of the subject type for the export data section of the db modification tab.
 */
public class SubjectNodeTypeSelectionListener extends AbstractListener {
	
	static final Logger logger = LogManager.getLogger(SubjectNodeTypeSelectionListener.class.getName());
	String subjectNodeType = "";
	int exportNo = 1;
	ActionEvent event;
	private static final String CONCEPT = "Concept";
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		JComboBox subjectNodeTypeComboBox = (JComboBox) arg0.getSource();
		int length = subjectNodeTypeComboBox.getName().length();
		this.exportNo = Integer.parseInt(subjectNodeTypeComboBox.getName().substring(length-1, length));
		this.subjectNodeType = subjectNodeTypeComboBox.getSelectedItem().toString();
		
		if(this.subjectNodeType != null && !this.subjectNodeType.equals("")) {
			runQuery(this.subjectNodeType);
		}
	}
	
	/**
	 * Method populateObjectComboBox.
	 * @param values Object[]
	 */
	private void populateObjectComboBox(Object[] values) {
		JComboBox objectNodeTypeComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + this.exportNo);
		DefaultComboBoxModel model = new DefaultComboBoxModel(values);
		objectNodeTypeComboBox.setModel(model);
		objectNodeTypeComboBox.setEditable(false);
		
		updateRelationshipComboBox();
	}
	
	/**
	 * Method updateRelationshipComboBox.
	 */
	private void updateRelationshipComboBox() {
		JComboBox objectNodeTypeComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + this.exportNo);
		if(objectNodeTypeComboBox.getSelectedItem() != null) {
			ObjectNodeTypeSelectionListener objectListener = new ObjectNodeTypeSelectionListener();
			objectListener.updateFromSubjectNodeTypeComboBox(this.exportNo);
		}
	}

	/**
	 * Method runQuery.  Runs the query given an input node type.
	 * @param nodeType String
	 */
	private void runQuery(String nodeType) {
		String query = "SELECT DISTINCT ?s WHERE { {?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
		query += nodeType;
		query += "> ;}";
		
		query += " {?s <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> ;}" 
				+ "{?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?s ;} {?in ?p ?out ;} }";
		
		JComboBox exportDataSourceComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_SOURCE_COMBOBOX);
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(exportDataSourceComboBox.getSelectedItem().toString());
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		*/
		
		int count = 0;
		String[] names = wrapper.getVariables();
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		HashSet<String> properties = new HashSet<String>();
		
		try {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				Object [] values = new Object[names.length];
				boolean filledData = true;
				
				for(int colIndex = 0;colIndex < names.length;colIndex++) {
					if(sjss.getVar(names[colIndex]) != null) {
						if(colIndex == 1) {
							properties.add((String) sjss.getVar(names[colIndex]));
						}
						values[colIndex] = sjss.getVar(names[colIndex]);
					}
					else {
						filledData = false;
						break;
					}
				}
				if(filledData) {
					list.add(count, values);
					count++;
				}
			}
		} 
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		
		ArrayList<String> objectNodeTypes = new ArrayList<String>();
		if(list.size() > 0) {
			for(Object[] array : list) {
				if(array[0] != null && !CONCEPT.equals(array[0])) {
					objectNodeTypes.add((String) array[0]);
				}
			}
		} else {
			objectNodeTypes.add(0, "None");
		}
		
		populateObjectComboBox(objectNodeTypes.toArray());
	}
	

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		
	}
}
