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
 * Controls the selection of the object type for the export data section of the db modification tab.
 */
public class ObjectNodeTypeSelectionListener extends AbstractListener {

	static final Logger logger = LogManager.getLogger(ObjectNodeTypeSelectionListener.class.getName());
	String subjectNodeType = "";
	String objectNodeType = "";
	int exportNo = 1;
	private static final String NONE_SELECTED = "None";
	private static final String RELATION_URI = "http://semoss.org/ontologies/Relation/";
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		JComboBox source = (JComboBox) arg0.getSource();
		int length = source.getName().length();
		this.exportNo = Integer.parseInt(source.getName().substring(length-1, length));
		updateComboBoxes();
	}
	
	/**
	 * Method updateFromSubjectNodeTypeComboBox.
	 * @param exportNo int
	 */
	public void updateFromSubjectNodeTypeComboBox(int exportNo) {
		this.exportNo = exportNo;
		updateComboBoxes();
	}
	
	private void updateComboBoxes() {
		JComboBox subjectNodeTypeComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX + this.exportNo);
		JComboBox objectNodeTypeComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + this.exportNo);
		if(subjectNodeTypeComboBox.getSelectedItem() != null && objectNodeTypeComboBox.getSelectedItem() != null) {
			this.subjectNodeType = subjectNodeTypeComboBox.getSelectedItem().toString();
			this.objectNodeType = objectNodeTypeComboBox.getSelectedItem().toString();
		}
		
		runQuery(this.subjectNodeType, this.objectNodeType);
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
	}
	
	/**
	 * Method populateRelationshipComboBox.
	 * @param values Object[]
	 */
	private void populateRelationshipComboBox(Object[] values) {
		JComboBox nodeRelationshipComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + this.exportNo);
		DefaultComboBoxModel model = new DefaultComboBoxModel(values);
		nodeRelationshipComboBox.setModel(model);
		nodeRelationshipComboBox.setEditable(false);
	}

	/**
	 * Method runQuery.  Runs the specified query.
	 * @param subjectNodeType String
	 * @param objectNodeType String
	 */
	private void runQuery(String subjectNodeType, String objectNodeType) {
		String query = "SELECT DISTINCT ?verb WHERE {" + 
				"{?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
		query += subjectNodeType;
		query += "> ;} ";
		
		if(this.NONE_SELECTED.equals(subjectNodeType)) {
			String[] noneValueArray = {"None"};
			populateRelationshipComboBox(noneValueArray);
		} else {
			query += "{?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
			query += objectNodeType;
			query += "> ;} ";
		}
		
		query += "{?in ?relationship ?out ;} {?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?verb } }";
		
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
					Object var = sjss.getRawVar(names[colIndex]);
					if(var != null) {
						if(var.toString() != null && var.toString().contains(this.RELATION_URI)) {
							if(!var.toString().replace(this.RELATION_URI, "").contains("/")) {
								if(colIndex == 1) {
									properties.add((String) var.toString().replace(this.RELATION_URI, ""));
								}
								values[colIndex] = var.toString().replace(this.RELATION_URI, "");
							} else {
								filledData = false;
								break;
							}
						}
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
				if(array[0] != null) {
					objectNodeTypes.add(array[0].toString());
				}
			}
		} else {
			objectNodeTypes.add(0, "None");
		}
		
		populateRelationshipComboBox(objectNodeTypes.toArray());
	}
	
}
