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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.main.NodeLoadingSheetWriter;
import prerna.poi.main.RelationshipLoadingSheetWriter;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.QueryProcessor;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Controls exporting of graph nodes into loading sheets.
 */
public class ExportDatabaseLoadSheetsListener implements IChakraListener {	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		
		//Get engine to export from
		JComboBox exportDataSourceComboBox = (JComboBox) Utility.getDIHelperLocalProperty(Constants.EXPORT_LOAD_SHEET_SOURCE_COMBOBOX);
		IDatabaseEngine engine = (IDatabaseEngine) Utility.getDIHelperLocalProperty(exportDataSourceComboBox.getSelectedItem().toString());

		//Get all concepts in the database
		String conceptQuery = "SELECT ?entity WHERE {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <" + Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "> ;} ORDERBY ?entity";
		ArrayList<String> conceptList = QueryProcessor.getStringList(conceptQuery, engine.getEngineId());

		//Export all properties in one excel file
		//One sheet for each concept with its properties
		//if concept does not have properties, will not have a sheet
		NodeLoadingSheetWriter conWriter = new NodeLoadingSheetWriter();
		conWriter.setShowSuccessMessage(false);
		conWriter.setWriteFileName(engine.getEngineId() + "_Property_LoadingSheet_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "") + ".xlsx");
		conWriter.writeNodeLoadingSheets(engine, conceptList);
		
		//Export all relationships in multiple files
		//One file for each concept as the subject node
		for(String inputConcept : conceptList) {
			ArrayList<String[]> relationshipList = getRelationshipListForConcept(engine, inputConcept);
			if(!relationshipList.isEmpty()) {
				RelationshipLoadingSheetWriter relWriter = new RelationshipLoadingSheetWriter();
				relWriter.setShowSuccessMessage(false);
				relWriter.setWriteFileName(engine.getEngineId() + "_" + inputConcept + "_Relations_LoadingSheet_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "") + ".xlsx");
				relWriter.writeRelationshipLoadingSheets(engine, relationshipList);
			}
		}

		Utility.showMessage("Exported nodes and properties successfully.");
		
	}
	
	private ArrayList<String[]> getRelationshipListForConcept(IDatabaseEngine engine, String concept) {

		ArrayList<String[]> relationshipList = new ArrayList<String[]>();
		String outConceptQuery = "SELECT DISTINCT ?s WHERE { {?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/" + concept + "> ;} {?s <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> ;}{?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?s ;} {?in ?p ?out ;} }";

		ISelectWrapper sjsw = Utility.processQuery(engine, outConceptQuery);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();

			String outConcept = sjss.getVar(values[0]).toString();
			if(!outConcept.equals("Concept")) {
				String relationshipQuery = "SELECT DISTINCT ?relationship WHERE {{?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/" + concept + "> ;} {?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/" + outConcept + "> ;} {?in ?relationship ?out ;} {?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?verb }}";// MINUS {?relationship <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>}
		
				ISelectWrapper sjsw2 = Utility.processQuery(engine, relationshipQuery);
				String[] values2 = sjsw2.getVariables();
				while (sjsw2.hasNext()) {
					ISelectStatement sjss2 = sjsw2.next();
					
					String verb = sjss2.getVar(values2[0]).toString();
					if(!verb.contains(":")&&!verb.equals("Relation")) {
					String[] relationshipArray = new String[]{concept, verb, outConcept};
					relationshipList.add(relationshipArray);
					}
				}
			}
		}

		return relationshipList;
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {	
	}
}
