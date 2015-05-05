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
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.main.NodeLoadingSheetWriter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls exporting of graph nodes into loading sheets.
 */
public class ExportNodeLoadSheetsListener implements IChakraListener {
	
	Logger log = Logger.getLogger(getClass());
	private String subjectNodeType = "";
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		//Get all node types to be exported
		ArrayList<String> nodeTypes = new ArrayList<String>();
		for(int i = 1; i < Constants.MAX_EXPORTS; i++) {
			JComboBox box = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX + i);
			if(box.isVisible() && box.getSelectedItem() != null && !box.getSelectedItem().toString().isEmpty()) {
				nodeTypes.add(box.getSelectedItem().toString());
			}
		}
		Collections.sort(nodeTypes);
		
		//Export file details
		Hashtable<String, Vector<String[]>> hash = new Hashtable<String, Vector<String[]>>();
		NodeLoadingSheetWriter writer = new NodeLoadingSheetWriter();
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = "\\export\\Nodes\\";
		String writeFileName = "Nodes_LoadingSheet_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "") + ".xlsx";
		String fileLoc = workingDir + folder + writeFileName;
		log.info(fileLoc);

		//Iterate through each node type, grab data, store in hashtable
		for(String nodeType : nodeTypes) {
			ArrayList<Object[]> list = new ArrayList<Object[]>();
			String query = "SELECT ?s ?p ?prop WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
			query += nodeType;
			query += "> ;}";

			query += "OPTIONAL{ {?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?s ?p ?prop ;} } }";

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
			HashSet<String> properties = new HashSet<String>();
			// now get the bindings and generate the data
			try {
				while(wrapper.hasNext()) {
					ISelectStatement sjss = wrapper.next();
					Object [] values = new Object[names.length];
					boolean filledData = true;

					for(int colIndex = 0;colIndex < names.length;colIndex++) {
						if(sjss.getVar(names[colIndex]) != null || sjss.getVar(names[colIndex]).toString().equals("")) {
							if(colIndex == 1 && !sjss.getVar(names[colIndex]).toString().equals("")) {
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
			String nodeKey = nodeType;
			if(nodeKey.length()>31)nodeKey = nodeKey.substring(0, 31);
			hash.put(nodeKey, formatData(nodeType, properties, list));
		}
		
		//Write all data to file
		writer.ExportLoadingSheets(fileLoc, hash);
	}
	
	/**
	 * Method formatData.  Formats the data into a vector of string arrays for processing.
	 * @param nodeType String
	 * @param properties HashSet<String>
	 * @param list ArrayList<Object[]>
	
	 * @return Vector<String[]> the results. */
	public Vector<String[]> formatData(String nodeType, HashSet<String> properties, ArrayList<Object[]> list) {
		Collections.sort(list, new Comparator<Object[]>() {
			public int compare(Object[] a, Object[] b) {
				return a[0].toString().compareTo(b[0].toString());
			}
		});
		String[] relation = {"Node", "Ignore"};
		list.add(0, relation);
		String[] header = new String[properties.size()+1];
		Iterator<String> it = properties.iterator();
		header[0] = nodeType;
		for(int i = 1; i <= properties.size(); i++) {
			header[i] = it.next();
		}
		list.add(1, header);
		Vector<String[]> results = new Vector<String[]>();
		for(Object[] o : list) {
			String[] toAdd = new String[o.length];
			for(int i = 0; i < o.length; i++) {
				toAdd[i] = o[i].toString();
			}
			results.add(toAdd);
		}
		
		return results;
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {	
	}
}
