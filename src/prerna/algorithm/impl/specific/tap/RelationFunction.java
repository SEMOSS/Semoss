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
package prerna.algorithm.impl.specific.tap;



import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.RelationPlaySheet;

/**
 * This class is used to process through two variables to identify relationships.
 */
public class RelationFunction implements IAlgorithm {

	static final Logger logger = LogManager.getLogger(RelationFunction.class.getName());
	RelationPlaySheet playSheet;
	IEngine engine;
	String[] names; 

	ArrayList<String> rowNames = new ArrayList<String>();
	ArrayList<String> colNames = new ArrayList<String>();

	public void processRelations()
	{		
		// sort selected systems and data alphabetically 
		Vector <String> rowNamesAsVector = new Vector<String>(rowNames);
		Collections.sort(rowNamesAsVector);
		rowNames = new ArrayList<String>(rowNamesAsVector);

		Vector <String> colNamesAsVector = new Vector<String>(colNames);
		Collections.sort(colNamesAsVector);
		colNames = new ArrayList<String>(colNamesAsVector);
		
		// set the column names in the global names variable
		String[] colNamesArray = new String[colNames.size()+2];
		colNamesArray[0] = "";
		colNamesArray[1] = "SOR";
		for (int i=0; i<colNames.size(); i++) {
			colNamesArray[i+2] = colNames.get(i);
		}
		names = colNamesArray;

		// get systems that are source of record for data objects
		String sorQueryString = "SELECT DISTINCT ?system ?data (COUNT(DISTINCT ?icd) as ?icdCount) WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?system ?provideData ?data} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?provideData <http://semoss.org/ontologies/Relation/Contains/SOR> 'Yes'} } GROUP BY ?system ?data ";
		logger.info("PROCESSING QUERY: " + sorQueryString);
		
		// get systems that are source of record for data objects
		String consumerQueryString = "SELECT DISTINCT ?System ?Data ?Count WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?otherSystem <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}  BIND( 1 AS ?Count)}";
		logger.info("PROCESSING QUERY: " + consumerQueryString);

		ArrayList<Object[]> processedList = processQuery(sorQueryString);
		ArrayList<Object[]> processedConsumerList = processQuery(consumerQueryString);

		Object[][] sorVariableMatrix = createVariableMatrix(processedList);
		Object[][] consumerVariableMatrix = createVariableMatrix(processedConsumerList);

		// convert matrix back into arraylist
		ArrayList<Object[]> sorArrayList = new ArrayList<Object[]>(Arrays.asList(sorVariableMatrix));
		sorArrayList.remove(0);
		
		Hashtable sorAllHash = createHashtable(sorVariableMatrix);
		Hashtable consumerAllHash = createHashtable(consumerVariableMatrix);
		
		display(sorArrayList,sorAllHash,consumerAllHash);
	}
	public void display(ArrayList<Object[]> sorArrayList,Hashtable sorAllHash,Hashtable consumerAllHash)
	{
		// display output for functionality analysis tab
		GridScrollPane pane = new GridScrollPane(names, sorArrayList);
		pane.addHorizontalScroll();
		((RelationPlaySheet) playSheet).specificFuncAlysPanel.removeAll();
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		((RelationPlaySheet) playSheet).specificFuncAlysPanel.setLayout(gridBagLayout);
		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		((RelationPlaySheet) playSheet).specificFuncAlysPanel.add(pane, gbc_panel_1_1);
		((RelationPlaySheet) playSheet).specificFuncAlysPanel.repaint();
		try {
			((RelationPlaySheet) playSheet).setSelected(false);
			((RelationPlaySheet) playSheet).setSelected(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}

		// display output for heatmap tab
		((RelationPlaySheet) playSheet).sorHeatMap.callIt(sorAllHash);
		((RelationPlaySheet) playSheet).sorHeatMap.setVisible(true);
		// display output for heatmap tab
		((RelationPlaySheet) playSheet).consumerHeatMap.callIt(consumerAllHash);
		((RelationPlaySheet) playSheet).consumerHeatMap.setVisible(true);
	}

	/**
	 * Casts a given playsheet as a relation playsheet.
	 * @param playSheet 	Playsheet to be cast.
	 */

	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (RelationPlaySheet) playSheet;
	}

	@Override
	public String[] getVariables() {
		return null;
	}

	@Override
	public void execute() {
		processRelations();
	}

	@Override
	public String getAlgoName() {
		return null;
	}

	public void setRDFEngine(IEngine engine) {
		this.engine = engine;	
	}

	public void setDataList(ArrayList<String> dataList)
	{
		this.rowNames = dataList;
	}

	public void setSysList(ArrayList<String> sysList)
	{
		this.colNames = sysList;
	}

	public ArrayList<Object[]> processQuery(String query) {
		// execute the query on a specified engine
		//SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		ISelectWrapper sjsw = WrapperManager.getInstance().getSWrapper(engine, query);
		/*sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();*/
		
		String[] names = sjsw.getVariables();
		
		ArrayList<Object[]> processedList = new ArrayList<Object[]>();
		
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
		
			String sys = (String) sjss.getVar(names[0]);
			String data = (String) sjss.getVar(names[1]);
			double count = (Double) sjss.getVar(names[2]);
		
			if (colNames.contains(sys)) {
				processedList.add(new Object[]{sys, data, count});
			}
		}
		return processedList;
	}
	
	public Object[][] createVariableMatrix(ArrayList<Object[]> processedList)
	{
		// create a matrix with row and column names
		// iterate through processed list, implement logic to create X's based on relationship
		Object[][] variableMatrix = new Object[rowNames.size()+1][colNames.size()+2];

		for (int i=0; i<rowNames.size(); i++) {
			variableMatrix[i+1][0] = rowNames.get(i);
		}

		for (int i=0; i<colNames.size(); i++) {
			variableMatrix[0][i+2] = colNames.get(i);
		}

		for (int i=0; i<processedList.size(); i++) {
			Object[] row = processedList.get(i);
			int rowInd = rowNames.indexOf(row[1])+1;
			int colInd = colNames.indexOf(row[0])+2;
			if (rowInd != 0) {
				variableMatrix[rowInd][colInd] = row[2];
			}
		}

		// counts how many systems are SOR for each data object
		for (int i=0; i<rowNames.size()+1; i++) {
			int count = -1;
			for (int j=0; j<colNames.size()+2; j++) {
				if (variableMatrix[i][j] != null) {
					count++;	
				}
			}
			variableMatrix[i][1] = count;
		}
		

		return variableMatrix;
	}
	
	public Hashtable createHashtable(Object[][] variableMatrix){
		ArrayList<Object[]> processedList2 = new ArrayList<Object[]>();

		for (int i=1; i<rowNames.size()+1; i++) {
			for (int j=2; j<colNames.size()+2; j++) {
				if (variableMatrix[i][j] == null) {
					processedList2.add(new Object[]{variableMatrix[0][j], variableMatrix[i][0], 0.0});
				} else {
					processedList2.add(new Object[]{variableMatrix[0][j], variableMatrix[i][0], variableMatrix[i][j]});
				}
			}
		}

		// update the counts so that they are percentages
		ArrayList<Object[]> percentList = new ArrayList<Object[]>();
		Hashtable processList = new Hashtable();

		for (int i=0; i<processedList2.size(); i++) {
			Object[] row = processedList2.get(i);
			String data = (String) row[1];
			Double count = (Double) row[2];
			if(!processList.containsKey(data)) {
				processList.put(data, count);
			}
			if(count > (Double) processList.get(data)) {
				processList.put(data, count);
			}
		}

		for (int i=0; i<processedList2.size(); i++) {
			Object[] row = processedList2.get(i);
			String sys = (String) row[0];
			String data = (String) row[1];
			Double count = (Double) row[2];
			if ((Double) processList.get(data) != 0) {
				count = count / (Double) processList.get(data);
			}
			percentList.add(new Object[]{sys, data, count});
		}

		// let's make a heatmap
		Hashtable dataHash = new Hashtable();
		Hashtable dataSeries = new Hashtable();
		String[] var = new String[]{"Systems","Data Objects","Value"};
		String xName = var[0]; //system
		String yName = var[1]; //data objects
		for (int i=0;i<percentList.size();i++)
		{
			Hashtable elementHash = new Hashtable();
			Object[] listElement = percentList.get(i);			
			String methodName = (String) listElement[0]; //system
			String groupName = (String) listElement[1]; //data
			methodName = methodName.replaceAll("\"", "");
			groupName = groupName.replaceAll("\"", "");
			String key = methodName +"-"+groupName;
			double count = (Double) listElement[2];
			elementHash.put(xName, methodName);
			elementHash.put(yName, groupName);
			elementHash.put(var[2], count);
			dataHash.put(key, elementHash);

		}

		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		allHash.put("title",  var[0] + " vs " + var[1]);
		allHash.put("xAxisTitle", var[0]);
		allHash.put("yAxisTitle", var[1]);
		allHash.put("value", var[2]);
		return allHash;
	}
}
