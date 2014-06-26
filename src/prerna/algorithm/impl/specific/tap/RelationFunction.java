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
package prerna.algorithm.impl.specific.tap;



import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.RelationPlaySheet;

/**
 * This class is used to process through two variables to identify relationships.
 */
public class RelationFunction implements IAlgorithm {

	Logger logger = Logger.getLogger(getClass());
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

		// get systems that are source of record for data objects
		String queryString = "SELECT DISTINCT ?system ?data (COUNT(DISTINCT ?icd) as ?icdCount) WHERE { { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?system ?provideData ?data} } UNION { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?system} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?data} } FILTER(!BOUND(?icd2)) } } GROUP BY ?system ?data";
		logger.info("PROCESSING QUERY: " + queryString);

		//executes the query on a specified engine
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		sjsw.setEngine(engine);
		sjsw.setQuery(queryString);
		sjsw.executeQuery();

		names = sjsw.getVariables();

		ArrayList<Object[]> processedList = new ArrayList<Object[]>();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();

			String sys = (String) sjss.getVar(names[0]);
			String data = (String) sjss.getVar(names[1]);
			double count = (double) sjss.getVar(names[2]);

			if (colNames.contains(sys)) {
				processedList.add(new Object[]{sys, data, count});
			}
		}

		// set the column names in the global names variable
		String[] colNamesArray = new String[colNames.size()+2];
		colNamesArray[0] = "";
		colNamesArray[1] = "SOR";
		for (int i=0; i<colNames.size(); i++) {
			colNamesArray[i+2] = colNames.get(i);
		}
		names = colNamesArray;

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
			variableMatrix[rowInd][colInd] = row[2];
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

		// convert matrix back into arraylist
		ArrayList<Object[]> arrayList = new ArrayList<Object[]>(Arrays.asList(variableMatrix));
		arrayList.remove(0);

		// display output
		GridScrollPane pane = new GridScrollPane(names, arrayList);
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

}
