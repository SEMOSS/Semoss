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
package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.impl.ExactStringMatcher;
import prerna.algorithm.impl.ExactStringOuterJoinMatcher;
import prerna.algorithm.impl.ExactStringPartialOuterJoinMatcher;
import prerna.ds.BTreeDataFrame;
import prerna.ds.SimpleTreeNode;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;

/**
 * This class is a temporary fix for queries to run across multiple databases
 * The query passed through this class must have the format engine1&engine2&engine1query&engine2query
 * The two queries must have exactly one variable name in common--which is how this class will line up the table
 */
public class DualEngineGridPlaySheet extends GridPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(DualEngineGridPlaySheet.class.getName());
	private String query1;
	private String query2;
	private String engineName1;
	private String engineName2;
	private IEngine engine1;
	private IEngine engine2;
	private boolean partialOuterJoinTable1 = true;
	private boolean partialOuterJoinTable2 = true;

	/**
	 * This is the function that is used to create the first view 
	 * of any play sheet.  It often uses a lot of the variables previously set on the play sheet, such as {@link #setQuery(String)},
	 * {@link #setJDesktopPane(JDesktopPane)}, {@link #setRDFEngine(IEngine)}, and {@link #setTitle(String)} so that the play 
	 * sheet is displayed correctly when the view is first created.  It generally creates the model for visualization from 
	 * the specified engine, then creates the visualization, and finally displays it on the specified desktop pane
	 * 
	 * <p>This is the function called by the PlaysheetCreateRunner.  PlaysheetCreateRunner is the runner used whenever a play 
	 * sheet is to first be created, most notably in ProcessQueryListener.
	 */
	@Override
	public void createData() {

		ISelectWrapper wrapper1 = WrapperManager.getInstance().getSWrapper(engine1, query1);
		String [] names1 = wrapper1.getVariables();
		ITableDataFrame tree1 = new BTreeDataFrame(names1);
		while(wrapper1.hasNext()) {
			tree1.addRow(wrapper1.next());
		}

		ISelectWrapper wrapper2 = WrapperManager.getInstance().getSWrapper(engine2, query2);
		String[] names2 = wrapper2.getVariables();
		ITableDataFrame tree2 = new BTreeDataFrame(names2);
		while(wrapper2.hasNext()) {
			tree2.addRow(wrapper2.next());
		}

		//find the common variable in the wrapper names (this will be the hashtable key)
		Set<String> setNames1 = new HashSet<String> (Arrays.asList(names1));
		Set<String> setNames2 = new HashSet<String> (Arrays.asList(names2));

		Set<String> uniqueNames = new HashSet<String>();
		uniqueNames.addAll(setNames1);
		uniqueNames.addAll(setNames2);
		Set<String> setDifference = new LinkedHashSet<String>();
		setDifference.addAll(setNames1);
		setDifference.retainAll(setNames2);
		String commonVar = setDifference.iterator().next();

		if(names1.length > 1 && names2.length > 1) {
			IAnalyticRoutine matcher = null;
			if(partialOuterJoinTable1 && partialOuterJoinTable2) {
				matcher = new ExactStringOuterJoinMatcher();
				tree1.join(tree2, commonVar, commonVar, 1.0, matcher);
				dataFrame = tree1;
				
			} else if(partialOuterJoinTable1) {
				matcher = new ExactStringPartialOuterJoinMatcher();
				tree1.join(tree2, commonVar, commonVar, 1.0, matcher);
				dataFrame = tree1;
				
			} else if(partialOuterJoinTable2) {
				matcher = new ExactStringPartialOuterJoinMatcher();
				tree2.join(tree1, commonVar, commonVar, 1.0, matcher);
				dataFrame = tree2;
				
			} else {
				matcher = new ExactStringMatcher();
				tree1.join(tree2, commonVar, commonVar, 1.0, matcher);
				dataFrame = tree1;
			}
		} else {
			// not performing a join, but a filter/append
			if(names2.length == 1) {
				Iterator<Object[]> it = tree2.iterator(false, null);
				if(partialOuterJoinTable2) {
					// this is an append on tree1
					while(it.hasNext()) {
						Object[] row = it.next();
						Map<String, Object> hashRow = new HashMap<String, Object>();
						hashRow.put(commonVar, row[0]);
						for(int i = 0; i < names1.length; i++) {
							if(names1[i].equals(commonVar)) {
								continue;
							}
							hashRow.put(names1[i], SimpleTreeNode.EMPTY);
						}
						tree1.addRow(hashRow, hashRow);
					}
					dataFrame = tree1;

				} else {
					// this is a filter on tree1
					//TODO: need to add filter in BTREE!!!!!!!!!!!!!!!
					Object[] col1 = tree1.getUniqueValues(commonVar);
					Object[] col2 = tree2.getUniqueValues(commonVar);

					List<Object> filterValues = findDisjointInFirstCol(col1, col2);
					tree1.filter(commonVar, filterValues);
					dataFrame = tree1;

				}
			} else {
				Iterator<Object[]> it = tree1.iterator(false, null);
				if(partialOuterJoinTable2 ) {
					// this is an append on tree2
					while(it.hasNext()) {
						Object[] row = it.next();
						Map<String, Object> hashRow = new HashMap<String, Object>();
						hashRow.put(commonVar, row[0]);
						for(int i = 0; i < names2.length; i++) {
							if(names2[i].equals(commonVar)) {
								continue;
							}
							hashRow.put(names2[i], SimpleTreeNode.EMPTY);
						}
						tree2.addRow(hashRow, hashRow);
					}
					dataFrame = tree2;

				} else {
					// this is a filter on tree2
					//TODO: need to add filter in BTREE!!!!!!!!!!!!!!!
					Object[] col1 = tree1.getUniqueValues(commonVar);
					Object[] col2 = tree2.getUniqueValues(commonVar);

					List<Object> filterValues = findDisjointInFirstCol(col2, col1);
					tree2.filter(commonVar, filterValues);
					dataFrame = tree2;
				}
			}
		}
	}
	
	public List<Object> findDisjointInFirstCol(Object[] col1, Object[] col2) {
		List<Object> vals = new ArrayList<Object>();
		for(Object val1 : col1) {
			boolean found = false;
			for(Object val2 : col2) {
				if(val1.equals(val2)) {
					found = true;
					break;
				}
			}
			if(!found) {
				vals.add(val1);
			}
		}
		return vals;
	}

	/**
	 * Sets the String version of the SPARQL query on the play sheet. <p> The query must be set before creating the model for
	 * visualization.  Thus, this function is called before createView(), extendView(), overlayView()--everything that 
	 * requires the play sheet to pull data through a SPARQL query.
	 * @param query the full SPARQL query to be set on the play sheet
	 * @see	#createView()
	 * @see #extendView()
	 * @see #overlayView()
	 */
	@Override
	public void setQuery(String query) {

		StringTokenizer queryTokens = new StringTokenizer(query, "&");
		for (int queryIdx = 0; queryTokens.hasMoreTokens(); queryIdx++){
			String token = queryTokens.nextToken();
			if (queryIdx == 0){
				this.engineName1 = token;
				this.engine1 = (IEngine) DIHelper.getInstance().getLocalProp(engineName1);
			}
			else if (queryIdx == 1){
				this.engineName2 = token;
				this.engine2 = (IEngine) DIHelper.getInstance().getLocalProp(engineName2);
			}
			else if (queryIdx == 2)
				this.query1 = token;
			else if (queryIdx == 3)
				this.query2 = token;
			else if (queryIdx == 4)
				this.partialOuterJoinTable1 = Boolean.parseBoolean(token);
			else if (queryIdx == 5)
				this.partialOuterJoinTable2 = Boolean.parseBoolean(token);
		}
	}
}
