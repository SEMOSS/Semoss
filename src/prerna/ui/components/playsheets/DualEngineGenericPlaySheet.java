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
package prerna.ui.components.playsheets;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IDatabaseEngine;
import prerna.util.DIHelper;
import prerna.util.PlaySheetRDFMapBasedEnum;
import prerna.util.Constants;

/**
 * This class is a temporary fix for queries to run across multiple databases
 * The query passed through this class must have the format engine1&engine2&engine1query&engine2query
 * The two queries must have exactly one variable name in common--which is how this class will line up the table
 */
public class DualEngineGenericPlaySheet extends DualEngineGridPlaySheet {

	private static final Logger logger = LogManager.getLogger(DualEngineGenericPlaySheet.class.getName());
	String playsheetName;
	String[] finalNames;
	String[] mathFunctions;
	BrowserPlaySheet playSheet = null;

	@Override
	public Map<String, String> getDataTableAlign() {
		return playSheet.getDataTableAlign();
	}
	
	@Override
	public void createData(){
		this.engine1 = this.engine;
		this.engineName1 = this.engine.getEngineId();
		super.createData();
		createFinalList(finalNames);
	}
	
	@Override
	public void createView(){
		String playSheetClassName = PlaySheetRDFMapBasedEnum.getClassFromName(playsheetName);
		try {
			playSheet = (BrowserPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException ex) {
			logger.error(Constants.STACKTRACE, ex);
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (InstantiationException e) {
			logger.error(Constants.STACKTRACE, e);
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (IllegalAccessException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		} catch (IllegalArgumentException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		} catch (InvocationTargetException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		} catch (NoSuchMethodException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		} catch (SecurityException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		}
		Hashtable hash = new Hashtable();
		finalNames = this.getNames();
		for(String name : finalNames){
			hash.put(name, name);
		}
		ITableDataFrame f = new H2Frame(finalNames);
//		f.addRow(hash);
		playSheet.setDataMaker(f);
		playSheet.setQuestionID(this.questionNum);
		playSheet.setTitle(this.title);
		playSheet.pane = this.pane;
		playSheet.dataFrame = this.dataFrame;
		playSheet.processQueryData();
		playSheet.createView();
	}
	
	@Override
	public Map getDataMakerOutput(String... selectors){
		String playSheetClassName = PlaySheetRDFMapBasedEnum.getClassFromName(playsheetName);
		try {
			playSheet = (BrowserPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException ex) {
			logger.error(Constants.STACKTRACE, ex);
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (InstantiationException e) {
			logger.error(Constants.STACKTRACE, e);
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (IllegalAccessException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		} catch (IllegalArgumentException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		} catch (InvocationTargetException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		} catch (NoSuchMethodException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		} catch (SecurityException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			logger.error(Constants.STACKTRACE, e);
		}
		Hashtable hash = new Hashtable();
		finalNames = this.getNames();
		for(String name : finalNames){
			hash.put(name, name);
		}
		ITableDataFrame f = new H2Frame(finalNames);
//		f.addRow(hash);
		playSheet.setDataMaker(f);
		playSheet.setQuestionID(this.questionNum);
		Map retHash = playSheet.getDataMakerOutput();
		retHash.put("data", this.getList());
		return retHash;
	}
	
	private void createFinalList(String[] myNames){
		this.dataFrame = new H2Frame(myNames);
		int groupIdx = -1;
		for(int i = 0; i<this.mathFunctions.length; i++){
			if (this.mathFunctions[i].equals("GROUP")){
				groupIdx = i;
				break;
			}
		}
		// create the name mapping
		// for each new name, where does it sit in orig names?
		Integer[] nameMapping = new Integer[myNames.length];
		String[] origNames = this.getNames();
		for(int nmIdx = 0; nmIdx < myNames.length; nmIdx++){
			String myName = myNames[nmIdx];
			for(int origIdx = 0; origIdx < origNames.length; origIdx++){
				if(origNames[origIdx].equals(myName)){
					nameMapping[nmIdx] = origIdx;
					continue;
				}
			}
		}
		
		// do the grouping and the cutting of columns that aren't specified
		// for each row, get the group value
		Map<Object, Object[]> groupedRows = new HashMap<Object, Object[]>();
		List<Object[]> table = this.getList();
		for(Object[] origRow : table){
			Object groupVal = origRow[groupIdx];
			Object[] newRow = new Object[myNames.length];
			if(groupedRows.containsKey(groupVal)){
				newRow = groupedRows.get(groupVal);
			}
			else{
				groupedRows.put(groupVal, newRow);
			}
			addToRow(origRow, newRow, nameMapping);
		}
		
		//add it to the list
		List<Object[]> finalList = new ArrayList<Object[]>();
		for(Object[] finalRow : groupedRows.values()){
//			this.dataFrame.addRow(finalRow);
			finalList.add(finalRow);
		}
		this.list = finalList;
		this.names = myNames;
	}
	
	private void addToRow(Object[] origRow, Object[] newRow, Integer[] nameMapping){
//		Object[] updatedRow = new Object[newRow.length];
		//need to do the math and call it a day
		for(int colIdx = 0; colIdx < nameMapping.length; colIdx ++){
			Object origVal = origRow[nameMapping[colIdx]];
			Object existingVal = newRow[colIdx];
			String mathFunc = this.mathFunctions[colIdx];
			if(origVal == null){
				continue;
			}
			else if(existingVal == null){
				newRow[colIdx] = origVal;
			}
			else if (mathFunc.equals("GROUP")){
				newRow[colIdx] = origVal;
			}
			else if (mathFunc.equals("SAMPLE")){
				newRow[colIdx] = origVal;
			}
			else if (mathFunc.equals("SUM")){
				newRow[colIdx] = (Double) origVal + (Double) existingVal;
			}
		}
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
				StringTokenizer tokenTokens = new StringTokenizer(token, ",");
				finalNames = new String[tokenTokens.countTokens()];
				for (int i = 0; tokenTokens.hasMoreTokens(); i++){
					finalNames[i] = tokenTokens.nextToken();
				}
			}
			else if (queryIdx == 1){
				StringTokenizer tokenTokens = new StringTokenizer(token, ",");
				mathFunctions = new String[tokenTokens.countTokens()];
				for (int i = 0; tokenTokens.hasMoreTokens(); i++){
					mathFunctions[i] = tokenTokens.nextToken();
				}
			}
			else if (queryIdx == 2){
				this.engineName2 = token;
				this.engine2 = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(engineName2);
			}
			else if (queryIdx == 3)
				this.query1 = token;
			else if (queryIdx == 4)
				this.query2 = token;
			else if (queryIdx == 5)
				this.playsheetName = token;
			else if (queryIdx == 6)
				this.match1 = Boolean.parseBoolean(token);
			else if (queryIdx == 7)
				this.match2 = Boolean.parseBoolean(token);
		}
	}
}
