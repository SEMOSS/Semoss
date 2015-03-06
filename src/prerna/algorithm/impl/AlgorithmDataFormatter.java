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
package prerna.algorithm.impl;

import java.util.ArrayList;

import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public final class AlgorithmDataFormatter {
		
	public static final String STRING_KEY = "STRING";
	public static final String DOUBLE_KEY = "DOUBLE";
	public static final String DATE_KEY = "DATE";
	public static final String SIMPLEDATE_KEY = "SIMPLEDATE";
	
	//TODO: get to work with nulls/missing data
	//TODO: already parse through data in clustering data processor, better way to improve efficiency?
	//TODO: this uses indexing starting at 1 because this doesn't include the actual instance node name
	public static Object[][] manipulateValues(ArrayList<Object[]> queryData, boolean includeLastColumn) {
		int counter = 0;
		
		int numProps = queryData.get(0).length;
		int numPropsSize = numProps;
		
		if(!includeLastColumn)
			numPropsSize--;

		Object[][] data = new Object[numPropsSize][queryData.size()];
		
		int i;
		int size = queryData.size();
		for(i = 0; i < size; i++) {
			Object[] dataRow = queryData.get(i);
			int j;
			for(j = 0; j < numPropsSize; j++) {
				data[j][counter] = dataRow[j];
			}
			counter++;
		}
		return data;
	}

	//TODO: parse through data too many times in clustering data processor, better way to improve efficiency?
	public static Object[][] convertColumnValuesToRows(Object[][] queryData) {
		int counter = 0;
		
		int numProps = queryData[0].length;
		int size = queryData.length;

		Object[][] data = new Object[numProps][size];
		
		int i;
		for(i = 0; i < size; i++) {
			Object[] dataRow = queryData[i];
			int j;
			for(j = 0; j < numProps; j++) {
				data[j][counter] = dataRow[j];
			}
			counter++;
		}

		return data;
	}
	
	/**
	 * Determines the column type for every column but the first since this is reserved for an identifier.
	 * TODO: some boolean to account for including the first column
	 * @param names
	 * @param list
	 * @param categoryPropNames
	 * @param categoryPropIndices
	 * @param numericalPropNames
	 * @param numericalPropIndices
	 * @param dateTypeIndices
	 * @param simpleDateTypeIndices
	 */
	public static void determineColumnTypes(String[] names, ArrayList<Object []> list, String[] categoryPropNames, Integer[] categoryPropIndices, String[] numericalPropNames, Integer[] numericalPropIndices, Integer[] dateTypeIndices, Integer[] simpleDateTypeIndices) {

		int categoryPropNamesCounter = 0;
		int numericalPropNamesCounter = 0;
		int dateTypeIndicesCounter = 0;
		int simpleDateTypeIndicesCounter = 0;

		//iterate through columns
		for(int j = 0; j < names.length; j++) {
			if(j != 0) {
				String type = determineColumnType(list,j);
				if(type.equals(STRING_KEY)) {
					categoryPropNames[categoryPropNamesCounter] = names[j];
					categoryPropIndices[categoryPropNamesCounter] = j;
					categoryPropNamesCounter++;
					//					LOGGER.info("Found " + varNames[j] + " to be a categorical data column");
				} else {
					numericalPropNames[numericalPropNamesCounter] = names[j];
					numericalPropIndices[numericalPropNamesCounter] = j;
					numericalPropNamesCounter++;
					//					LOGGER.info("Found " + varNames[j] + " to be a numerical data column");
					if(type.equals(DATE_KEY)){
						dateTypeIndices[dateTypeIndicesCounter] = j;
						dateTypeIndicesCounter++;
					} else if(type.equals(SIMPLEDATE_KEY)){
						simpleDateTypeIndices[simpleDateTypeIndicesCounter] = j;
						simpleDateTypeIndicesCounter++;
					}
				}
			} 
		}

	}
	
	/**
	 *  Determines the column type for every column (except the first since this is reserved for an identifier).
	 *  TODO: some boolean to account for including the first column
	 * @param names
	 * @param list
	 * @param columnTypes
	 */
	public static String[] determineColumnTypes(ArrayList<Object []> list) {
		int numCols = list.get(0).length;
		String[] columnTypes = new String[numCols];
		//iterate through columns
		for(int j = 0; j < numCols; j++) {
			columnTypes[j] = determineColumnType(list,j);
		}
		return columnTypes;
	}
	
	private static String determineColumnType(ArrayList<Object []> list,int column) {
		//iterate through rows
		int numCategorical = 0;
		int numNumerical = 0;
		String type;
		for(int i = 0; i < list.size(); i++) {
			Object[] dataRow = list.get(i);
			if(dataRow[column] != null && !dataRow[column].toString().equals("")) {
				String colEntryAsString = dataRow[column].toString();
				if(!colEntryAsString.isEmpty()) {
					type = Utility.processType(colEntryAsString);
					if(type.equals(STRING_KEY)) {
						numCategorical++;
					} else {
						numNumerical++;
					}
				}
			}
		}
		if(numCategorical > numNumerical) {
			return STRING_KEY;
		} else {
			return DOUBLE_KEY;
			//TODO account for dates
//			if(type.equals("DATE")){
//				return "DATE";
//			} else if(type.equals("SIMPLEDATE")){
//				return "SIMPLEDATE";
//			}
		}
	}

	/**
	 *  Goes through the list of columns and filters the names to only include those of the type.
	 *  TODO: some boolean to account for including the first column
	 * @param names
	 * @param columnTypesArr
	 * @param typeToFind
	 * @return
	 */
	public static String[] determineColumnNamesOfType(String[] names, String[] columnTypesArr, String typeToFind) {
		
		int namesCounter = 0;
		int namesLength = names.length;
		String[] filteredNames = new String[namesLength];
		//iterate through columns
		for(int j = 0; j < names.length; j++) {
			if(j != 0) {
				String type = columnTypesArr[j];
				if(type.equals(typeToFind)) {
					filteredNames[namesCounter] = names[j];
					namesCounter++;
				}
			} 
		}
		filteredNames = (String[]) ArrayUtilityMethods.trimEmptyValues(filteredNames);
		return filteredNames;
	}

}
