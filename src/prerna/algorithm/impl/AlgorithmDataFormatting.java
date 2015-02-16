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

import prerna.util.Utility;

public class AlgorithmDataFormatting {

	boolean[] isCategorical;
	boolean includeLastColumn = false;
	
	public AlgorithmDataFormatting() {
		
	}
	
	public boolean[] getIsCategorical() {
		return isCategorical;
	}
	
	public void setIncludeLastColumn(boolean includeLastColumn) {
		this.includeLastColumn = includeLastColumn;
	}
	
	//TODO: get to work with nulls/missing data
	//TODO: already parse through data in clustering data processor, better way to improve efficiency?
	//TODO: this uses indexing starting at 1 because this doesn't include the actual instance node name
	public Object[][] manipulateValues(ArrayList<Object[]> queryData) {
		int counter = 0;
		
		int numProps = queryData.get(0).length;
		int numPropsSize = numProps;
		if(!includeLastColumn)
			numPropsSize--;
		Object[][] data = new Object[numPropsSize][queryData.size()];
		isCategorical = new boolean[numProps];
		Object[][] trackType = new Object[numProps][queryData.size()];
		
		int i;
		int size = queryData.size();
		for(i = 0; i < size; i++) {
			Object[] dataRow = queryData.get(i);
			int j;
			for(j = 1; j < numPropsSize; j++) {
				data[j][counter] = dataRow[j];
				trackType[j][counter] = Utility.processType(dataRow[j].toString());
			}
			counter++;
		}
		
		for(i = 1; i < numPropsSize; i++) {
			int j;
			int stringCounter = 0;
			int doubleCounter = 0;
			for(j = 0; j < counter; j++) {
				if(trackType[i][j].toString().equals("STRING")) {
					stringCounter++;
				} else {
					doubleCounter++;
				}
			}
			if(stringCounter > doubleCounter) {
				isCategorical[i] = true;
			}
		}
		
		return data;
	}
	
	//TODO: parse through data too many times in clustering data processor, better way to improve efficiency?
	public Object[][] convertColumnValuesToRows(Object[][] queryData) {
		int counter = 0;
		
		int numProps = queryData[0].length;
		int size = queryData.length;

		Object[][] data = new Object[numProps][size];
		isCategorical = new boolean[numProps];
		Object[][] trackType = new Object[numProps][size];
		
		int i;
		for(i = 0; i < size; i++) {
			Object[] dataRow = queryData[i];
			int j;
			for(j = 0; j < numProps; j++) {
				data[j][counter] = dataRow[j];
				if(dataRow[j] != null) {
					trackType[j][counter] = Utility.processType(dataRow[j].toString());
				}
			}
			counter++;
		}
		
		for(i = 0; i < numProps; i++) {
			int j;
			int stringCounter = 0;
			int doubleCounter = 0;
			for(j = 0; j < counter; j++) {
				Object val = trackType[i][j];
				if(val != null) {
					if(val.toString().equals("STRING")) {
						stringCounter++;
					} else {
						doubleCounter++;
					}
				}
			}
			if(stringCounter > doubleCounter) {
				isCategorical[i] = true;
			}
		}
		
		return data;
	}
}
