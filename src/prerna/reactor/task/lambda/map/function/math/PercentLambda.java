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
package prerna.reactor.task.lambda.map.function.math;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.task.lambda.map.AbstractMapLambda;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PercentLambda extends AbstractMapLambda {

	private int numCols;
	// store indices of column values
	private List<Integer> colIndices;
	// dynamically create the percent column names
	private String[] percentColumnArr;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		Object[] values = row.getValues();
		
		Object[] percentValues = new Object[numCols];
		for(int i = 0; i < numCols; i++) {
			int indexToGet = colIndices.get(i).intValue();
			percentValues[i] = getPercent(values[indexToGet]);
		}
		
		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = row.copy();
		rowCopy.addFields(this.percentColumnArr, percentValues);
		return rowCopy;
	}
	
	private String getPercent(Object val) {
		try {
			if(val instanceof Number) {
				double percentVal = ((Number) val).doubleValue() * 100;
				if(percentVal == Math.rint(percentVal)) {
					// it is an int
					return percentVal + "%";
				} else {
					String strPercent = percentVal + "";
					int dotIndex = strPercent.indexOf('.');
					if(dotIndex > 0 & (dotIndex+2) < strPercent.length()) {
						strPercent = strPercent.substring(0, strPercent.indexOf('.') + 2);
					}
					return strPercent + "%";
				}
			} else if(val instanceof String) {
				return getPercent(Double.parseDouble(val.toString()));
			}
		} catch(Exception e) {
			// ignore
		}
		return null;
	}
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;
		
		// figure out which indices are those we want to use
		List<String> colNames = new ArrayList<String>();
		this.colIndices = new ArrayList<Integer>();
		int totalCols = headerInfo.size();
		int inputCols = columns.size();
		
		// this modifies the header info map by reference
		NEXT_COLUMN : for(int i = 0; i < inputCols; i++) {
			String valueToFind = columns.get(i);
			for(int j = 0; j < totalCols; j++) {
				Map<String, Object> headerMap = headerInfo.get(j);
				String alias = headerMap.get("alias").toString();
				if(alias.equals(valueToFind)) {
					colNames.add("PER_" + alias);
					this.colIndices.add(new Integer(j));
					continue NEXT_COLUMN;
				}
			}
			
			// throw an error
			throw new SemossPixelException(
					new NounMetadata("Could not find column " + valueToFind + " in Percent routine",
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}
		
		this.numCols = colIndices.size();
		this.percentColumnArr = new String[numCols];
		
		// add all the trim columns
		for(int i = 0; i < numCols; i++) {
			String colName = colNames.get(i);
			this.percentColumnArr[i] = colName;
			// need to add a new entity for the column
			Map<String, Object> headerMap = new HashMap<String, Object>();
			headerMap.put("alias", colName);
			headerMap.put("header", colName);
			headerMap.put("type", "STRING");
			headerMap.put("derived", true);
			this.headerInfo.add(headerMap);
		}
	}
}
