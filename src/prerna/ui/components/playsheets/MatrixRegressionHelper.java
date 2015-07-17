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

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.util.ArrayUtilityMethods;

public final class MatrixRegressionHelper{

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionHelper.class.getName());

	/**
	 * Creates the A array
	 * Start index tells what column to start at
	 * If bIndex is less than the number of columns, bIndex will be removed
	 */
	public static double[][] createA(ITableDataFrame data, List<String> skipAttribute, int variableStartCol, int bIndex) {
		String[] dataCols = data.getColumnHeaders();
//		int offset = 0;
//		for(int i = 0; i < dataCols.length; i++) {
//			if(skipAttribute.contains(dataCols[i])) {
//				if(ArrayUtilityMethods.arrayContainsValueAtIndex(dataCols, dataCols[i]) < bIndex) {
//					offset++;
//				}
//			}
//		}
//		bIndex -= offset;
		
		int listNumRows = data.getNumRows();
//		offset = 0;
//		if(skipAttribute != null) {
//			offset = skipAttribute.size();
//		}
		int listNumCols = data.getNumCols();// - offset;
		
		int outNumCols;
		if(bIndex<listNumCols) {
			outNumCols = listNumCols - variableStartCol - 1;
		} else {
			outNumCols = listNumCols - variableStartCol;
		}
		double[][] A = new double[listNumRows][outNumCols];

		Iterator<Object[]> it = data.iterator(false, skipAttribute);
		int i = 0;
		int j = 0;
		while(it.hasNext()) {
			Object[] oldRow = it.next();
			int outIndex = 0;
			for(j = variableStartCol; j< listNumCols; j++) {
				if(j != bIndex) {
					if(oldRow[j] != null && !oldRow[j].toString().trim().isEmpty()) {
						A[i][outIndex] = (double)oldRow[j];
					} else {
						A[i][outIndex] = 0.0;
					}
					outIndex++;
				}
			}
			i++;
		}

		return A;
	}

	/**
	 * create the b array by pulling the appropriate column
	 * @param list
	 * @param bIndex
	 * @return
	 */
	public static double[] createB(ITableDataFrame data, List<String> skipAttribute, int bIndex) {
		String[] dataCols = data.getColumnHeaders();
//		int offset = 0;
//		for(int i = 0; i < dataCols.length; i++) {
//			if(skipAttribute.contains(dataCols[i])) {
//				if(ArrayUtilityMethods.arrayContainsValueAtIndex(dataCols, dataCols[i]) < bIndex) {
//					offset++;
//				}
//			}
//		}
//		bIndex -= offset;
		
		int listNumRows = data.getNumRows();
		double[] b = new double[listNumRows];

		Iterator<Object[]> it = data.iterator(false, skipAttribute);
		int i = 0;
		while(it.hasNext()) {
			Object[] row = it.next();
			if(row[bIndex] != null && !row[bIndex].toString().trim().isEmpty()) {
				b[i] = (Double) row[bIndex];
			} else {
				b[i] = 0;
			}
			i++;
		}
		return b;
	}
	
	/**
	 * Creates the A array
	 * Start index tells what column to start at
	 * If bIndex is less than the number of columns, bIndex will be removed
	 */
	public static double[][] createA(List<Object[]> list, int variableStartCol, int bIndex) {
		int i;
		int j;
		int listNumRows = list.size();
		int listNumCols = list.get(0).length;

		int outNumCols;
		if(bIndex<listNumCols)
			outNumCols = listNumCols - variableStartCol - 1;
		else
			outNumCols = listNumCols - variableStartCol;
		
		double[][] A = new double[listNumRows][outNumCols];

		for(i=0; i<listNumRows; i++) {
			Object[] oldRow = list.get(i);
			int outIndex = 0;
			for(j=variableStartCol; j<listNumCols; j++) {
				if(j!=bIndex) {
					if(oldRow[j] != null && !oldRow[j].toString().trim().isEmpty()) {
						A[i][outIndex] = (double)oldRow[j];
					} else {
						A[i][outIndex] = 0.0;
					}
					outIndex++;
				}
			}
		}
		return A;
	}

	/**
	 * create the b array by pulling the appropriate column
	 * @param list
	 * @param bIndex
	 * @return
	 */
	public static double[] createB(List<Object[]> list,int bIndex) {
		int i;
		int listNumRows = list.size();
		double[] b = new double[listNumRows];
		for(i=0;i<listNumRows;i++)
			if(list.get(i)[bIndex] != null && !list.get(i)[bIndex].toString().trim().isEmpty()) {
				b[i] = (Double)list.get(i)[bIndex];
			} else {
				b[i] = 0;
			}
		return b;
	}
	
	/**
	 * Appends array b to be the last column of array A.
	 * @param A
	 * @param b
	 * @return
	 */
	public static double[][] appendB(double[][] A, double[] b) {
		int i;
		int j;
		int numRows = A.length;
		int numCols = A[0].length;
		double[][] Ab = new double[numRows][numCols + 1];
		
		for(i=0;i<numRows;i++) {
			for(j=0;j<numCols;j++)
				Ab[i][j] = A[i][j];
			Ab[i][j] = b[i];
		}
		
		return Ab;
	}

	public static String[] moveNameToEnd(String[] names, int bIndex) {
		int i;
		int numNames =names.length;
		
		String nameToMove = names[bIndex];
		for(i=bIndex;i<numNames-1;i++)
			names[i] = names[i+1];
		names[i] = nameToMove;

		return names;
	}

}
