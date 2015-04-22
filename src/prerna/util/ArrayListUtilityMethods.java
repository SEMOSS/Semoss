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
package prerna.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Vector;

public final class ArrayListUtilityMethods {

	private ArrayListUtilityMethods() {

	}

	public static Object[] getColumnFromList(ArrayList<Object[]> list, int colToGet) {
		if(list == null || list.isEmpty()) {
			return null;
		}

		int numRows = list.size();
		Object[] retList = new Object[numRows];

		int i;
		for(i = 0; i < numRows; i++) {
			Object[] oldRow = list.get(i);
			retList[i] = oldRow[colToGet];
		}

		return retList;
	}

	public static ArrayList<Object[]> removeColumnFromList(ArrayList<Object[]> list, int colToRemove) {
		if(list == null || list.isEmpty()) {
			return null;
		}

		int numRows = list.size();
		int numCols = list.get(0).length;
		ArrayList<Object[]> retList = new ArrayList<Object[]>(numRows);

		int i;
		int j;
		for(i = 0; i < numRows; i++) {
			Object[] newRow = new Object[numCols - 1];
			Object[] oldRow = list.get(i);
			int counter = 0;
			for(j = 0; j < numCols; j++) {
				if(j != colToRemove) {
					newRow[counter] = oldRow[j];
					counter++;
				}
			}
			retList.add(newRow);
		}

		return retList;
	}

	/**
	 * Returns a specific range of rows from the original list passed in
	 * @param list			The main list you plan on taking a section from
	 * @param startRow		The first row you want returned from the list, inclusive
	 * @param endRow		The last row you want returned from the list, exclusive
	 * @return				The portioned list from row startNum to row endNum 
	 */
	public static ArrayList<Object[]> getRowRangeFromList(ArrayList<Object[]> list, int startRow, int endRow) {
		if(list == null) {
			throw new NullPointerException("list is null");
		}
		int size = list.size();
		if(startRow < 0) {
			throw new IllegalArgumentException("startRow, " + startRow + ", must be larger than 0");
		} 
		if(startRow > size) {
			throw new IllegalArgumentException("startRow, " + startRow + " is larger than the size of the list, " + size);
		}
		if(endRow < 0) {
			throw new IllegalArgumentException("endRow, " + endRow +", must be larger than 0");
		}
		if(endRow > size) {
			throw new IllegalArgumentException("endRow, " + endRow + ", is larger than the size of the list, " + size);
		}
		if(endRow <= startRow) {	
			throw new IllegalArgumentException("startRow, " + startRow +", is larger than or equal to endRow, " + endRow);
		}

		ArrayList<Object[]> retList = new ArrayList<Object[]>(endRow-startRow);
		int i = startRow;
		for(; i < endRow; i++) {
			retList.add(list.get(i));
		}

		return retList;
	}

	public static ArrayList<Object[]> filterList(ArrayList<Object[]> list, boolean[] include) {
		int size = 0;
		for(boolean val : include) {
			if(val) {
				size++;
			}
		}

		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		for(Object[] row : list) {
			Object[] newRow = new Object[size];
			int nextIndex=0;
			for(int i=0;i<row.length;i++) {
				if(include[i]) {
					newRow[nextIndex]=row[i];
					nextIndex++;
				}
			}
			newList.add(newRow);
		}
		return newList;
	}

	public static ArrayList<Object[]> filterList(ArrayList<Object[]> list, Boolean[] include) {
		int size = 0;
		for(boolean val : include) {
			if(val) {
				size++;
			}
		}

		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		for(Object[] row : list) {
			Object[] newRow = new Object[size];
			int nextIndex=0;
			for(int i=0;i<row.length;i++) {
				if(include[i]) {
					newRow[nextIndex]=row[i];
					nextIndex++;
				}
			}
			newList.add(newRow);
		}
		return newList;
	}
	
	public static ArrayList<Object[]> orderQuery(ArrayList<Object[]> queryResults){
		ArrayList<Object[]> sortedQuery = new ArrayList<Object[]>();
		ArrayList<String> sortingList = new ArrayList<String>();
		for(Object[] o : queryResults){
			String objectContents = "";
			for(int i = 0; i < o.length; i++){
				objectContents += o[i].toString() + "+++";
			}
			sortingList.add(objectContents);
		}

		Collections.sort(sortingList, new Comparator<String>(){
			public int compare(String s1, String s2){
				return s1.compareTo(s2);
			}
		});

		for(String s : sortingList){
			String[] data = s.split("\\+\\+\\+");
			sortedQuery.add((Object[]) data);
		}

		return sortedQuery;
	}
	
	/**
	 * Helper method to determining the list of systems.
	 * Performs an AND union on two lists if neither is empty.
	 * If one is empty, returns the other list.
	 * @param list1
	 * @param list2
	 * @return
	 */
	public static Vector<String> createAndUnionIfBothFilled(Vector<String> list1,Vector<String> list2) {
		if(list1.isEmpty())
			return list2;
		if(list2.isEmpty())
			return list1;
		return createAndUnion(list1,list2);
	}
	
	/**
	 * Helper method to determining the list of systems.
	 * Performs an AND union on two lists.
	 * @param list1
	 * @param list2
	 * @return
	 */
	public static Vector<String> createAndUnion(Vector<String> list1,Vector<String> list2) {
		Vector<String> retList = new Vector<String>();
		Iterator<String> it1 = list1.iterator();
		while(it1.hasNext()) {
			String check = it1.next();
			if(list2.contains(check)&&!retList.contains(check))
				retList.add(check);
		}
		return retList;
	}
	
	/**
	 * Helper method to determining the list of systems.
	 * Performs an OR union on two lists.
	 * @param list1
	 * @param list2
	 * @return
	 */
	public static Vector<String> createOrUnion(Vector<String> list1,Vector<String> list2) {
		Vector<String> retList = new Vector<String>();
		Iterator<String> it1 = list1.iterator();
		while(it1.hasNext()) {
			String check = it1.next();
			if(!retList.contains(check))
				retList.add(check);
		}
		Iterator<String> it2 = list2.iterator();
		while(it2.hasNext()) {
			String check = it2.next();
			if(!retList.contains(check))
				retList.add(check);
		}
		return retList;
	}


}
