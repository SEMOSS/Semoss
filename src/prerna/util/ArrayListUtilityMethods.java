/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

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
			throw new IllegalArgumentException("startRow must be larger than 0");
		} 
		if(startRow > size) {
			throw new IllegalArgumentException("startRow is larger than the size of the list");
		}
		if(endRow < 0) {
			throw new IllegalArgumentException("endRow must be larger than 0");
		}
		if(endRow > size) {
			throw new IllegalArgumentException("endRow is larger than the size of the list");
		}
		if(endRow <= startRow) {	
			throw new IllegalArgumentException("startRow is larger than or equal to endRow");
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
	
}
