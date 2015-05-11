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
package prerna.algorithm.learning.similarity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class ClusterRemoveDuplicatesNumerical {

	private static final Logger LOGGER = LogManager.getLogger(ClusterRemoveDuplicatesNumerical.class.getName());
	
	private ArrayList<Object[]> retMasterTable;
	private String[] retVarNames;
	
	public ClusterRemoveDuplicatesNumerical(ArrayList<Object[]> masterTable, String[] names) {
			formatDuplicateResults(masterTable, names);
	}
	
	public ClusterRemoveDuplicatesNumerical(ArrayList<Object[]> masterTable, String[] names, int column) {
		if(!(column==0))
			formatDuplicateResults(normalize(masterTable, column), adjustNames(names, column));
		else
			formatDuplicateResults(masterTable, names);
	}

	public ArrayList<Object[]> getRetMasterTable() {
		return retMasterTable;
	}

	public String[] getRetVarNames() {
		return retVarNames;
	}
	
	public ArrayList<Object[]> normalize(ArrayList<Object[]> masterTable, int column){
		ArrayList<Object[]> nMasterTable = new ArrayList<Object[]>();
		
		Object[] o;
		for(int i=0; i<masterTable.size(); i++) {
			o = masterTable.get(i);
			Object[] row = new Object[o.length];
			for(int j=0; j<o.length; j++){
				if(j==0){
					row[0] = o[column];
				} else if(j<=column){
					row[j] = o[j-1];
				} else {
					row[j]=o[j];
				}
			}
			nMasterTable.add(row);
		}
		return nMasterTable;
	}
	
	public String[] adjustNames(String[] names, int column){
		String[] retNames = new String[names.length];
		for(int j=0; j<retNames.length; j++){
			if(j==0){
				retNames[0] = names[column];
			} else if(j<=column){
				retNames[j] = names[j-1];
			} else {
				retNames[j]=names[j];
			}
		}
		return retNames;
	}

	public void formatDuplicateResults(ArrayList<Object[]> masterTable, String[] names) {
		int i;
		int j;
		int numRows = masterTable.size();
		int numCols = masterTable.get(0).length;
		Set<String> instancesSet = new HashSet<String>();
		
		// first check that there are duplicates in instances
		for(i = 0; i < numRows; i++) {
			instancesSet.add(masterTable.get(i)[0].toString());
		}
		
		
		int numInstances = instancesSet.size();
		/*if(numInstances == numRows) {
			retMasterTable = masterTable;
			retVarNames = names;
			return;
		}*/
		
		// if there are duplicates in results
		// determine which columns are numeric and which are categorical
		boolean[] isNumeric = new boolean[numCols - 1]; // minus 2 since instances are not included
		for(j = 1; j < numCols; j++) {
			int countNumeric = 0;
			int countCategorical = 0;
			
			for(i = 0; i < numRows; i++) {
				Object[] row = masterTable.get(i);
				String type = Utility.processType(row[j].toString());
				if(type.equals("STRING")) {
					countCategorical++;
				} else {
					countNumeric++;
				}
			}
			if(countCategorical > countNumeric) {
				isNumeric[j-1] = false;
			} else {
				isNumeric[j-1] = true;
			}
		}
		
		Set<String> uniquePropNamesSet = new HashSet<String>();
		HashMap<String, HashMap<String, HashMap<String, Double>>> numericPropForInstance = new HashMap<String, HashMap<String, HashMap<String, Double>>>();
		
		for(j = 1; j < numCols; j++ ) {
			for(i = 0; i < numRows; i++) {
				Object[] row = masterTable.get(i);
				String instance = row[0].toString();
				// average all numerical values
				if(isNumeric[j-1]) {
					String numericPropName = names[j];
					double val = 0;
					try {
						val = (double) row[j];
					} catch(ClassCastException ex) {
						try {
							val = Double.parseDouble(row[j].toString());
						} catch(NumberFormatException e) {
							LOGGER.info("Error processing " + row[j] + " as a numerical value");
							continue;
						}
					}
					
					HashMap<String, HashMap<String, Double>> innerHash;
					if(numericPropForInstance.containsKey(instance)) {
						innerHash = numericPropForInstance.get(instance);
						if(innerHash.containsKey(numericPropName)) {
							double newVal = val + innerHash.get(numericPropName).get("value");;
							innerHash.get(numericPropName).put("value", newVal);
							double newCount = 1 + innerHash.get(numericPropName).get("count");
							innerHash.get(numericPropName).put("count", newCount);
						} else {
							HashMap<String, Double> propHash = new HashMap<String, Double>();
							propHash.put("value", val);
							propHash.put("count", 1.0);
							innerHash.put(numericPropName, propHash);
						}
					} else {
						HashMap<String, Double> propHash = new HashMap<String, Double>();
						innerHash = new HashMap<String, HashMap<String, Double>>();
						propHash.put("value", val);
						propHash.put("count", 1.0);
						innerHash.put(numericPropName, propHash);
						numericPropForInstance.put(instance, innerHash);
					}
				} 
				// keep track of all categorical properties that create duplicate columns and make those instances new columns
				else {
					if(!row[j].toString().isEmpty()) {
						uniquePropNamesSet.add(names[j] + "_" + row[j].toString());
					}
				}
			}
		}
		
		retMasterTable = new ArrayList<Object[]>();
		String[] uniquePropNames = ArrayUtilityMethods.convertObjArrToStringArr(uniquePropNamesSet.toArray());
		
		Set<String> numericPropNamesSet = new HashSet<String>();
		for(i = 1; i < names.length; i++) {
			if(isNumeric[i-1]) {
				numericPropNamesSet.add(names[i]);
			}
		}
		String[] numericPropNames = ArrayUtilityMethods.convertObjArrToStringArr(numericPropNamesSet.toArray());
		String[] instances = ArrayUtilityMethods.convertObjArrToStringArr(instancesSet.toArray());
		
		retVarNames = Arrays.copyOf(new String[]{names[0]}, 1 + uniquePropNames.length + numericPropNames.length);
		System.arraycopy(uniquePropNames, 0, retVarNames, 1, uniquePropNames.length);
		System.arraycopy(numericPropNames, 0, retVarNames, 1 + uniquePropNames.length, numericPropNames.length);

		int newCategoricalCols = uniquePropNames.length;
		int newNumericCols = numericPropNames.length;
		int newNumCols = newCategoricalCols + newNumericCols;
		for(i = 0; i < numInstances; i++) {
			Object[] newRow = new Object[newNumCols + 1];
			newRow[0] = instances[i];
			retMasterTable.add(newRow);
		}
		
		// add in categorical information
		OUTER: for(i = 0; i < numRows; i++) {	
			Object[] row = masterTable.get(i);
			for(j = 0; j < numInstances; j++) {
				Object[] newRow = retMasterTable.get(j);
				if(row[0].toString().equals(newRow[0].toString())) {
					int k;
					INNERMOST: for(k = 0; k < newCategoricalCols; k++) {
						int l;
						for(l = 1; l < row.length; l++) {
							if(uniquePropNames[k].toString().equals(names[l].toString().concat("_").concat(row[l].toString()).toString())){
								newRow[k+1] = 1;
								continue INNERMOST;
							}
						}
					}
					continue OUTER;
				}
			}
		}
		
		// any other parts which are null get a value of no
		for(i = 0; i < retMasterTable.size(); i++)
		{
			Object[] newRow = retMasterTable.get(i);
			for(j = 0; j < newCategoricalCols; j++) {
				if(newRow[j+1] == null) {
					newRow[j+1] = 0;
				}
			}
		}
		
		// add in numerical information
		int newNumRow = retMasterTable.size();
		for(i = 0; i < newNumRow; i++) {	
			Object[] newRow = retMasterTable.get(i);
			int retIndex = newCategoricalCols + 1;
			HashMap<String, HashMap<String, Double>> innerHash = numericPropForInstance.get(newRow[0].toString());
			for(j = 0; j < newNumericCols; j++) {
				if(innerHash.containsKey((numericPropNames[j]))) {
					double val = innerHash.get(numericPropNames[j]).get("value") / innerHash.get(numericPropNames[j]).get("count");
					newRow[retIndex] = val;
					retIndex++;
				} else {
					newRow[retIndex] = "";
					retIndex++;
				}
			}
		}
	}		
}
