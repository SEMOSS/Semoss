package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class ClusterRemoveDuplicates {

	private ArrayList<Object[]> retMasterTable;
	private String[] retVarNames;
	
	public ClusterRemoveDuplicates(ArrayList<Object[]> masterTable, String[] names) {
		formatDuplicateResults(masterTable, names);
	}

	public ArrayList<Object[]> getRetMasterTable() {
		return retMasterTable;
	}

	public String[] getRetVarNames() {
		return retVarNames;
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
		if(numInstances == numRows) {
			retMasterTable = masterTable;
			retVarNames = names;
			return;
		}
		
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
		HashMap<String, HashMap<String, Object>> numericPropForInstance = new HashMap<String, HashMap<String, Object>>();
		
		for(j = 1; j < numCols; j++ ) {
			for(i = 0; i < numRows; i++) {
				Object[] row = masterTable.get(i);
				if(isNumeric[j-1]) {
					HashMap<String, Object> innerHash;
					if(numericPropForInstance.containsKey(row[0].toString())) {
						innerHash = numericPropForInstance.get(row[0].toString());
						innerHash.put(names[j], row[j]);
					} else {
						innerHash= new HashMap<String, Object>();
						innerHash.put(names[j], row[j]);
						numericPropForInstance.put(row[0].toString(), innerHash);
					}
				} else {
					uniquePropNamesSet.add(row[j].toString());
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
				if(row[0].equals(newRow[0])) {
					int k;
					INNERMOST: for(k = 0; k < newCategoricalCols; k++) {
						int l;
						for(l = 1; l < row.length; l++) {
							if(uniquePropNames[k].toString().equals(row[l].toString())){
								newRow[k+1] = "Yes";
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
					newRow[j+1] = "No";
				}
			}
		}
		
		// add in numerical information
		int newNumRow = retMasterTable.size();
		for(i = 0; i < newNumRow; i++) {	
			Object[] newRow = retMasterTable.get(i);
			int retIndex = newCategoricalCols + 1;
			HashMap<String, Object> innerHash = numericPropForInstance.get(newRow[0].toString());
			for(j = 0; j < newNumericCols; j++) {
				Object val = innerHash.get(numericPropNames[j]);
				newRow[retIndex] = val;
				retIndex++;
			}
		}
	}		
}
