package prerna.util;

import java.util.ArrayList;

public final class ArrayListUtilityMethods {
	
	private ArrayListUtilityMethods() {
		
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
	
	public static String[] removeNameFromList(String[] name, int colToRemove) {
		if(name == null || name.length == 0) {
			return null;
		}
		
		int numCols = name.length;

		String[] retNames = new String[numCols - 1];
		int i;
		int counter = 0;
		for(i = 0; i < numCols; i++) {
			if(i != colToRemove) {
				retNames[counter] = name[i];
				counter++;
			}
		}
		
		return retNames;
	}
	
}
