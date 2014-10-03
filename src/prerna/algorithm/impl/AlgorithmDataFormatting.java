package prerna.algorithm.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

public class AlgorithmDataFormatting {

	boolean[] isCategorical;
	
	public AlgorithmDataFormatting() {
		
	}
	
	public boolean[] getIsCategorical(){
		return isCategorical;
	}
	
	//TODO: get to work with nulls/missing data
	//TODO: already parse through data in clustering data processor, better way to improve efficiency?
	//TODO: this uses indexing starting at 1 because this doesn't include the actual instance node name
	public Object[][] manipulateValues(ArrayList<Object[]> queryData) {
		int counter = 0;
		
		int numProps = queryData.get(0).length;
		Object[][] data = new Object[numProps-1][queryData.size()];
		isCategorical = new boolean[numProps];
		Object[][] trackType = new Object[numProps][queryData.size()];
		
		int i;
		int size = queryData.size();
		for(i = 0; i < size; i++) {
			Object[] dataRow = queryData.get(i);
			int j;
			for(j = 1; j < numProps - 1; j++) {
				data[j][counter] = dataRow[j];
				trackType[j][counter] = processType(dataRow[j].toString());
			}
			counter++;
		}
		
		for(i = 1; i < numProps - 1; i++) {
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
	
	//TODO: get to work with nulls/missing data
	//TODO: already parse through data in clustering data processor, better way to improve efficiency?
	//TODO: this uses indexing starting at 0 because this doesn't include the actual instance node name
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
				trackType[j][counter] = processType(dataRow[j].toString());
			}
			counter++;
		}
		
		for(i = 0; i < numProps; i++) {
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
	
	/**
	 * Determines the type of a given value
	 * @param s		The value to determine the type off
	 * @return		The type of the value
	 */
	private static String processType(String s) {
		
		boolean isDouble = true;
		try {
			Double.parseDouble(s);
		} catch(NumberFormatException e) {
			isDouble = false;
		}

		if(isDouble) {
			return ("DOUBLE");
		}

		// will analyze date types as numerical data
		Boolean isLongDate = true;
		SimpleDateFormat formatLongDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
		Date longdate = null;
		try {
			formatLongDate.setLenient(true);
			longdate  = formatLongDate.parse(s);
		} catch (ParseException e) {
			isLongDate = false;
		}
		if(isLongDate){
			return ("DATE");
		}

		Boolean isSimpleDate = true;
		SimpleDateFormat formatSimpleDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
		Date simpleDate = null;
		try {
			formatSimpleDate.setLenient(true);
			simpleDate  = formatSimpleDate.parse(s);
		} catch (ParseException e) {
			isSimpleDate = false;
		}
		if(isSimpleDate){
			return ("SIMPLEDATE");
		}

		return ("STRING");
	}
	
	
//	//TODO: make generic
//	private void formatDuplicateResults() {
//		int instanceCounter = 0;
//		String previousInstance = "";
//
//		int i;
//		int numRows = masterTable.size();
//		int numCols = masterTable.get(0).length;
//		String[] uniquePropNames = new String[50];
//		String[] instances = new String[50];
//		int counter = 0;
//		for(i = 0; i < numCols; i++ ) {
//			String previousProp = "";
//			int j;
//			for(j = 0; j < numRows; j++) {
//				Object[] row = masterTable.get(j);
//
//				if(i == 0) {
//					if(previousInstance.equals(row[i])){
//						continue;
//					} else {
//						previousInstance = row[i].toString();
//						try{
//							instances[instanceCounter] = previousInstance;
//						} catch(IndexOutOfBoundsException ex) {
//							instances = (String[]) ArrayUtilityMethods.resizeArray(instances, 2);
//							instances[instanceCounter] = previousInstance;
//						}
//						instanceCounter++;
//					}
//				} else {
//					if(previousProp.equals(row[i])) {
//						continue;
//					} else {
//						previousProp = row[i].toString();
//						try{
//							uniquePropNames[counter] = row[i].toString();
//						} catch(IndexOutOfBoundsException ex) {
//							uniquePropNames = (String[]) ArrayUtilityMethods.resizeArray(uniquePropNames, 2);
//							uniquePropNames[counter] = row[i].toString();
//						}
//						counter++;
//					}
//				}
//			}
//		}
//
//		instances = (String[]) ArrayUtilityMethods.removeAllNulls(instances);
//		int numInstances = instances.length;
//		if(numInstances == numRows) {
//			return;
//		}
//		ArrayList<Object[]> retMasterTable = new ArrayList<Object[]>();
//
//		uniquePropNames = (String[]) ArrayUtilityMethods.removeAllNulls(uniquePropNames);
//		uniquePropNames = ArrayUtilityMethods.getUniqueArray(uniquePropNames);
//		varNames = uniquePropNames;
//
//		int newNumCols = uniquePropNames.length;
//		for(i = 0; i < numInstances; i++) {
//			Object[] newRow = new Object[newNumCols + 1];
//			newRow[0] = instances[i];
//			retMasterTable.add(newRow);
//		}
//
//		OUTER: for(i = 0; i < numRows; i++) {
//			Object[] row = masterTable.get(i);
//			int j;
//			for(j = 0; j < numInstances; j++) {
//				Object[] newRow = retMasterTable.get(j);
//				if(row[0].equals(newRow[0])){
//					int k;
//					INNER: for(k = 0; k < newNumCols; k++) {
//						int l;
//						for(l = 1; l < row.length; l++) {
//							if(uniquePropNames[k].toString().equals(row[l].toString())){
//								newRow[k+1] = "Yes";
//								continue INNER;
//							} else {
//								newRow[k+1] = "No";
//							}
//						}
//					}
//					continue OUTER;
//				}
//			}
//		}
//
//		masterTable = retMasterTable;
//	}
	
}
