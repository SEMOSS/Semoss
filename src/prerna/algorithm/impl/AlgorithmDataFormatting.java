package prerna.algorithm.impl;

import java.util.ArrayList;

import prerna.util.Utility;

public class AlgorithmDataFormatting {

	boolean[] isCategorical;
	
	public AlgorithmDataFormatting() {
		
	}
	
	public boolean[] getIsCategorical() {
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
				trackType[j][counter] = Utility.processType(dataRow[j].toString());
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
				trackType[j][counter] = Utility.processType(dataRow[j].toString());
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
}
