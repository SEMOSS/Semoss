package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;
import java.util.HashMap;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;

public class DescriptiveStatsReactor extends AbstractFrameReactor {

	/**
	 * This reactor gets statistics for a column
	 * 1) column to get stats on
	 * 2) panelId (defaults to zero if no panel id is entered)
	 */

	
	public DescriptiveStatsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		
		// initialize the rJavaTranslator
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		//get frame name
		String table = frame.getName();
				
		//get inputs
		String column = getColumn();
		//clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		
		//need panel id to display
		String panelId = getPanelId();
		
		HashMap output = (HashMap)frame.runScript(table+"w.stat('" + column + "')");
		ArrayList sum_median = (ArrayList)frame.runScript(table+"w.sum_median('" + column + "')");
		
		//if(output.size() == 8) // this was numeric and seems like we can only handle that ?
		//{
		
		//create the object to store the output in
		Object [][] retOutput = new Object[8][2]; // name and the number of items
		
		//get minimum
		retOutput[0][0] = "Minimum";
		retOutput[0][1] = ((HashMap)output.get("min")).get(column);

		//get quartiles
		retOutput[1][0] = "Q1";
		retOutput[1][1] = ((HashMap)output.get("25%")).get(column);
		retOutput[2][0] = "Q3";
		retOutput[2][1] = ((HashMap)output.get("75%")).get(column);
		
		//get maximum
		retOutput[3][0] = "Maximum";
		retOutput[3][1] = ((HashMap)output.get("max")).get(column);

		//get mean
		retOutput[4][0] = "Mean";
		retOutput[4][1] = ((HashMap)output.get("mean")).get(column);

		//get median
		retOutput[5][0] = "Median";
		retOutput[5][1] = sum_median.get(1);

		//get sum
		retOutput[6][0] = "Sum";
		retOutput[6][1] = sum_median.get(0);

		//get standard deviation
		retOutput[7][0] = "Standard Deviation";
		retOutput[7][1] = ((HashMap)output.get("std")).get(column);

		ITask taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, column, "StatOutput", retOutput);
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getColumn() {
		GenRowStruct columnGRS = this.store.getNoun(keysToGet[0]);
		if (columnGRS != null && !columnGRS.isEmpty()) {
			NounMetadata noun1 = columnGRS.getNoun(0);
			String column = noun1.getValue() + "";
			if (column.length() == 0) {
				throw new IllegalArgumentException("Need to define column for descriptive statistics");
			}
			return column;
		}
		throw new IllegalArgumentException("Need to define column for descriptive statistics");
	}
	
	// get panel id using key "PANEL"
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return "0";
	}
}
