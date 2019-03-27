package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;

public class ColumnCountReactor extends AbstractFrameReactor {

	/**
	 * This reactor counts the number of columns and unique columns
	 * it stores these values in a matrix
	 * 1) column to use
	 * 2) boolean indicator (optional)
	 * 		if true (default), sort by descending frequency of items in a column
	 * 		if false, sort ascending
	 * 3) panelId (defaults to zero if nothing is entered)
	 */

	private static final String TOP = "top";

	public ColumnCountReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), TOP, ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		//get inputs
		String column = getColumn();
		//clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		//get boolean top variable
		boolean top = getTop();
		//get panel id in order to display
		String panelId = getPanelId();

		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		//get frame name
		String table = frame.getName();

		ArrayList output = (ArrayList)frame.runScript(table+".get_hist('" + column + "')");

		// create the object with the right size
		// the length will be the same as the number of unique values in the column
		ArrayList keys = (ArrayList)output.get(0);
		ArrayList vals = (ArrayList)output.get(1);
		Object[][] retOutput = new Object[keys.size()][2];

		for (int outputIndex = 0; outputIndex < keys.size(); outputIndex++) {
			//we are storing each uniqe col val and its frequency
			retOutput[outputIndex][0] = keys.get(outputIndex);
			retOutput[outputIndex][1] = vals.get(outputIndex);
		}

		// create and return a task
		ITask taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, column, "Frequency", retOutput);

		// variable cleanup
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
				throw new IllegalArgumentException("Need to define column for column count");
			}
			return column;
		}
		throw new IllegalArgumentException("Need to define column for column count");
	}

	private boolean getTop() {
		GenRowStruct topGRS = this.store.getNoun(TOP);
		if (topGRS != null) {
			NounMetadata noun2 = topGRS.getNoun(0);
			String topString = noun2.getValue().toString();
			if (topString.equalsIgnoreCase("false")) {
				return false;
			} else {
				// return true if input is anything other than false
				return true;
			}
		}
		// default to true
		return true;
	}

	// get panel id using key "PANEL"
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[2]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return "0";
	}

	//////////////////////////////////KEYS////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(TOP)) {
			return "Indicates if a column should be sorted by descending frequency";
		} else {
			return super.getDescriptionForKey(key);
		}
	}


}
