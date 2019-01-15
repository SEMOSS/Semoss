package prerna.sablecc2.reactor.frame.r;

import org.rosuda.JRI.RFactor;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.util.Utility;

public class ColumnCountReactor extends AbstractRFrameReactor {

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
		// initialize the rJavaTranslator
		init();
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
		RDataTable frame = (RDataTable) getFrame();
		//get frame name
		String table = frame.getName();

		String colType = this.rJavaTranslator.getColumnType(table, column);
		if(colType.equals("int") || colType.equals("num") || colType.equals("numeric")) {
			//this accounts for the case that the values are numeric - just make a histogram using the histogram reactor
			//without this section we get errors because the column values are not string or factors
			HistogramReactor histogram = new HistogramReactor();
			//define the number of breaks to build the histogram
			int numBreaks = 0;
			//the get histogram method return nounmetadata
			return histogram.getHistogram(this.rJavaTranslator, table, column, panelId, numBreaks);
		}

		//create temporary table
		String tempName = Utility.getRandomString(6);
		//define r script to be executed
		//this script will create a table with one column of col vals and one column of the corresponding frequency
		String script = null;

		//sort based on boolean top variable; if true, sort descending
		//more frequent items in the column will appear first
		if (top) {
			script = tempName + " <-  head(" + table + "[, .N, by=\"" + column + "\"][order(-rank(N)),] , 25);";
		} else {
			script = tempName + " <-  head(" + table + "[, .N, by=\"" + column + "\"][order(rank(N)),] , 25);";
		}
		this.rJavaTranslator.executeEmptyR(script);

		// store the values of the column in a string array
		// get the column names
		script = tempName + "$" + column;
		String[] uniqueColumns = null;
		if (colType.equalsIgnoreCase("date")) {
			String dateFormat = "%Y-%m-%d";
			uniqueColumns = this.rJavaTranslator.getStringArray("format(" + script + ", format='" + dateFormat + "')");
		} else {
			uniqueColumns = this.rJavaTranslator.getStringArray(script);
		}
		// if its still null
		// we have a factor
		if (uniqueColumns == null) {
			RFactor factors = (RFactor) this.rJavaTranslator.getFactor(script);
			int numFactors = factors.size();
			uniqueColumns = new String[numFactors];
			for (int i = 0; i < numFactors; i++) {
				uniqueColumns[i] = factors.at(i);
			}
		}

		// this will store a count of each values occurrence in the column
		script = tempName + "$N";
		int[] colCount = this.rJavaTranslator.getIntArray(script);

		// create the object with the right size
		// the length will be the same as the number of unique values in the column
		Object[][] retOutput = new Object[uniqueColumns.length][2];

		for (int outputIndex = 0; outputIndex < uniqueColumns.length; outputIndex++) {
			//we are storing each uniqe col val and its frequency
			retOutput[outputIndex][0] = uniqueColumns[outputIndex];
			retOutput[outputIndex][1] = colCount[outputIndex];
		}

		// create and return a task
		ITask taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, column, "Frequency", retOutput);

		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + tempName + "); gc();");
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
