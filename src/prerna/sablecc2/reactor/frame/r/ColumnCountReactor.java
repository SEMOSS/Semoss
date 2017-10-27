package prerna.sablecc2.reactor.frame.r;

import java.util.Map;

import org.rosuda.JRI.RFactor;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
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
	
	private static final String COLUMN = "column";
	private static final String TOP = "top";
	private static final String PANEL = "panel";

	@Override
	public NounMetadata execute() {
		
		// initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		//get frame name
		String table = frame.getTableName();
				
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
		
		//store output in a matrix
		Object [][] retOutput = null; // name and the number of items

		//create temporary table
		String tempName = Utility.getRandomString(6);
		
		//define r script to be executed
		//this script will create a table with one column of col vals and one column of the corresponding frequency
		String script = tempName + " <-  " + table + "[, .N, by=\"" + column + "\"];";
		frame.executeRScript(script);
		
		//sort based on boolean top variable; if true, sort descending
		//more frequent items in the column will appear first
		if (top) {
			frame.executeRScript(tempName + " <- " + tempName + "[order(-rank(N)),]");
		} else {
			frame.executeRScript(tempName + " <- " + tempName + "[order(rank(N)),]");
		}

		// get the column names
		
		//store the values of the column in a string array
		// get the column names
		String colType = this.rJavaTranslator.getColumnType(table, column);
		script = tempName + "$" + column;

		String[] uniqueColumns = this.rJavaTranslator.getStringArray(script);
		if (colType.equalsIgnoreCase("string") || colType.equalsIgnoreCase("factor") || colType.equalsIgnoreCase("character") || colType.equalsIgnoreCase("date")) {
			if (colType.equalsIgnoreCase("date")) {
				String dateFormat = "%Y-%m-%d";
				uniqueColumns = this.rJavaTranslator.getStringArray("format(" + table + "$" + column + ", format='" + dateFormat + "')");
			} 
			if (uniqueColumns == null) {
				RFactor factors = (RFactor) this.rJavaTranslator.getFactor(script);
				int numFactors = factors.size();
				uniqueColumns = new String[numFactors];
				for (int i = 0; i < numFactors; i++) {
					uniqueColumns[i] = factors.at(i);
				}
			}
		} else {
			//this accounts for the case that the values are numeric - just make a histogram using the histogram reactor
			//without this section we get errors because the column values are not string or factors
			HistogramReactor histogram = new HistogramReactor();
			//define the number of breaks to build the histogram
			int numBreaks = 0;
			//the get histogram method return nounmetadata
			return histogram.getHistogram(rJavaTranslator, table, column, panelId, numBreaks);
		}
		
		// this will store a count of each values occurrence in the column
		script = tempName + "$N";
		int[] colCount = this.rJavaTranslator.getIntArray(script);

		// create the object with the right size
		//the length will be the same as the number of uniqe values in the column
		if (uniqueColumns.length > 100) {
			retOutput = new Object[100][2];
		} else {
			retOutput = new Object[uniqueColumns.length][2];
		}

		int counter = 0;
		for (int outputIndex = 0; outputIndex < uniqueColumns.length && counter < 100; outputIndex++) {
			//we are storing each uniqe col val and its frequency
			retOutput[outputIndex][0] = uniqueColumns[outputIndex];
			retOutput[outputIndex][1] = colCount[outputIndex];
			counter++;
		}

		// create and return a task
		Map<String, Object> taskData = getBarChartInfo(panelId ,column, "Frequency", retOutput);

		// variable cleanup
		frame.executeRScript("rm(" + tempName + "); gc();");

		return new NounMetadata(taskData, PixelDataType.TASK, PixelOperationType.TASK_DATA);
		
	}
	
		//////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////
		///////////////////////// GET PIXEL INPUT ////////////////////////////
		//////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////

	private String getColumn() {
		GenRowStruct columnGRS = this.store.getNoun(COLUMN);
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
		GenRowStruct columnGrs = this.store.getNoun(PANEL);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return "0";
	}
}
