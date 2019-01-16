package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ReplaceColumnValueReactor extends AbstractRFrameReactor{
	
	/**
	 * This reactor updates row values to a new value based on the existing value
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the old value
	 * 3) the new value
	 */
	
	public ReplaceColumnValueReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.NEW_VALUE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get table name
		String table = frame.getName();

		// get inputs
		// first input is the column that we are updating
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		// second input is the old value
		String oldValue = this.keyValue.get(this.keysToGet[1]);

		// third input is the new value
		String newValue = this.keyValue.get(this.keysToGet[2]);

		// use method to retrieve a single column type
		String colDataType = getColumnType(table, column);
		
		// account for quotes that will be needed in the query with string values
		String neededQuote = "";
		if (colDataType.equalsIgnoreCase("string") || colDataType.equalsIgnoreCase("character")) {
			neededQuote = "\"";
		} else if (colDataType.equalsIgnoreCase("factor")) {
			changeColumnType(frame, table, column, "string", "%Y/%m/%d");
			neededQuote = "\"";
		}
			
		String script = "";
		if (oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("NA")) {
			script = table + "$" + column + "[is.na(" + table + "$" + column + ")] <- " + neededQuote + newValue + neededQuote;
		} else {
			script = table + "$" + column + "[" + table + "$" + column + " == " + neededQuote + oldValue + neededQuote + "] <- " + neededQuote + newValue + neededQuote;
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ReplaceColumnValue", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
			
		// execute the r script
		// script is of the form FRAME$Director[FRAME$Director == "oldVal"] <- "newVal"
		frame.executeRScript(script);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
