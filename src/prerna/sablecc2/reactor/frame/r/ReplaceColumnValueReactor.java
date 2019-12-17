package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
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
	
	private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");
	
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
		List<String> columnNames = getColumns();

		// second input is the old value
		String oldValue = this.keyValue.get(this.keysToGet[1]);

		// third input is the new value
		String newValue = this.keyValue.get(this.keysToGet[2]);
		
		// iterate through all passed columns
		StringBuilder script = new StringBuilder();
		for (String column : columnNames) {
		
			// use method to retrieve a single column type
			String colDataType = getColumnType(table, column);
			SemossDataType sType = SemossDataType.convertStringToDataType(colDataType);
			
			if(sType == SemossDataType.INT || sType == SemossDataType.DOUBLE) {
				// make sure the new value can be properly casted to a number
				if(newValue.isEmpty() || newValue.equals("null") || newValue.equals("NA") || newValue.equals("NaN")) {
					newValue = "NaN";
				} else if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
					throw new IllegalArgumentException("Cannot update a numeric field with string value = " + newValue);
				}
				
				// account for nulls
				// account for NA
				// account for NaN
				if(oldValue.isEmpty() || oldValue.equals("null") || oldValue.equals("NA") || oldValue.equals("NaN")) {
					script.append(table + "$" + column + "[is.na(" + table + "$" + column + ")] <- " + newValue + ";");
				} else {
					script.append(table + "$" + column + "[" + table + "$" + column + " == " + oldValue + "] <- " + newValue + ";");
				}
			} else if(sType == SemossDataType.DATE) {
				if(oldValue.isEmpty() || oldValue.equals("null") || oldValue.equals("NA") || oldValue.equals("NaN")) {
					script.append(table + "$" + column + "[is.na(" + table + "$" + column + ")] <- as.Date(\"" + newValue + "\");");
				} else {
					script.append(table + "$" + column + "[" + table + "$" + column + " == \"" + oldValue + "\"] <- as.Date(\"" + newValue + "\");");
				}
				
			} else if(sType == SemossDataType.TIMESTAMP) {
				if(oldValue.isEmpty() || oldValue.equals("null") || oldValue.equals("NA") || oldValue.equals("NaN")) {
					script.append(table + "$" + column + "[is.na(" + table + "$" + column + ")] <- as.POSIXct(\"" + newValue + "\");");
				} else {
					script.append(table + "$" + column + "[" + table + "$" + column + " == \"" + oldValue + "\"] <- as.POSIXct(\"" + newValue + "\");");
				}
				
			} else if(sType == SemossDataType.STRING) {
				// escape and update
				String escapedOldValue = oldValue.replace("\"", "\\\"");
				String escapedNewValue = newValue.replace("\"", "\\\"");
				script.append(table + "$" + column + "[" + table + "$" + column + " == \"" + escapedOldValue + "\"] <- \"" + escapedNewValue + "\";");

			} else if(sType == SemossDataType.FACTOR) {
				// need to convert factor to string since factor is defined as a predefined list of values
				changeColumnType(frame, table, column, "string", null);
				
				// this is same as string now
				// escape and update
				String escapedOldValue = oldValue.replace("\"", "\\\"");
				String escapedNewValue = newValue.replace("\"", "\\\"");
				script.append(table + "$" + column + "[" + table + "$" + column + " == \"" + escapedOldValue + "\"] <- \"" + escapedNewValue + "\";");
			}
		}

		// execute the r script
		// script is of the form FRAME$Director[FRAME$Director == "oldVal"] <- "newVal"
		frame.executeRScript(script.toString());

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ReplaceColumnValue", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
			
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	private List<String> getColumns() {
		List<String> cols = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				String column =grs.get(i).toString();
				if (column.contains("__")) {
					column = column.split("__")[1];
				}
				cols.add(column);
			}
			return cols;
		}
		
		return cols;
	}
}
