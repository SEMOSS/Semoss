package prerna.reactor.frame.r;

import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
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
	private static final String QUOTE = "\"";

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
			String columnSelect = table + "$" + column;
			String colDataType = getColumnType(table, column);

			if(colDataType == null)
				return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");

			SemossDataType sType = SemossDataType.convertStringToDataType(colDataType);
			
			if(sType == SemossDataType.INT || sType == SemossDataType.DOUBLE) {
				// make sure the new value can be properly casted to a number
				if(newValue.isEmpty() || newValue.equalsIgnoreCase("null") || newValue.equalsIgnoreCase("na") || newValue.equalsIgnoreCase("nan")) {
					newValue = "NaN";
				} else if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
					throw new IllegalArgumentException("Cannot update a numeric field with string value = " + newValue);
				}
				
				// account for nulls
				// account for NA
				// account for NaN
				if(oldValue.isEmpty() || oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("na") || oldValue.equalsIgnoreCase("nan")) {
					script.append(columnSelect + "[is.na(" + columnSelect + ")] <- " + newValue + ";");
				} else {
					script.append(columnSelect + "[" + columnSelect + " == " + oldValue + "] <- " + newValue + ";");
				}
			} else if(sType == SemossDataType.DATE) {
				if(oldValue.isEmpty() || oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("na") || oldValue.equalsIgnoreCase("nan")) {
					SemossDate newD = SemossDate.genDateObj(newValue);
					if(newD == null) {
						throw new IllegalArgumentException("Unable to parse new date value = " + newValue);
					}
					
					script.append(columnSelect + "[is.na(" + columnSelect + ")] <- as.Date(\"" + newD.getFormatted("yyyy-MM-dd") + "\");");
				} else {
					SemossDate oldD = SemossDate.genDateObj(oldValue);
					SemossDate newD = SemossDate.genDateObj(newValue);
					String error = "";
					if(oldD == null) {
						error = "Unable to parse old date value = " + oldValue;
					}
					if(newD == null) {
						error += ". Unable to parse new date value = " + newValue;
					}
					if(!error.isEmpty()) {
						throw new IllegalArgumentException(error);
					}
					
					script.append(columnSelect + "[" + columnSelect + " == " + QUOTE + oldD.getFormatted("yyyy-MM-dd") + QUOTE + "] <- as.Date(" + QUOTE + newD.getFormatted("yyyy-MM-dd") + QUOTE + ");");
				}
				
			} else if(sType == SemossDataType.TIMESTAMP) {
				if(oldValue.isEmpty() || oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("na") || oldValue.equalsIgnoreCase("nan")) {
					SemossDate newD = SemossDate.genTimeStampDateObj(newValue);
					if(newD == null) {
						throw new IllegalArgumentException("Unable to parse new date value = " + newValue);
					}
					
					script.append(columnSelect + "[is.na(" + columnSelect + ")] <- as.POSIXct(\"" + newD.getFormatted("yyyy-MM-dd HH:mm:ss") + "\");");
				} else {
					SemossDate oldD = SemossDate.genTimeStampDateObj(oldValue);
					SemossDate newD = SemossDate.genTimeStampDateObj(newValue);
					String error = "";
					if(oldD == null) {
						error = "Unable to parse old date value = " + oldValue;
					}
					if(newD == null) {
						error += ". Unable to parse new date value = " + newValue;
					}
					if(!error.isEmpty()) {
						throw new IllegalArgumentException(error);
					}
					
					script.append(columnSelect + "[" + columnSelect + " == " + QUOTE + oldD.getFormatted("yyyy-MM-dd") + QUOTE + "] <- as.POSIXct(" + QUOTE + newD.getFormatted("yyyy-MM-dd HH:mm:ss") + QUOTE + ");");
				}
				
			} else if(sType == SemossDataType.STRING) {
				if(oldValue.equalsIgnoreCase("null")) {
					String escapedNewValue = newValue.replace("\"", "\\\"");
					script.append(columnSelect + "[is.na(" + columnSelect + ")] <- " + QUOTE + escapedNewValue + QUOTE + ";");
				} else {
					// escape and update
					String escapedOldValue = oldValue.replace("\"", "\\\"");
					String escapedNewValue = newValue.replace("\"", "\\\"");
					script.append(columnSelect + "[" + columnSelect + " == " + QUOTE + escapedOldValue + QUOTE + "] <- " + QUOTE + escapedNewValue + QUOTE + ";");
					
				}

			} else if(sType == SemossDataType.FACTOR) {
				// need to convert factor to string since factor is defined as a predefined list of values
				script.append(columnSelect + "<- as.character(" + columnSelect + ");");
				// this is same as string now
				// escape and update
				if(oldValue.equalsIgnoreCase("null")) {
					String escapedNewValue = newValue.replace("\"", "\\\"");
					script.append(columnSelect + "[is.na(" + columnSelect + ")] <- " + QUOTE + escapedNewValue + QUOTE + ";");
				} else {
					// escape and update
					String escapedOldValue = oldValue.replace("\"", "\\\"");
					String escapedNewValue = newValue.replace("\"", "\\\"");
					script.append(columnSelect + "[" + columnSelect + " == " + QUOTE + escapedOldValue + QUOTE + "] <- " + QUOTE + escapedNewValue + QUOTE + ";");
				}
				// turn back to factor
				script.append(columnSelect + "<- as.factor(" + columnSelect + ");");
				
				// TODO: account for ordered factor ...
				// TODO: account for ordered factor ...
			}
		}

		// execute the r script
		// script is of the form FRAME$Director[FRAME$Director == "oldVal"] <- "newVal"
		frame.executeRScript(script.toString());
		this.addExecutedCode(script.toString());

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
