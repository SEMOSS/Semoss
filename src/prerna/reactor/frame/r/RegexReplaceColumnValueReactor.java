package prerna.reactor.frame.r;

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

public class RegexReplaceColumnValueReactor extends AbstractRFrameReactor {

	/**
	 * This reactor updates row values based on a regex
	 * It replaces all portions of the current cell value that is an exact match to the input value
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the regex to look for
	 * 3) value to replace the regex with 
	 */
	
	private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");
	private static final String QUOTE = "\"";

	public RegexReplaceColumnValueReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.NEW_VALUE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		//initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		
		//get table name
		String table = frame.getName();
		
		// get inputs
		// first input is the column that we are updating
		List<String> columnNames = getColumns();

		//get regular expression
		String regex = this.keyValue.get(this.keysToGet[1]);
		if (regex == null) {
			regex = getRegex();
		}
		
		//get new value
		String newValue = this.keyValue.get(this.keysToGet[2]);
		if (newValue == null) {
			newValue = getNewValue();
		}
		
		// iterate through all passed columns
		StringBuilder script = new StringBuilder();
		for (String column : columnNames) {
			
			// use method to retrieve a single column type
			String columnSelect = table + "$" + column;
			String colDataType = getColumnType(table, column);
			if(colDataType == null)
				return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");

			SemossDataType sType = SemossDataType.convertStringToDataType(colDataType);

			// script is of the form FRAME$Genre = gsub("-","M", FRAME$Genre)

			if(sType == SemossDataType.INT) {
				// make sure the new value can be properly casted to a number
				if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
					throw new IllegalArgumentException("Cannot update a numeric field to non-numeric values");
				}
				
				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", " + columnSelect + ");");
				// turn back to int
				script.append(columnSelect + "<- as.integer(" + columnSelect + ");");
				
			} else if(sType == SemossDataType.DOUBLE) {
				// make sure the new value can be properly casted to a number
				if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
					throw new IllegalArgumentException("Cannot update a numeric field to non-numeric values");
				}
				
				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", " + columnSelect + ");");
				// turn back to numeric
				script.append(columnSelect + "<- as.numeric(" + columnSelect + ");");

			} else if(sType == SemossDataType.DATE) {
				// NOT VALID - WHAT IF I WANT TO UPDATE A MONTH - DAY PORTION ?
//				if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
//					throw new IllegalArgumentException("Cannot update a date field to non-numeric values");
//				}
				
				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", " + columnSelect + ");");
				// turn back to date
				script.append(columnSelect + "<- as.Date(" + columnSelect + ");");
				
			} else if(sType == SemossDataType.TIMESTAMP) {
				// NOT VALID - WHAT IF I WANT TO UPDATE A MONTH - DAY PORTION ?
//				if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
//					throw new IllegalArgumentException("Cannot update a date field to non-numeric values");
//				}
				
				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", " + columnSelect + ");");
				// turn back to timestamp
				script.append(columnSelect + "<- as.POSIXct(" + columnSelect + ");");
				
			} else if(sType == SemossDataType.STRING) {
				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", " + columnSelect + ");");
				
			} else if(sType == SemossDataType.FACTOR) {
				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", " + columnSelect + ");");
				// turn back to factor
				script.append(columnSelect + "<- as.factor(" + columnSelect + ");");
				
				// TODO: account for ordered factor ...
				// TODO: account for ordered factor ...
			}
		}

		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"RegexReplaceColumnValue", 
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
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getUpdateColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//first input is the column that we are updating
			NounMetadata input1 = inputsGRS.getNoun(0);
			String column = input1.getValue() + "";
			if (column.length() == 0) {
				throw new IllegalArgumentException("Need to defne the column to be updated");
			}
			return column;
		}	
		throw new IllegalArgumentException("Need to define the column to be updated");
	}
	
	private String getRegex() {
		//second input is the regex
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String regex = input2.getValue() + ""; 
		return regex;
	}
	
	private String getNewValue() {
		//third input is the new value
		NounMetadata input3 = this.getCurRow().getNoun(2);
		String newValue = input3.getValue() + ""; 
		return newValue;
	}

}
