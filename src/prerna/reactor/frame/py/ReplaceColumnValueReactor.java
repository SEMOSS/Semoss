package prerna.reactor.frame.py;

import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ReplaceColumnValueReactor extends AbstractPyFrameReactor {
	
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
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get wrapper name
		String wrapperFrameName = frame.getWrapperName();

		// get inputs
		// first input is the column that we are updating
		List<String> columnNames = getColumns();

		// second input is the old value
		String oldValue = this.keyValue.get(this.keysToGet[1]);

		// third input is the new value
		String newValue = this.keyValue.get(this.keysToGet[2]);
		
		int numColumns = columnNames.size();
		String[] scripts = new String[columnNames.size()];
		
		// iterate through all passed columns
		for(int i = 0; i < numColumns; i++) {
			String column = columnNames.get(i);
			SemossDataType columnDataType = SemossDataType.convertStringToDataType(getColumnType(frame, column));

			if (columnDataType == SemossDataType.INT || columnDataType == SemossDataType.DOUBLE) {
				if(newValue.isEmpty() || newValue.equalsIgnoreCase("null") || newValue.equalsIgnoreCase("na") || newValue.equalsIgnoreCase("nan")) {
					newValue = "NaN";
				} else if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
					throw new IllegalArgumentException("Cannot update a numeric field with string value = " + newValue);
				}
				// Account for nulls, na, nan
				if (oldValue.isEmpty() || oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("na") || oldValue.equalsIgnoreCase("nan")) {
					scripts[i] = frame.getName() + "['" + column+ "'].fillna(" + newValue + ",inplace=True)";
				} else {
					scripts[i] = wrapperFrameName + ".replace_val('" + column + "'," + oldValue + " , " + newValue + ")";
				}
			} else if (columnDataType == SemossDataType.DATE) {
				if(oldValue.isEmpty() || oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("na") || oldValue.equalsIgnoreCase("nan") || oldValue.equalsIgnoreCase("nat")) {
					SemossDate newD = SemossDate.genDateObj(newValue);
					if(newD == null) {
						throw new IllegalArgumentException("Unable to parse new date value = " + newValue);
					}
					scripts[i] = frame.getName() + "['" + column + "'].fillna(value=pd.to_datetime('" + newD + "'),inplace=True)";
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
					scripts[i] = wrapperFrameName + ".replace_val('" + column + "', pd.to_datetime('" + oldD + "') , "
						+ "pd.to_datetime('" + newD + "') )";
				}
			} else if (columnDataType == SemossDataType.TIMESTAMP) {
				if(oldValue.isEmpty() || oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("na") || oldValue.equalsIgnoreCase("nan") || oldValue.equalsIgnoreCase("nat")) {
					SemossDate newD = SemossDate.genTimeStampDateObj(newValue);
					if(newD == null) {
						// try to cast to date object
						newD = SemossDate.genDateObj(newValue);
						if (newD == null) {
							throw new IllegalArgumentException("Unable to parse new date value = " + newValue);
						}
					}
					scripts[i] = frame.getName() + "['" + column + "'].fillna(value=pd.to_datetime('" + newD + "'),inplace=True)";
				} else {
					SemossDate oldD = SemossDate.genTimeStampDateObj(oldValue);
					SemossDate newD = SemossDate.genTimeStampDateObj(newValue);
					if (newD == null) {
						// try to cast to date object
						newD = SemossDate.genDateObj(newValue);
					}
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
					scripts[i] = wrapperFrameName + ".replace_val('" + column + "', pd.to_datetime('" + oldD + "') , "
						+ "pd.to_datetime('" + newD + "') )";
				}
			} else if (columnDataType == SemossDataType.STRING) {
				if (oldValue.equalsIgnoreCase("null")) {
					String escapedNewValue = newValue.replace("\"", "\\\"");
					scripts[i] = frame.getName() + "['" + column+ "'].fillna(" + escapedNewValue + ", inplace=True)";
				} else {
					scripts[i] = wrapperFrameName + ".replace_val('" + column + "', \"" + oldValue + "\", \"" + newValue + "\"" + ", False)";
				}
			}
		}
		
		// execute all of the routines after we have done our validation
		insight.getPyTranslator().runPyAndReturnOutput(scripts);
		for(String script : scripts) {
			this.addExecutedCode(script);
		}
		
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
