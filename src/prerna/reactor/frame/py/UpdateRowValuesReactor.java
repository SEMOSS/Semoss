package prerna.reactor.frame.py;

import java.util.regex.Pattern;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.py.PandasFrame;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class UpdateRowValuesReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor updates row values to a new value based on a filter condition
	 * (where a column equals a specified value)
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the new value
	 * 3) the filter condition
	 */
	
	private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");
	private static final String QUOTE = "\"";
	
	public UpdateRowValuesReactor() {
		this.keysToGet = new String[] { 
				ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get frame name
		String frameName = frame.getName();
		String wrapperFrameName = frame.getWrapperName();

		// get inputs
		String column = getUpdateColumn();
		// separate the column name from the frame name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		// second noun will be the value to update (the new value)
		String newValue = getNewValue();

		// get data type of column being updated
		String colDataType = getColumnType(frame, column);
		SemossDataType sType = SemossDataType.convertStringToDataType(colDataType);

		// the third noun will be a filter; we can get the qs from this
		SelectQueryStruct qs = getQueryStruct();
		// get all of the filters from this querystruct
		GenRowFilters grf = qs.getExplicitFilters();
		if (grf.isEmpty()) {
			throw new IllegalArgumentException("Need to define filter condition");
		}

		// use RInterpreter to create filter syntax
		StringBuilder pyFilterBuilder = new StringBuilder();
		PandasInterpreter pi = new PandasInterpreter();
		pi.setDataTableName(frameName, wrapperFrameName + ".cache['data']");
		pi.setDataTypeMap(frame.getMetaData().getHeaderToTypeMap());
		pi.addFilters(grf.getFilters(), wrapperFrameName + ".cache['data']", pyFilterBuilder, true);

		// execute the r scripts
		if (pyFilterBuilder.length() <= 0) {
			throw new IllegalArgumentException("Must define a filter criteria");
		}
		
		String script = null;
		
		if(sType == SemossDataType.INT || sType == SemossDataType.DOUBLE) {
			// make sure the new value can be properly casted to a number
			if(newValue.isEmpty() || newValue.equalsIgnoreCase("null") || newValue.equalsIgnoreCase("na") || newValue.equalsIgnoreCase("nan")) {
				newValue = "np.NaN";
			} else if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
				throw new IllegalArgumentException("Cannot update a numeric field with string value = " + newValue);
			}
			script = wrapperFrameName + ".cache['data'].loc[" + pyFilterBuilder.toString() + ", '"+ column +"'] = " + newValue ;

		} else if(sType == SemossDataType.DATE) {
			// make sure the new value can be properly casted to a date
			if(newValue.isEmpty() || newValue.equalsIgnoreCase("null") || newValue.equalsIgnoreCase("na") || newValue.equalsIgnoreCase("nan")) {
				newValue = "NaT";
			} else {
				SemossDate newD = SemossDate.genDateObj(newValue);
				if(newD == null) {
					throw new IllegalArgumentException("Unable to parse new date value = " + newValue);
				}
				newValue = newD.getFormatted("yyyy-MM-dd");
			}
			script = wrapperFrameName + ".cache['data'].loc[" + pyFilterBuilder.toString() + ", '"+ column +"'] = np.datetime64(" + QUOTE + newValue + QUOTE + ", format='%Y-%m-%d')";

		} else if(sType == SemossDataType.TIMESTAMP) {
			// make sure the new value can be properly casted to a timestamp
			if(newValue.isEmpty() || newValue.equalsIgnoreCase("null") || newValue.equalsIgnoreCase("na") || newValue.equalsIgnoreCase("nan")) {
				newValue = "NaT";
			} else {
				SemossDate newD = SemossDate.genTimeStampDateObj(newValue);
				if(newD == null) {
					newD = SemossDate.genDateObj(newValue);
					if(newD == null) {
						throw new IllegalArgumentException("Unable to parse new date value = " + newValue);
					}
				}
				newValue = newD.getFormatted("yyyy-MM-dd HH:mm:ss");
			}
			script = wrapperFrameName + ".cache['data'].loc[" + pyFilterBuilder.toString() + ", '"+ column +"'] = np.datetime64(" + QUOTE + newValue + QUOTE + ", format='%Y-%m-%d %H:%M:%S')";

		} else if(sType == SemossDataType.STRING) {
			// escape and update
			String escapedNewValue = newValue.replace("\"", "\\\"");
			script = wrapperFrameName + ".cache['data'].loc[" + pyFilterBuilder.toString() + ", '"+ column +"'] = " + QUOTE + escapedNewValue + QUOTE;
		}
		
		// execute the script
		frame.runScript(script);
		this.addExecutedCode(script);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"UpdateRowValues", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getUpdateColumn() {
		GenRowStruct inputsGRS = this.store.getNoun(this.keysToGet[0]);
		if (inputsGRS == null) {
			inputsGRS = this.getCurRow();
		}
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first noun will be the column to update
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullUpdateCol = noun1.getValue() + "";
			if (fullUpdateCol.length() == 0) {
				throw new IllegalArgumentException("Need to define column to update");
			}
			return fullUpdateCol;
		}
		throw new IllegalArgumentException("Need to define column to update");
	}

	private String getNewValue() {
		GenRowStruct inputsGRS = this.store.getNoun(this.keysToGet[1]);
		if (inputsGRS != null) {
			return inputsGRS.get(0) + "";
		}
		inputsGRS = this.getCurRow();
		NounMetadata noun2 = inputsGRS.getNoun(1);
		String value = noun2.getValue() + "";
		return value;
	}

	private SelectQueryStruct getQueryStruct() {
		GenRowStruct inputsGRS = this.store.getNoun(this.keysToGet[2]);
		if (inputsGRS != null) {
			NounMetadata filterNoun = inputsGRS.getNoun(0);
			// filter is query struct pksl type
			// the qs is the value of the filterNoun
			SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
			if (qs == null) {
				throw new IllegalArgumentException("Need to define filter condition");
			}
			return qs;
		}
		inputsGRS = this.getCurRow();
		NounMetadata filterNoun = inputsGRS.getNoun(2);
		// filter is query struct pksl type
		// the qs is the value of the filterNoun
		SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
		if (qs == null) {
			throw new IllegalArgumentException("Need to define filter condition");
		}
		return qs;
	}
}
