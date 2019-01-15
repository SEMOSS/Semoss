package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class UpdateRowValuesWhereColumnContainsValueReactor extends AbstractRFrameReactor {

	/**
	 * This reactor updates row values to a new value based on a filter condition
	 * (where a column equals a specified value)
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the new value
	 * 3) the filter condition
	 */
	
	public UpdateRowValuesWhereColumnContainsValueReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get frame name
		String table = frame.getName();

		// get inputs
		String updateCol = getUpdateColumn();
		// separate the column name from the frame name
		if (updateCol.contains("__")) {
			updateCol = updateCol.split("__")[1];
		}

		// second noun will be the value to update (the new value)
		String value = getNewValue();

		// get data type of column being updated
		String updateDataType = getColumnType(table, updateCol);

		// account for quotes around the new value if needed
		if (updateDataType.contains("character") || updateDataType.contains("string")
				|| updateDataType.contains("factor")) {
			value = "\"" + value + "\"";
		}

		////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////
		///////////////////// QUERY FILTER//////////////////////////////////
		///////////////////////////////////////////////////////////////////
		
		// the third noun will be a filter; we can get the qs from this
		SelectQueryStruct qs = getQueryStruct();
		// get all of the filters from this querystruct
		GenRowFilters grf = qs.getExplicitFilters();

		// use RInterpreter to create filter syntax
		StringBuilder rFilterBuilder = new StringBuilder();
		RInterpreter ri = new RInterpreter();
		ri.setColDataTypes(frame.getMetaData().getHeaderToTypeMap());
		ri.addFilters(grf.getFilters(), table, rFilterBuilder, true);

		// execute the r scripts
		if (rFilterBuilder.length() > 0) {
			String script = table + "$" + updateCol + "[" + rFilterBuilder.toString() + "] <- " + value + ";";
			this.rJavaTranslator.runR(script);
		}
		
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
