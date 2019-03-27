package prerna.sablecc2.reactor.frame.py;

import prerna.ds.py.PandasFrame;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class DropRowsReactor extends AbstractFrameReactor {

	/**
	 * This reactor drops rows based on a comparison The inputs to the reactor
	 * are: 1) the filter comparison for dropping rows
	 */

	public DropRowsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String table = frame.getName();
		

		// the first noun will be a query struct - the filter
		SelectQueryStruct qs = getQueryStruct();
		// get the filters from the query struct
		// and iterate through each filtered column
		GenRowFilters grf = qs.getExplicitFilters();

		// use RInterpreter to create filter syntax
		StringBuilder pyFilterBuilder = new StringBuilder();
		PandasInterpreter pi = new PandasInterpreter();
		pi.setDataTableName(table + ".cache['data']");
		pi.setDataTypeMap(frame.getMetaData().getHeaderToTypeMap());
		pi.addFilters(grf.getFilters(), table, pyFilterBuilder, true);

		// execute the r script
		// FRAME <- FRAME[!( FRAME$Director == "value"),]
		String newScript = table + ".cache['data'] =  " + table + ".cache['data'][~" + pyFilterBuilder.toString() + "]";
		frame.runScript(newScript);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "DropRows",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private SelectQueryStruct getQueryStruct() {
		GenRowStruct inputsGRS = this.store.getNoun(this.keysToGet[0]);
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
		NounMetadata filterNoun = inputsGRS.getNoun(0);
		// filter is query struct pksl type
		// the qs is the value of the filterNoun
		SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
		if (qs == null) {
			throw new IllegalArgumentException("Need to define filter condition");
		}
		return qs;
	}

}
