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

public class DropRowsReactor extends AbstractRFrameReactor {

	/**
	 * This reactor drops rows based on a comparison The inputs to the reactor
	 * are: 1) the filter comparison for dropping rows
	 */

	public DropRowsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String table = frame.getName();

		// the first noun will be a query struct - the filter
		SelectQueryStruct qs = getQueryStruct();
		// get the filters from the query struct
		// and iterate through each filtered column
		GenRowFilters grf = qs.getExplicitFilters();

		// use RInterpreter to create filter syntax
		StringBuilder rFilterBuilder = new StringBuilder();
		RInterpreter ri = new RInterpreter();
		ri.setColDataTypes(frame.getMetaData().getHeaderToTypeMap());
		ri.addFilters(grf.getFilters(), table, rFilterBuilder, true);

		// execute the r script
		// FRAME <- FRAME[!( FRAME$Director == "value"),]
		String newScript = table + "<- " + table + "[!(" + rFilterBuilder.toString() + "),]";
		frame.executeRScript(newScript);

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
