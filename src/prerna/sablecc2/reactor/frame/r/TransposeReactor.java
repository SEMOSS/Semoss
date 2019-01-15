package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class TransposeReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		
		/**
		 * This reactor transposes the frame No inputs are needed
		 */

		// get frame
		// use init to initialize rJavaTranslator object that will be used later
		init();
		RDataTable frame = (RDataTable) getFrame();

		// get table name
		String table = frame.getName();

		// define r script string to be executed
		String script = table + " <- " + table + "[, data.table(t(.SD), keep.rownames=TRUE)]";
		// execute the r script
		frame.executeRScript(script);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"Transpose", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// with transpose - the column data is changing so we must recreate metadata
		recreateMetadata(table);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
