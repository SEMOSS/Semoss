package prerna.sablecc2.reactor.frame.py;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RemoveDuplicateRowsReactor extends AbstractFrameReactor {

	/**
	 * This reactor removes duplicate rows from the data frame
	 * There are no inputs needed
	 */

	@Override
	public NounMetadata execute() {
		
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		
		//get table name
		String table = frame.getName();
		
		//define the r script to be executed
		String script = table + "w.drop_dup()";
		
		//execute the r script
		frame.runScript(script);
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"RemoveDuplicateRows", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
