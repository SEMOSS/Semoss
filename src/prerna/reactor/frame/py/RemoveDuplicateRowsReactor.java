package prerna.reactor.frame.py;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RemoveDuplicateRowsReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor removes duplicate rows from the data frame
	 * There are no inputs needed
	 */

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		
		//get table name
		String wrapperFrameName = frame.getWrapperName();
		
		//define the r script to be executed
		String script = wrapperFrameName + ".drop_dup()";
		
		//execute the r script
		frame.runScript(script);
		this.addExecutedCode(script);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"RemoveDuplicateRows", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
