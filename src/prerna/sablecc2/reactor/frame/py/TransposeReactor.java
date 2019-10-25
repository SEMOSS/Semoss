package prerna.sablecc2.reactor.frame.py;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class TransposeReactor extends AbstractPyFrameReactor {

	@Override
	public NounMetadata execute() {
		PandasFrame frame = (PandasFrame) getFrame();
		// get table name
		String table = frame.getName();
		String transposeScript = table + " = " + table + ".transpose()";
		// python transpose creates numeric columns
		// need to make str type
		String stringHeaderType = table + ".columns = " + table + ".columns.astype(str)";
		// change all datatypes to string
		String stringType = table + " = " + table + ".astype(str)";
		frame.runScript(transposeScript, stringHeaderType, stringType);
		// TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "Transpose", AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		// the column data has changed
		frame = (PandasFrame) recreateMetadata(frame);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

}
