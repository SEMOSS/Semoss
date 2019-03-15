package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ReplaceColumnValueReactor extends AbstractFrameReactor{
	
	/**
	 * This reactor updates row values to a new value based on the existing value
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the old value
	 * 3) the new value
	 */
	
	public ReplaceColumnValueReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.NEW_VALUE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get wrapper name
		String table = frame.getName() + "w";

		// get inputs
		// first input is the column that we are updating
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		// second input is the old value
		String oldValue = this.keyValue.get(this.keysToGet[1]);

		// third input is the new value
		String newValue = this.keyValue.get(this.keysToGet[2]);
		
		
		String neededQuote = (boolean)frame.runScript(table + ".is_numeric('" + column + "')")?"":"'";
		

		String script = "";
		if (oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("NA")) {
			frame.runScript(frame.getName() + ".fillna({'" + column + "':" + neededQuote + newValue + neededQuote + "})");
		} else {
			frame.runScript(table + ".replace_val('" + column + "'," + neededQuote + oldValue + neededQuote + " , " + neededQuote + newValue + neededQuote + ")");
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ReplaceColumnValue", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
			
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
}
