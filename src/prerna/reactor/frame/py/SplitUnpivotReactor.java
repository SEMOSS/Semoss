package prerna.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class SplitUnpivotReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor splits columns based on a separator The split values will be
	 * combined into a single column The inputs to the reactor are: 1) the
	 * columns to split "columns" 2) the delimiters "delimiters"
	 */

	public SplitUnpivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(),
				ReactorKeysEnum.DELIMITER.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get table name
		String wrapperFrameName = frame.getWrapperName();

		// get the columns
		// already cleaned to exclude the frame name
		List<String> columns = getColumns();

		// get the delimiters
		List<String> delimiters = getDelimiters();

		// throw an error if the number of delimiters doesn't make sense
		// delimiters must match the number of columns, or just use a single
		// delimiter
		if ((columns.size() != delimiters.size()) && delimiters.size() != 1) {
			throw new IllegalArgumentException(
					"Need to enter a single delimiter for all columns or one for each column");
		}

		for (int i = 0; i < columns.size(); i++) {
			String column = columns.get(i);
			String delimiter = "";
			if (delimiters.size() == 1) {
				delimiter = delimiters.get(0);
			} else {
				delimiter = delimiters.get(i);
			}

			// split_unpivot(column, delimiter)
			// build the script to execute
			String script = wrapperFrameName + ".split_unpivot('" + column + "', '" + delimiter + "')";
			frame.runScript(script);
			this.addExecutedCode(script);
		}
		// update the frame reference as well since these changes modify the object
		String script = frame.getName() + " = " + wrapperFrameName + ".cache['data']";
		frame.runScript(script);
		// TODO : should this be recorded
		this.addExecutedCode(script);
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight,
				frame,
				"SplitUnpivot",
				AnalyticsTrackerHelper
						.getHashInputs(this.store, this.keysToGet));

		// column header data is changing so we must recreate metadata
		recreateMetadata(frame, false);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	// ////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////
	// /////////////////////// GET PIXEL INPUT ////////////////////////////
	// ////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////

	private List<String> getDelimiters() {
		// inputs are passed based on a key
		// store in a vector of inputs
		List<String> delInputs = new Vector<String>();
		GenRowStruct delGRS = this.store.getNoun(keysToGet[1]);
		if (delGRS != null) {
			int size = delGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					delInputs.add(delGRS.get(i).toString());
				}
				return delInputs;
			}
		}
		throw new IllegalArgumentException("Need to define delimiters");
	}

	private List<String> getColumns() {
		// if it was passed based on a key
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getNoun(keysToGet[0]);
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					// get each individul column entry and clean
					String column = colGRS.get(i).toString();
					if (column.contains("__")) {
						column = column.split("__")[1];
					}
					colInputs.add(column);
				}
				return colInputs;
			}
		}
		throw new IllegalArgumentException("Need to define columns");
	}

}
