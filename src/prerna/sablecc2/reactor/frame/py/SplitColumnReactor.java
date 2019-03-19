package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class SplitColumnReactor extends AbstractFramePyReactor {

	/**
	 * This reactor splits columns based on a separator
	 * It replaces all portions of the current cell value that is an exact match to the input value
	 * The inputs to the reactor are: 
	 * 1) the separator
	 * 2) the columns to split 
	 */

	private static final String SEARCH_TYPE = "search";
	private static final String REGEX = "Regex";

	public SplitColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.DELIMITER.getKey(), SEARCH_TYPE };
	}
	
	@Override
	public NounMetadata execute() {
		List<String> cols = getColumns();
		String separator = getSeparator();
		boolean isRegex = isRegex();

		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get table name
		String table = frame.getName()+"w";
		
		// get length of input to use when iterating through
		int inputSize = cols.size();

		for (int i = 0; i < inputSize; i++) {
			// next input will be the column that we are splitting
			// we can specify to split more than one column, so there could be
			// multiple column inputs
			String column = cols.get(i);
			// clean column name
			if (column.contains("__")) {
				column = column.split("__")[1];
			}

			// evaluate the r script
			frame.runScript(frame.getName() + " = " + table + ".split('" + column + "', '" + separator + "')");

		}

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"SplitColumn", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		// column header data is changing so we must recreate metadata
		frame = (PandasFrame)recreateMetadata(frame);
		
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getSeparator() {
		GenRowStruct separatorGrs = this.store.getNoun(keysToGet[1]);
		if (separatorGrs == null || separatorGrs.isEmpty()) {
			throw new IllegalArgumentException("Need to define a separator to split the column with");
		}
		String separator = separatorGrs.get(0).toString();
		if (separator.isEmpty()) {
			throw new IllegalArgumentException("Need to define a separator to split the column with");
		}
		return separator;
	}
		
	private boolean isRegex() {
		GenRowStruct regexGrs = this.store.getNoun(SEARCH_TYPE);
		if (regexGrs == null || regexGrs.isEmpty()) {
			return true;
		}
		String val = regexGrs.get(0).toString();
		if (val.equalsIgnoreCase(REGEX)) {
			return true;
		}
		return false;
	}
	
	private List<String> getColumns() {
		List<String> cols = new ArrayList<String>();

		// try its own key
		GenRowStruct colsGrs = this.store.getNoun(keysToGet[0]);
		if (colsGrs != null && !colsGrs.isEmpty()) {
			int size = colsGrs.size();
			for (int i = 0; i < size; i++) {
				cols.add(colsGrs.get(i).toString());
			}
			return cols;
		}

		int inputSize = this.getCurRow().size();
		if (inputSize > 0) {
			for (int i = 0; i < inputSize; i++) {
				cols.add(this.getCurRow().get(i).toString());
			}
			return cols;
		}

		throw new IllegalArgumentException("Need to define the columns to split");
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SEARCH_TYPE)) {
			return "The type of search: Regex or an Exact Match";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
	
	
}
