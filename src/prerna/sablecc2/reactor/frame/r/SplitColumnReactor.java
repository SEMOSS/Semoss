package prerna.sablecc2.reactor.frame.r;

import java.util.ArrayList;
import java.util.List;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class SplitColumnReactor extends AbstractRFrameReactor {

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

		// use init to initialize rJavaTranslator object that will be used later
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get table name
		String table = frame.getName();
		
		// make a temporary table name
		// we will reassign the table to this variable
		// then assign back to the original table name
		String tempName = Utility.getRandomString(8);
		// script to change the name of the table back to the original name - will be used later
		String frameReplaceScript = table + " <- " + tempName + ";";
		// false columnReplaceScript indicates that we will not drop the
		// original column of data
		String columnReplaceScript = "FALSE";
		String direction = "wide";

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

			// build the script to execute
			String script = tempName + " <- cSplit(" + table + ", " + "\"" + column + "\", \"" + separator
					+ "\", direction = \"" + direction + "\", drop = " + columnReplaceScript;
			if (isRegex) {
				script += ", fixed = FALSE";
			} else {
				script += ", fixed = TRUE";
			}
			script += ");";

			// evaluate the r script
			frame.executeRScript(script);

			// get all the columns that are factors
			script = "sapply(" + tempName + ", is.factor);";
			// keep track of which columns are factors
			int[] factors = this.rJavaTranslator.getIntArray(script);
			String[] colNames = getColumns(tempName);

			// now I need to compose a string based on it
			// we will convert the columns that are factors into strings using
			// as.character
			String conversionString = "";
			for (int factorIndex = 0; factorIndex < factors.length; factorIndex++) {
				if (factors[factorIndex] == 1) // this is a factor
				{
					conversionString = conversionString + tempName + "$" + colNames[factorIndex] + " <- "
							+ "as.character(" + tempName + "$" + colNames[factorIndex] + ");";
				}
			}

			// convert factors
			frame.executeRScript(conversionString);
			// change table back to original name
			frame.executeRScript(frameReplaceScript);
			// perform variable cleanup
			frame.executeRScript("rm(" + tempName + "); gc();");
		}

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"SplitColumn", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		// column header data is changing so we must recreate metadata
		recreateMetadata(table);
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
