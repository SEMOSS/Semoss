package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class SplitUnpivotReactor extends AbstractRFrameReactor {

	/**
	 * This reactor splits columns based on a separator
	 * The split values will be combined into a single column
	 * The inputs to the reactor are: 
	 * 1) the columns to split "columns"
	 * 2) the delimiters "delimiters" 
	 */
	
	public SplitUnpivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.DELIMITER.getKey() };
	}

	@Override
	public NounMetadata execute() {
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
		// script to change the name of the table back to the original name -
		// will be used later
		String frameReplaceScript = table + " <- " + tempName + ";";
		// false columnReplaceScript indicates that we will not drop the
		// original column of data
		String columnReplaceScript = "FALSE";
		String direction = "long";

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

			// build the script to execute
			String script = tempName + " <- cSplit(" + table + ", " + "\"" + column + "\", \"" + delimiter
					+ "\", direction = \"" + direction + "\", drop = " + columnReplaceScript + ");";

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
				"SplitUnpivot", 
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
