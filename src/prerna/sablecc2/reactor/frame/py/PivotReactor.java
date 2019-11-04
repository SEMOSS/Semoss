package prerna.sablecc2.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class PivotReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor pivots a column so that the unique values will be transformed into new headers
	 * The inputs to the reactor are: 
	 * 1) the column to pivot
	 * 2) the column to turn into values for the selected pivot column
	 * 3) the aggregate function
	 * 4) the other columns to maintain
	 */

	private static final String PIVOT_COLUMN_KEY = "pivotCol";
	private static final String VALUE_COLUMN_KEY = "valueCol";
	private static final String AGGREGATE_FUNCTION_KEY = "function";

	public PivotReactor() {
		this.keysToGet = new String[] { PIVOT_COLUMN_KEY, VALUE_COLUMN_KEY, ReactorKeysEnum.MAINTAIN_COLUMNS.getKey(),
				AGGREGATE_FUNCTION_KEY };
	}

	/*
	 * What is being sent Pivot(pivotCol = ["Nominated"], valueCol =
	 * ["MovieBudget"], function = [""], maintainCols =
	 * []); Frame()|QueryAll()|AutoTaskOptions(panel=["0"],
	 * layout=["Grid"])|Collect(500); the maintain cols is the keep cols
	 * 
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		// get frame name
		String table = frame.getName();
		// get inputs
		// get the column to pivot
		String pivotCol = getColumnToPivot();
		// separate the column name from the frame name
		if (pivotCol.contains("__")) {
			pivotCol = pivotCol.split("__")[1];
		}

		// get the column to turn into values for the selected pivot column
		// so it cant have many ?

		String valuesCol = getValuesCol();
		// separate the column name from the frame name
		if (valuesCol.contains("__")) {
			valuesCol = valuesCol.split("__")[1];
		}

		// keep track of the columns to keep
		List<String> colsToKeep = getKeepCols();
		// makes the columns and converts them into rows
		// I need columns to keep and columns to pivot

		String keepString = "";
		int numColsToKeep = 0;
		if (colsToKeep != null) {
			numColsToKeep = colsToKeep.size();
		}

		if (numColsToKeep > 0) {
			// with the portion of code to ignore if the user passes in the
			// col to pivot or value to pivot in the selected columns
			// we need to account for this so we dont end the keepString
			// with " + "
			keepString = ", index = [";
			for (int colIndex = 0; colIndex < numColsToKeep; colIndex++) {
				String newKeepString = "'" + colsToKeep.get(colIndex) + "'";
				if (newKeepString.equals(pivotCol) || newKeepString.equals(valuesCol)) {
					continue;
				}
				if (colIndex == 0)
					keepString = keepString + newKeepString;
				else
					keepString = keepString + ", " + newKeepString;
			}
			keepString = keepString + "]";
		}

		String pivotString = ", columns  = ['" + pivotCol + "']";
		String newFrame = Utility.getRandomString(5);
		boolean canAgg = (boolean) frame.runScript(frame.getWrapperName() + ".is_numeric('" + valuesCol + "')");
		if (canAgg) {

			// get the aggregate function if it exists; if it does not exist
			// it will be of length zero
			String aggregateFunction = getAggregateFunction();

			// aggregation function
			// need a way to map it .. will get to ti post implementtion of
			// others
			String aggregateString = "";
			if (aggregateFunction != null && aggregateFunction.length() > 0) {
				aggregateString = ", aggfunc = " + aggregateFunction;
			}

			String valueString = ", values=['" + valuesCol + "']";

			// make python pivot script
			// pd.pivot_table(frame, columns = ['col2'], index = ['col'],
			// values=['col3'], aggfunc='function').reset_index()
			String colScript = newFrame + "= pd.pivot_table(" + table + pivotString + keepString + valueString
					+ aggregateString + ").reset_index()";
			frame.runScript(colScript);

			// straighten up the columns
			// whacked out headers when you have columns specified
			// need way to organize them
			if (keepString != null && keepString.length() > 0) {
				colScript = newFrame + ".columns = " + newFrame + ".columns.to_series().str.join('_')";
			}
			frame.runScript(colScript);

			// clean up temp r variables
		} else {

			// if we can't aggregate we will count non numeric values
			frame.runScript(
					newFrame + "= pd.pivot_table(" + table + pivotString + keepString + " , aggfunc='count'" + ")");

			// python allows weird headers
			// need to clean them up!!!
			// need to get old headers
			List<String> headerList = (List<String>) frame.runScript(PandasSyntaxHelper.getColumns(table));
			frame.runScript(newFrame + " = pd.DataFrame(" + newFrame + ".to_records())");
			// headers have the form ('columnName', 'instanceName')
			StringBuilder sb = new StringBuilder();
			// replacing ('columnName', '
			sb.append("[hdr.replace(\"('\", \"\").replace(\")'\", \"\")");
			for (String header : headerList) {
				sb.append(".replace(\"" + header + "', '\", \"\")");
			}
			// replacing ')
			sb.append(".replace(\"')\",\"\") for hdr in " + newFrame + ".columns]");
			frame.runScript(newFrame + ".columns = " + sb.toString());
			// remove duplicate header names
			frame.runScript(PandasSyntaxHelper.removeDuplicateColumns(newFrame, newFrame));

		}

		// assign it back
		String colScript = table + " = " + newFrame;
		frame.runScript(colScript);

		recreateMetadata(frame);
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "Pivot",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	// get column to pivot based on key "PIVOT_COLUMN_KEY"
	private String getColumnToPivot() {
		GenRowStruct pivotColInput = this.store.getNoun(PIVOT_COLUMN_KEY);
		if (pivotColInput != null) {
			String pivotCol = pivotColInput.getNoun(0).getValue().toString();
			return pivotCol;
		}
		throw new IllegalArgumentException("Need to define column to pivot");
	}

	// get column to turn into values based on key "VALUE_COLUMN_KEY"
	private String getValuesCol() {
		GenRowStruct valueColInput = this.store.getNoun(VALUE_COLUMN_KEY);
		if (valueColInput != null) {
			String valueCol = valueColInput.getNoun(0).getValue().toString();
			return valueCol;
		}
		throw new IllegalArgumentException("Need to define column to turn into values for the selected pivot column");
	}

	// get any additional columns to keep based on the key
	// "MAINTAIN_COLUMNS_KEY"
	private List<String> getKeepCols() {
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getNoun(ReactorKeysEnum.MAINTAIN_COLUMNS.getKey());
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					// get each individual column entry and clean
					String column = colGRS.get(i).toString();
					if (column.contains("__")) {
						column = column.split("__")[1];
					}
					colInputs.add(column);
				}
				return colInputs;
			}
		}
		return null;
	}

	// aggregate function is optional, uses key "AGGREGATE_FUNCTION_KEY"
	private String getAggregateFunction() {
		GenRowStruct functionInput = this.store.getNoun(AGGREGATE_FUNCTION_KEY);

		// need some way to change this to py specific if it is not the same
		if (functionInput != null) {
			String function = functionInput.getNoun(0).getValue().toString();
			return function;
		}
		// don't throw an error because this input is optional
		return "";
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PIVOT_COLUMN_KEY)) {
			return "The column to pivot on";
		} else if (key.equals(VALUE_COLUMN_KEY)) {
			return "The column to turn into values for the selected pivot column";
		} else if (key.equals(AGGREGATE_FUNCTION_KEY)) {
			return "The function used to aggregate columns";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
