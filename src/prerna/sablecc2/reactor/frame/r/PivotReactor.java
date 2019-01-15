package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class PivotReactor extends AbstractRFrameReactor {

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
		this.keysToGet = new String[] { PIVOT_COLUMN_KEY, VALUE_COLUMN_KEY, ReactorKeysEnum.MAINTAIN_COLUMNS.getKey(), AGGREGATE_FUNCTION_KEY };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// initialize the rJavaTranslator
		init();

		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get frame name
		String table = frame.getName();
		// get inputs
		// get the column to pivot
		String pivotCol = getColumnToPivot();
		// separate the column name from the frame name
		if (pivotCol.contains("__")) {
			pivotCol = pivotCol.split("__")[1];
		}
		
		//get the column to turn into values for the selected pivot column
		String valuesCol = getValuesCol(); 
		//separate the column name from the frame name
		if (valuesCol.contains("__")) {
			valuesCol = valuesCol.split("__")[1];
		} 
			
		// keep track of the columns to keep
		List<String> colsToKeep = getKeepCols();
		// get the aggregate function if it exists; if it does not exist
		// it will be of length zero
		String aggregateFunction = getAggregateFunction();
		// makes the columns and converts them into rows
		// dcast(molten, formula = subject~ variable)
		// I need columns to keep and columns to pivot
		String newFrame = Utility.getRandomString(8);
		String keepString = "";
		int numColsToKeep = 0;
		if (colsToKeep != null) {
			numColsToKeep = colsToKeep.size();
		}
		boolean dropColumn = false;
		if (numColsToKeep > 0) {
			// with the portion of code to ignore if the user passes in the 
			// col to pivot or value to pivot in the selected columns
			// we need to account for this so we dont end the keepString with " + "
			keepString = ", formula = ";
			for (int colIndex = 0; colIndex < numColsToKeep; colIndex++) {
				String newKeepString = colsToKeep.get(colIndex);
				if(newKeepString.equals(pivotCol) || newKeepString.equals(valuesCol)) {
					continue;
				}
				keepString = keepString + newKeepString;
				if (colIndex + 1 < numColsToKeep) {
					keepString = keepString + " + ";
				}
			}

			// with the portion of code to ignore if the user passes in the 
			// col to pivot or value to pivot in the selected columns
			// we need to account for this so we dont end the keepString with " + "
			if(keepString.endsWith(" + ")) {
				keepString = keepString.substring(0, keepString.length() - 3);
			}
			keepString = keepString + " ~ " + pivotCol + ", value.var=\"" + valuesCol + "\"";
		} else {
			// this creates a new column .
			dropColumn = true;
			keepString = ", formula = .~" + pivotCol + ", value.var=\"" + valuesCol + "\""; ;
		}

		String aggregateString = "";
		if (aggregateFunction != null && aggregateFunction.length() > 0) {
			// check data type of values col 
			SemossDataType dataType = frame.getMetaData().getHeaderTypeAsEnum(table + "__" + valuesCol);
			if (!(dataType == SemossDataType.INT || dataType == SemossDataType.DOUBLE)) {
				NounMetadata noun = new NounMetadata("Unable to aggregate on non-numeric column :" + valuesCol, PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException exception = new SemossPixelException(noun);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			aggregateString = ", fun.aggregate = " + aggregateFunction + " , na.rm = TRUE";
		}
		
		//we need to make sure that the column we are pivoting on does not have values with dashes
        //we should replace any dashes with underscores
        String colScript = table + "$" + pivotCol;
        String cleanScript = colScript + "= gsub(" + "\"-\"" + "," + "\"_\"" + ", " + colScript + ");";
        this.rJavaTranslator.executeR(cleanScript);

		String script = newFrame + " <- dcast(" + table + keepString + aggregateString + ");";
		script += RSyntaxHelper.asDataTable(newFrame, newFrame);
		script += table + " <- " + newFrame + ";";
		if(dropColumn) {
			// drop the . column
			script += table + " <- " + table + "[,.:=NULL];";
		}
		this.rJavaTranslator.runR(script);
		recreateMetadata(table);
		
		//clean up temp r variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + newFrame + ");");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"Pivot", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
		
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	//get column to pivot based on key "PIVOT_COLUMN_KEY"
	private String getColumnToPivot() {
		GenRowStruct pivotColInput = this.store.getNoun(PIVOT_COLUMN_KEY);
		if (pivotColInput != null) {
			String pivotCol = pivotColInput.getNoun(0).getValue().toString();
			return pivotCol;
		}
		throw new IllegalArgumentException("Need to define column to pivot");
	}
	
	//get column to turn into values based on key "VALUE_COLUMN_KEY"
	private String getValuesCol() {
		GenRowStruct valueColInput = this.store.getNoun(VALUE_COLUMN_KEY);
		if (valueColInput != null) {
			String valueCol = valueColInput.getNoun(0).getValue().toString();
			return valueCol;
		}
		throw new IllegalArgumentException("Need to define column to turn into values for the selected pivot column");
	}
	
	//get any additional columns to keep based on the key "MAINTAIN_COLUMNS_KEY"
	private List<String> getKeepCols() {
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getNoun(ReactorKeysEnum.MAINTAIN_COLUMNS.getKey());
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					//get each individual column entry and clean 
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
	
	//aggregate function is optional, uses key "AGGREGATE_FUNCTION_KEY"
	private String getAggregateFunction() {
		GenRowStruct functionInput = this.store.getNoun(AGGREGATE_FUNCTION_KEY);
		if (functionInput != null) {
			String function = functionInput.getNoun(0).getValue().toString();
			return function;
		}
		//don't throw an error because this input is optional
		return "";
	}
	
	///////////////////////// KEYS /////////////////////////////////////
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(PIVOT_COLUMN_KEY)) {
			return "The column to pivot on";
		} else if(key.equals(VALUE_COLUMN_KEY)) {
			return "The column to turn into values for the selected pivot column";
		} else if (key.equals(AGGREGATE_FUNCTION_KEY)) {
			return "The function used to aggregate columns";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
