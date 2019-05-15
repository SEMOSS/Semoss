package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class PivotReactor extends AbstractFrameReactor {

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
	
	
	/*
	 * What is being sent
	 * Pivot(pivotCol = ["Nominated"], valueCol = ["MovieBudget"], function = [""], maintainCols = []);Frame()|QueryAll()|AutoTaskOptions(panel=["0"], layout=["Grid"])|Collect(500);
	 * the maintain cols is th ekeep cols
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
		
		//get the column to turn into values for the selected pivot column
		// so it cant have many ?
		
		String valuesCol = getValuesCol(); 
		
		boolean canAgg = (boolean)frame.runScript(table + "w.is_numeric('" + valuesCol + "')");
		
		if(canAgg)
		{
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
			
			if (numColsToKeep > 0) 
			{
				// with the portion of code to ignore if the user passes in the 
				// col to pivot or value to pivot in the selected columns
				// we need to account for this so we dont end the keepString with " + "
				keepString = ", columns = [";
				for (int colIndex = 0; colIndex < numColsToKeep; colIndex++) {
					String newKeepString = "'" + colsToKeep.get(colIndex) + "'";
					if(newKeepString.equals(pivotCol) || newKeepString.equals(valuesCol)) {
						continue;
					}
					if(colIndex == 0)
						keepString = keepString + newKeepString;
					else
						keepString = keepString + ", " + newKeepString;
				}
				keepString = keepString + "]";
			} 
	
			// aggregation function
			// need a way to map it .. will get to ti post implementtion of others
			String aggregateString = "";
			if(aggregateFunction != null && aggregateFunction.length() > 0)
				aggregateString = ", aggfunc = " + aggregateFunction;
			
			String pivotString = ", index = ['" + pivotCol + "']";
			
			String valueString = ", values=['" + valuesCol + "']";
			
			//we need to make sure that the column we are pivoting on does not have values with dashes
	        //we should replace any dashes with underscores
			String randomVar = Utility.getRandomString(5);
	        String colScript = randomVar + "= pd.pivot_table("+ table + pivotString + keepString + valueString + aggregateString + ").reset_index()"; //table + "" + pivotCol;
	        frame.runScript(colScript);
	        
	        // straighten up the columns
	        // whacked out headers when you have columns specified
	        // need way to organize them
	        if(keepString != null && keepString.length() > 0)
	        	colScript = randomVar + ".columns = " + randomVar + ".columns.to_series().str.join('_')";
	        frame.runScript(colScript);
	        
	        // assign it back
	        colScript = table + " = " + randomVar;
	        frame.runScript(colScript);

	        
			recreateMetadata(frame);
			
			//clean up temp r variables
		}
		else
		{
			NounMetadata noun = new NounMetadata("Unable to aggregate on non-numeric column :" + valuesCol, PixelDataType.CONST_STRING,
					PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
			
		}
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"Pivot", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
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
		
		// need some way to change this to py specific if it is not the same
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
	
	//////////////////////// Recreation methods //////////////////////////////
	
	
	
	private void recreateMetadata(PandasFrame frame)
	{
		
		
		String frameName = frame.getName();

		String[] colNames = getColumns(frame);
		
		// I bet this is being done for pixel.. I will keep the same
		frame.runScript(PandasSyntaxHelper.cleanFrameHeaders(frameName, colNames));
		colNames = getColumns(frame);
		
		String[] colTypes = getColumnTypes(frame);

		if(colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + frameName + " exists and can be a valid data.table object");
		}
		
		// create the pandas frame
		// and set up teverything else
		

		ImportUtility.parseTableColumnsAndTypesToFlatTable(frame.getMetaData(), colNames, colTypes, frameName);

	}

	
	public String [] getColumns(PandasFrame frame)
	{
		
		String frameName = frame.getName();
		// get jep thread and get the names
		ArrayList <String> val = (ArrayList<String>)frame.runScript("list(" + frameName + ")");
		String [] retString = new String[val.size()];
		
		val.toArray(retString);
		
		return retString;
		
		
	}
	
	public String [] getColumnTypes(PandasFrame frame)
	{
		String frameName = frame.getName();
		
		// get jep thread and get the names
		ArrayList <String> val = (ArrayList<String>)frame.runScript(PandasSyntaxHelper.getTypes(frameName));
		String [] retString = new String[val.size()];
		
		val.toArray(retString);
		
		return retString;
	}



}
