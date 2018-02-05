package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ReplaceColumnValueReactor extends AbstractRFrameReactor{
	
	/**
	 * This reactor updates row values to a new value based on the existing value
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the old value
	 * 3) the new value
	 */
	
	public ReplaceColumnValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.NEW_VALUE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		//initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		
		//get table name
		String table = frame.getTableName();

		// get inputs
		//first input is the column that we are updating
		String column = getUpdateColumn();
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		//second input is the old value
		String oldValue = getOldValue();

		//third input is the new value
		String newValue = getNewValue();

		//use method to retrieve a single column type
		String colDataType = getColumnType(table, column);
		
		//account for quotes that will be needed in the query with string values
		String neededQuote = "";
		if (colDataType.equalsIgnoreCase("string") || colDataType.equalsIgnoreCase("character")) {
			neededQuote = "\"";
		} else if (colDataType.equalsIgnoreCase("factor")) {
			changeColumnType(frame, table, column, "string", "%Y/%m/%d");
			neededQuote = "\"";	
		}
			
		String script = "";
		if (oldValue.equalsIgnoreCase("null") || oldValue.equalsIgnoreCase("NA")) {
			script = table + "$" + column + "[is.null(" + table + "$" + column + ")] <- " + neededQuote
					+ newValue + neededQuote;
		} 
		else {
			script = table + "$" + column + "[" + table + "$" + column + " == " + neededQuote + oldValue
					+ neededQuote + "] <- " + neededQuote + newValue + neededQuote;
		}
			
		//execute the r script
		//script is of the form FRAME$Director[FRAME$Director == "oldVal"] <- "newVal"
		frame.executeRScript(script);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getUpdateColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//first input is the column that we are updating
			NounMetadata input1 = inputsGRS.getNoun(0);
			String fullColumn = input1.getValue() + "";
			if (fullColumn.length() == 0) {
				throw new IllegalArgumentException ("Need to define column to update");
			}
			return fullColumn;
		}
		throw new IllegalArgumentException("Need to define column to update");
	}
	
	private String getOldValue() {
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String oldValue = input2.getValue() + ""; 
		if (oldValue.length() == 0) {
			throw new IllegalArgumentException("Need to define old value to replace");
		}
		return oldValue;
	}
	
	private String getNewValue() {
		NounMetadata input3 =this.getCurRow().getNoun(2);
		String newValue = input3.getValue() + ""; 
		return newValue;
	}
	
}
