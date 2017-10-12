package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class ReplaceColumnValueReactor extends AbstractRFrameReactor{
	
	/**
	 * This reactor updates row values to a new value based on the existing value
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the old value
	 * 3) the new value
	 */

	@Override
	public NounMetadata execute() {
		//initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();

		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//first input is the column that we are updating
			NounMetadata input1 = inputsGRS.getNoun(0);
			String fullColumn = input1.getValue() + "";
			String column = fullColumn.split("__")[1];
		

			//second input is the old value
			NounMetadata input2 = inputsGRS.getNoun(1);
			String oldValue = null; 
			oldValue = input2.getValue() + ""; 

			//third input is the new value
			NounMetadata input3 = inputsGRS.getNoun(2);
			String newValue = null; 
			newValue = input3.getValue() + ""; 

			String table = frame.getTableName();
			//use method to retrieve a single column type
			String colDataType = getColumnType(table, column);
			//account for quotes that will be needed in the query with string values
			String neededQuote = "";
			if (colDataType.equalsIgnoreCase("string") || colDataType.equalsIgnoreCase("character")) {
				neededQuote = "\"";
			} else if (colDataType.equalsIgnoreCase("factor")) {
				//TODO
//				changeColumnType(frame, table, column, "string", "%Y/%m/%d");
//				neededQuote = "\"";	
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
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
