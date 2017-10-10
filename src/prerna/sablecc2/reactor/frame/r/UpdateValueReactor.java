package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class UpdateValueReactor extends AbstractRFrameReactor{

	@Override
	public NounMetadata execute() {
		//initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();

		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//first input is the column that we are updating
			NounMetadata input1 = inputsGRS.getNoun(0);
			PixelDataType nounType1 = input1.getNounType();
			String column = "";
			String fullColumn = "";
			if (nounType1 == PixelDataType.COLUMN) {
				fullColumn = input1.getValue() + "";
				column = fullColumn.split("__")[1];
			}

			//second input is the old value
			NounMetadata input2 = inputsGRS.getNoun(1);
			String oldValue = null; 
			oldValue = input2.getValue() + ""; 

			//third input is the new value
			NounMetadata input3 = inputsGRS.getNoun(2);
			String newValue = null; 
			newValue = input3.getValue() + ""; 

			//check that the frame is not null
			if (frame != null) {
				String table = frame.getTableName();
				//use method to retrieve a single column type
				String colDataType = getColumnType(table, column);
				//account for quotes that will be needed in the query with string values
				String neededQuote = "";
				if (colDataType.equalsIgnoreCase("string") || colDataType.equalsIgnoreCase("character")) {
					neededQuote = "\"";
				}
				//define the r script to be executed
				String script = table + "$" + column + "[" + table + "$" + column + " == "
						+ neededQuote + oldValue + neededQuote + "] <- " + neededQuote
						+ newValue + neededQuote;
				//execute the r script
				//script is of the form: FRAME$Director[FRAME$Director == "oldVal"] <- "newVal"
				frame.executeRScript(script);
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
