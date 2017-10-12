package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class RegexReplaceColumnValueReactor extends AbstractRFrameReactor {

	/**
	 * This reactor updates row values based on a regex
	 * It replaces all portions of the current cell value that is an exact match to the input value
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the regex to look for
	 * 3) value to replace the regex with 
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
			PixelDataType nounType1 = input1.getNounType();
			String column = "";
			String fullColumn = "";
			if (nounType1 == PixelDataType.COLUMN) {
				fullColumn = input1.getValue() + "";
				column = fullColumn.split("__")[1];
			}

			//second input is the regex
			NounMetadata input2 = inputsGRS.getNoun(1);
			String regex = null; 
			regex = input2.getValue() + ""; 

			//third input is the new value
			NounMetadata input3 = inputsGRS.getNoun(2);
			String newValue = null; 
			newValue = input3.getValue() + ""; 
			String table = frame.getTableName();
			String colScript = table + "$" + column;
			String script = colScript + " = ";
			String dataType = getColumnType(table, column);
			String quote = "";
			if (dataType.contains("character") || dataType.contains("factor")) {
				quote = "\"";
			}
			//script is of the form FRAME$Genre = gsub("-","M", FRAME$Genre)
			script += "gsub(" + quote + regex + quote + "," + quote + newValue + quote + ", " + colScript + ")";
			frame.executeRScript(script);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
