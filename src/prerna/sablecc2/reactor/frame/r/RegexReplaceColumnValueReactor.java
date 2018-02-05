package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RegexReplaceColumnValueReactor extends AbstractRFrameReactor {

	/**
	 * This reactor updates row values based on a regex
	 * It replaces all portions of the current cell value that is an exact match to the input value
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the regex to look for
	 * 3) value to replace the regex with 
	 */
	
	public RegexReplaceColumnValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.REGEX.getKey(), ReactorKeysEnum.NEW_VALUE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		//initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		
		//get table name
		String table = frame.getTableName();

		// get column to update
		String column = getUpdateColumn();
		//clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		//get regular expression
		String regex = getRegex();
		
		//get new value
		String newValue = getNewValue();

		//define r script
		String colScript = table + "$" + column;
		String script = colScript + " = ";
		String dataType = getColumnType(table, column);
		String quote = "";
		if (dataType.contains("character") || dataType.contains("factor")) {
			quote = "\"";
		}
		//script is of the form FRAME$Genre = gsub("-","M", FRAME$Genre)
		script += "gsub(" + quote + regex + quote + "," + quote + newValue + quote + ", " + colScript + ");";
			
		//doing gsub on a numeric column changes the data type to a string 
		//so change it back to numeric in r
		if (dataType.contains("numeric")) {
			script += table + "$" + column + " <- as.numeric(" + table + "$" + column + ");";
		}
			
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
			String column = input1.getValue() + "";
			if (column.length() == 0) {
				throw new IllegalArgumentException("Need to defne the column to be updated");
			}
			return column;
		}	
		throw new IllegalArgumentException("Need to define the column to be updated");
	}
	
	private String getRegex() {
		//second input is the regex
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String regex = input2.getValue() + ""; 
		return regex;
	}
	
	private String getNewValue() {
		//third input is the new value
		NounMetadata input3 = this.getCurRow().getNoun(2);
		String newValue = input3.getValue() + ""; 
		return newValue;
	}

}
