package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class UnpivotReactor extends AbstractRFrameReactor {

	/**
	 * This reactor unpivots columns so that the columns selected will
	 * be removed and combined to generate 2 new columns "variable" and "value"
	 * "variable" - original column headers
	 * "value" - value for original column header
	 * The inputs to the reactor are: 
	 * 1) the columns to unpivot
	 */
	
	public UnpivotReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		//initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		//get frame name
		String table = frame.getTableName();
		
		//get column inputs in an array
		String[] columns = getStringArray();
		
		// makes the columns and converts them into rows
		// melt(dat, id.vars = "FactorB", measure.vars = c("Group1", "Group2"))
		String concatString = "";
		String tempName = Utility.getRandomString(8);
		int numColsToUnPivot = columns.length;
		if(numColsToUnPivot > 0) {
			concatString = ", measure.vars = c(";
			for (int colIndex = 0; colIndex < numColsToUnPivot; colIndex++) {
				concatString = concatString + "\"" + columns[colIndex] + "\"";
				if (colIndex + 1 < numColsToUnPivot)
					concatString = concatString + ", ";
			}
			concatString = concatString + ")";
		}
		
		// we want to make sure the new columns that we add
		// are in fact unique
		// so we will loop through and ensure that
		final String defaultVarName = "variable";
		final String defaultValueName = "value";
		int headerNum = 1;
		String[] allColumns = frame.getColumnHeaders();
		String varName = defaultVarName;
		String valueName = defaultValueName;
		while(ArrayUtilityMethods.arrayContainsValueIgnoreCase(allColumns, varName) 
				|| ArrayUtilityMethods.arrayContainsValueIgnoreCase(allColumns, valueName)) {
			headerNum++;
			varName = defaultVarName + "_" + headerNum;
			valueName = defaultValueName + "_" + headerNum;
		}
		
		// now that we have unique values
		// we can proceed with the script
		String script = tempName + "<- melt(" + table + ", variable.name = \"" + varName + "\", value.name = \"" + valueName + "\"," + concatString + ");";

		// run the first script to unpivot into the temp frame
		frame.executeRScript(script);
		// if we are to replace the existing frame
		script = table + " <- " + tempName;
		frame.executeRScript(script);
		recreateMetadata(table);
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getColumn(int i) {
		NounMetadata input = this.getCurRow().getNoun(i);
		String column = input.getValue() + "";
		return column;
	}
	
	private String[] getStringArray() {
		GenRowStruct inputsGRS = this.getCurRow();
		String[] columns = new String[inputsGRS.size()];
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//input is the columns to unpivot
			for (int i = 0; i < inputsGRS.size(); i++) {
				String column = getColumn(i);
				//clean column
				if (column.contains("__")) {
					column = column.split("__")[1];
					//add the columns to keep to the string array
				}
				columns[i] = column;
			}
			return columns;
		}
		throw new IllegalArgumentException("Need to define columns to unpivot");
	}
}
