package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
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
	
	@Override
	public NounMetadata execute() {
		//initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String table = frame.getTableName();
		String[] columns = new String[inputsGRS.size()];
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//input is the columns to unpivot
			for (int i = 0; i < inputsGRS.size(); i++) {
				NounMetadata input = inputsGRS.getNoun(i);
				String fullColumn = input.getValue() + "";
				String column = "";
				if (fullColumn.contains("__")) {
					column = fullColumn.split("__")[1];
					//add the columns to keep to the string array
				}
				columns[i] = column;
			}
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
			
			String script = tempName + "<- melt(" + table + concatString + ");";
			// run the first script to unpivot into the temp frame
			frame.executeRScript(script);
			// if we are to replace the existing frame
			script = table + " <- " + tempName;
			frame.executeRScript(script);
			recreateMetadata(table);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
