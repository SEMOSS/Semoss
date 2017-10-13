package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Utility;

public class PivotReactor extends AbstractRFrameReactor{

	/**
	 * This reactor pivots a column so that the unique values will be transformed into new headers
	 * The inputs to the reactor are: 
	 * 1) the column to pivot
	 * 2) the column to turn into values for the selected pivot column
	 * 3) the other columns to maintain
	 * 4) the aggregate function
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
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			String pivotCol = ""; 
			//first noun will be the column to pivot
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullPivotCol = noun1.getValue() + ""; 
			//separate the column name from the frame name
			if (fullPivotCol.contains("__")) {
				pivotCol = fullPivotCol.split("__")[1];
			} else {
				pivotCol = fullPivotCol; 
			}

			//second noun will be the column to turn into values for the selected pivot column
			String valuesCol = ""; 
			NounMetadata noun2 = inputsGRS.getNoun(1);
			String fullValuesCol = noun2.getValue() + ""; 
			//separate the column name from the frame name
			if (fullValuesCol.contains("__")) {
				valuesCol = fullValuesCol.split("__")[1];
			} else {
				valuesCol = fullValuesCol; 
			}
			
			//keep track of the columns to keep
			//use minus three to account for the other inputs
			String[] colsToKeep = new String[inputsGRS.size() - 3];
			
			//third input will be the columns to maintain in the new table
			//start at 2 for indexing becuase we have already taken 2 inputs into account
			for (int i = 2; i < inputsGRS.size() - 1; i++) {
				NounMetadata input = inputsGRS.getNoun(i);
				String fullKeepCol = input.getValue() + "";
				String keepCol = "";
				if (fullKeepCol.contains("__")) {
					keepCol = fullKeepCol.split("__")[1];
					//add the columns to keep to the string array
				}
				colsToKeep[i - 2] = keepCol;
			}
			
			//fourth input is the aggregate function
			NounMetadata noun4 = inputsGRS.getNoun(3);
			String aggregateFunction = noun4.getValue() + ""; 
			
			
			// makes the columns and converts them into rows
			// dcast(molten, formula = subject~ variable)
			// I need columns to keep and columns to pivot
			String newFrame = Utility.getRandomString(8);

			String keepString = "";
			int numColsToKeep = colsToKeep.length;
			if (numColsToKeep > 0) {
				// with the portion of code to ignore if the user passes in the 
				// col to pivot or value to pivot in the selected columns
				// we need to account for this so we dont end the keepString with " + "
				keepString = ", formula = ";
				for (int colIndex = 0; colIndex < numColsToKeep; colIndex++) {
					String newKeepString = colsToKeep[colIndex];
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
			}

			String aggregateString = "";
			if (aggregateFunction != null && aggregateFunction.length() > 0) {
				aggregateString = ", fun.aggregate = " + aggregateFunction + " , na.rm = TRUE";
			}
			String script = newFrame + " <- dcast(" + table + keepString + aggregateString + ");";
			frame.executeRScript(script);
			script = newFrame + " <- as.data.table(" + newFrame + ");";
			frame.executeRScript(script);
			script = table + " <- " + newFrame;
			frame.executeRScript(script);
			recreateMetadata(table);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
