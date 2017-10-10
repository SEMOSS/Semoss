package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Utility;

public class ChangeColumnTypeReactor extends AbstractRFrameReactor {
	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		//column is just the name of the column
		String column = "";
		String newType = ""; 

		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//first input is the column that we are changing the type of
			NounMetadata input1 = inputsGRS.getNoun(0);
			PixelDataType nounType1 = input1.getNounType();
			if (nounType1 == PixelDataType.COLUMN) {
				String fullColName = input1.getValue() + "";
				column = fullColName.split("__")[1];
			}
			//second input is the new column type
			NounMetadata input2 = inputsGRS.getNoun(1);
			PixelDataType nounType2 = input2.getNounType();
			if (nounType2 == PixelDataType.CONST_STRING) {
				newType = input2.getValue() + "";
			}
			//make sure that the frame exists
			if (frame != null) {
				String table = frame.getTableName();
				//define the r script to execute
				//script depends on the new data type
				String script = null;
				if (newType.equalsIgnoreCase("string") || newType.equalsIgnoreCase("character")) {
					script = table + " <- " + table + "[, " + column + " := as.character(" + column + ")]";
					frame.executeRScript(script); 
				} else if (newType.equalsIgnoreCase("factor")) {
					script = table + " <- " + table + "[, " + column + " := as.factor(" + column + ")]";
					frame.executeRScript(script);
				} else if (newType.equalsIgnoreCase("number") || newType.equalsIgnoreCase("numeric")) {
					script = table + " <- " + table + "[, " + column + " := as.numeric(" + column + ")]";
					frame.executeRScript(script);
				} else if (newType.equalsIgnoreCase("date")) {
					// we have a different script to run if it is a str to date conversion
					// define date format
					String dateFormat = "%Y/%m/%d";
					//get the column type of the existing column
					String type = getColumnType(table, column);
					String tempTable = Utility.getRandomString(6);
					if (type.equalsIgnoreCase("date")) {
						String formatString = ", format = '" + dateFormat + "'";
						script = tempTable + " <- format(" + table + "$" + column + formatString + ")";
						frame.executeRScript(script);
						script = table + "$" + column + " <- " + "as.Date(" + tempTable + formatString + ")";
						frame.executeRScript(script);
					} else {
						script = tempTable + " <- as.Date(" + table + "$" + column + ", format='" + dateFormat + "')";
						frame.executeRScript(script);
						script = table + "$" + column + " <- " + tempTable;
						frame.executeRScript(script);
					}
					// perform variable cleanup
					frame.executeRScript("rm(" + tempTable + ");");
					frame.executeRScript("gc();");
				}
				//update the metadata
				this.getFrame().getMetaData().modifyDataTypeToProperty(table + "__" + column, table, newType);
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
