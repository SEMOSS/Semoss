package prerna.sablecc2.reactor.frame.r;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * This reactor changes the data type of an existing column The inputs to the
 * reactor are: 
 * 1) the column to update 
 * 2) the desired column type
 */

public class ChangeColumnTypeReactor extends AbstractRFrameReactor {
	
	public ChangeColumnTypeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		//get table name
		String table = frame.getTableName();
		// get inputs
		String column = getColumn();
		if (column.contains("__")) {
			String[] split = column.split("__");
			column = split[1];
		}
		String newType = getColumnType();
		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		String dataType = metadata.getHeaderTypeAsString(table + "__" + column);
		
		//check if there is a new dataType
		if (!dataType.equals(newType)) {
			// define the r script to execute
			// script depends on the new data type
			String script = null;
			if (Utility.isStringType(newType)) {
				// df$column <- as.character(df$column);
				script = table + "$" + column + " <- as.character(" + table + "$" + column + ");";
				frame.executeRScript(script);
			} else if (newType.equalsIgnoreCase("factor")) {
				// df$column <- as.factor(df$column);
				script = table + "$" + column + " <- as.factor(" + table + "$" + column + ");";
				frame.executeRScript(script);
			} else if (Utility.isDoubleType(newType)) {
				// r script syntax cleaning characters with regex
				script = table + "$" + column + " <- as.numeric(gsub('[^-\\\\.0-9]', '', " + table + "$" + column + "));";
				frame.executeRScript(script);
			} else if (Utility.isDateType(newType)) {
				// we have a different script to run if it is a str to date
				// conversion
				// define date format
				String dateFormat = "%Y-%m-%d";
				// get the column type of the existing column
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
			// update the metadata
			metadata.modifyDataTypeToProperty(table + "__" + column, table, newType);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////


	private String getColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			String colName = inputsGRS.getNoun(0).getValue() + "";
			if (colName.length() > 0) {
				return colName;
			}
		}
		throw new IllegalArgumentException("Need to define the new column name");
	}

	private String getColumnType() {
		GenRowStruct inputsGRS = this.getCurRow();
		NounMetadata input2 = inputsGRS.getNoun(1);
		String columnType = input2.getValue() + "";
		// default to string
		if (columnType.length() == 0) {
			columnType = "STRING";
		}
		// check if data type is supported
		SemossDataType dt = SemossDataType.convertStringToDataType(columnType);
		if (dt == null) {
			dt = SemossDataType.STRING;
		}
		return dt.toString();
	}
}
