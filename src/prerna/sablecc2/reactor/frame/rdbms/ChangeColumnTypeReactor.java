package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

/**
* This reactor changes the data type of an existing column The inputs to the
* reactor are: 
* 1) the column to update 
* 2) the desired column type
* 3) if the desired type is a date, can enter the current date format
*/

public class ChangeColumnTypeReactor extends AbstractFrameReactor {

	// this constant used when we specify the format for the dates
	private static final String FORMAT = "format";

	@Override
	public NounMetadata execute() {
		// get frame
		H2Frame frame = (H2Frame) getFrame();
		String column = getColumn();
		String columnType = getColumnType();

		String table = frame.getName();
		if (column.contains("__")) {
			String[] split = column.split("__");
			table = split[0];
			column = split[1];
		}

		// check the column exists, if not then throw warning
		String[] allCol = getColNames(table);
		if (!Arrays.asList(allCol).contains(column)) {
			throw new IllegalArgumentException("Column doesn't exist.");
		}

		// if we are changing from a string to a date, we need to parse using the format entered by the user
		if (columnType.equalsIgnoreCase("date")) {
			String dateFormat = getDateFormat();
			// query of form: UPDATE FRAME SET birthday = PARSEDATETIME(column, 'format');
			String convertString = "UPDATE " + table + " SET " + column + 
					" = PARSEDATETIME (" + column + ", " + "'" + dateFormat + "');";
			try {
				frame.getBuilder().runQuery(convertString);
				frame.getMetaData().modifyDataTypeToProperty(table + "__" + column, table, columnType);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			// if we are not working with a date
			String update = "ALTER TABLE " + table + " ALTER COLUMN " + column + " " + columnType + " ; ";
			try {
				frame.getBuilder().runQuery(update);
				frame.getMetaData().modifyDataTypeToProperty(table + "__" + column, table, columnType);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	// this method gets the name of the column that we are modifying
	private String getColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			String colName = inputsGRS.getNoun(0).getValue() + "";
			if (colName.length() == 0) {
				throw new IllegalArgumentException("Need to define the new column name");
			}
			return colName;
		}
		throw new IllegalArgumentException("Need to define the new column name");
	}

	// this method gets the desired column type
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
		// make sql dataType
		columnType = SemossDataType.convertDataTypeToString(dt);
		return columnType;
	}

	// we will get the format entered by the user so we can use PARSEDATETIME
	private String getDateFormat() {
		// grab the format that was inputted
		GenRowStruct formatGRS = this.store.getNoun(FORMAT);
		if (formatGRS != null) {
			if (formatGRS.size() > 0) {
				// get the user input which can either be from the drop down or free text
				String dateFormat = formatGRS.get(0).toString();
				// semoss uses underscores where the user might enter spaces
				// for example, MMM yyyy will be updated to MMM_yyyy
				dateFormat = dateFormat.replace(" ", "_");
				return dateFormat;
			}
		}
		// default to date format yyyy-mm-dd
		return "yyyy-MM-dd";
	}
}
