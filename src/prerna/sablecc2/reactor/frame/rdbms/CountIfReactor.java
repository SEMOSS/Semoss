package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.Utility;

public class CountIfReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		String newColumnName = getNewColumn();
		String columnToCount = getCountColumn();
		String regex = getRegexString();
		String table = frame.getName();

		// clean column to count
		if (columnToCount.contains("__")) {
			String[] split = columnToCount.split("__");
			table = split[0];
			columnToCount = split[1];
		}
		String[] existCols = getColNames(columnToCount);
		if (Arrays.asList(existCols).contains(columnToCount) != true) {
			throw new IllegalArgumentException("Column: " + columnToCount + " doesn't exist.");
		}
		// new column datatype is set to numeric
		String dataType = SemossDataType.convertDataTypeToString(SemossDataType.convertStringToDataType("NUMBER"));
		// clean new column name
		newColumnName = getCleanNewColName(table, newColumnName);

		// escape single quote for sql
		if (regex.contains("'")) {
			regex = regex.replaceAll("'", "''");
		}
		// 1) first add new column name
		String addColumnSQL = "ALTER TABLE " + table + " ADD " + newColumnName + " " + dataType + ";";
			// 2) create a temp column to replace the matching string in the column to count with a replacement string
		String tempColName = "REP_" + Utility.getRandomString(5);
		String addTempColumn = "ALTER TABLE " + table + " ADD  " + tempColName + " varchar(800);";
		String tempReplacementString = ";;;" + Utility.getRandomString(3) + ";;;";
		String updateTempColumn = "UPDATE " + table + " SET " + tempColName + "= REGEXP_REPLACE (" + columnToCount + ", '" + regex + "', '" + tempReplacementString + "');";

		// 3) Update the count column by setting it to the length of the col - replacing the temp column with empty string
		String updateCountColumn = "UPDATE " + table + " SET " + newColumnName + " = " + "LENGTH("+tempColName+") - LENGTH(REPLACE(" + tempColName + ",'" + tempReplacementString + "',''));";

		// 4) Update the count with MOD (tempColumn, tempString - 1)
		updateCountColumn += "UPDATE " + table + " SET " + newColumnName + " = MOD(" + newColumnName + "," + (tempReplacementString.length() - 1) + " );";

		// 5) Drop temp column
		String dropTempColumn = "ALTER TABLE " + table + " DROP COLUMN " + tempColName + ";";

		try {
			frame.getBuilder().runQuery(addColumnSQL);
			frame.getBuilder().runQuery(addTempColumn);
			frame.getBuilder().runQuery(updateTempColumn);
			frame.getBuilder().runQuery(updateCountColumn);
			frame.getBuilder().runQuery(dropTempColumn);

			// set metadata for new column name
			OwlTemporalEngineMeta metaData = frame.getMetaData();
			metaData.addProperty(table, table + "__" + newColumnName);
			metaData.setAliasToProperty(table + "__" + newColumnName, newColumnName);
			metaData.setDataTypeToProperty(table + "__" + newColumnName, dataType);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private String getCountColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		String countColumn = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			countColumn = inputsGRS.getNoun(0).getValue() + "";
			if (countColumn.length() < 0) {
				throw new IllegalArgumentException("Need to define the column for count");
			}
			return countColumn;
		}
		throw new IllegalArgumentException("Need to define the column for count");
	}

	private String getRegexString() {
		GenRowStruct inputsGRS = this.getCurRow();
		String regex = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			regex = inputsGRS.getNoun(1).getValue() + "";
			if (regex.length() < 0) {
				throw new IllegalArgumentException("Need to define the regex for count");
			}
			return regex;
		}
		throw new IllegalArgumentException("Need to define the regex for count");
	}

	private String getNewColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		String newColumn = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			newColumn = inputsGRS.getNoun(2).getValue() + "";
			if (newColumn.length() < 0) {
				throw new IllegalArgumentException("Need to define the new column name for count");
			}
			return newColumn;
		}
		throw new IllegalArgumentException("Need to define the new column name for count");
	}

}
