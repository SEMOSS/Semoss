package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class ChangeColumnTypeReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		// get frame
		H2Frame frame = (H2Frame) getFrame();
		String column = getColumn();
		String columnType = getColumnType();

		String table = frame.getTableName();
		String columnName = column;
		if (column.contains("__")) {
			String[] split = column.split("__");
			table = split[0];
			columnName = split[1];
		}
		// check the column exists, if not then throw warning
		String[] allCol = getColNames(table);
		if (!Arrays.asList(allCol).contains(columnName)) {
			throw new IllegalArgumentException("Column doesn't exist.");
		}
		String update = "ALTER TABLE " + table + " ALTER COLUMN " + columnName + " " + columnType + " ; ";
		try {
			frame.getBuilder().runQuery(update);
			frame.getMetaData().modifyDataTypeToProperty(table + "__" + columnName, table, columnType);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

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

}
