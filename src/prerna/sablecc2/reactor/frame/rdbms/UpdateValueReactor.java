package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;

import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class UpdateValueReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		String columnInput = "";
		String table = "";
		String column = "";
		GenRowStruct inputsGRS = this.getCurRow();
		// get column to update
		columnInput = inputsGRS.getNoun(0).getValue() + "";
		if (columnInput.contains("__")) {
			String[] split = columnInput.split("__");
			table = split[0];
			column = split[1];
		} else {
			table = frame.getName();
			column = columnInput;
		}

		// check the column exists, if not then throw warning
		String[] allCol = getColNames(table);
		if (Arrays.asList(allCol).contains(column) != true) {
			throw new IllegalArgumentException("Column doesn't exist.");
		}

		// get old column value
		String oldValueSQL = getOldValueSQL();

		// get new column value
		String newValueSQL = getNewValueSQL();

		// create sql update table set column = REXP_REPLACE(column, oldValue, newValue);
		String update = "UPDATE " + table + " SET " + column + " = REGEXP_REPLACE(" + column + ", " + oldValueSQL + ", " + newValueSQL + ");";

		try {
			frame.getBuilder().runQuery(update);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private String getNewValueSQL() {
		GenRowStruct inputsGRS = this.getCurRow();
		String newValueSQL = "";
		NounMetadata noun = inputsGRS.getNoun(2);
		PixelDataType nounType = noun.getNounType();
		if (nounType.equals(PixelDataType.CONST_STRING)) {
			newValueSQL = noun.getValue() + "";
			if (newValueSQL.contains("'")) {
				newValueSQL = newValueSQL.replaceAll("'", "''");
			}
			newValueSQL = "'" + newValueSQL + "'";
		} else {
			newValueSQL = noun.getValue() + "";
		}
		return newValueSQL;
	}

	private String getOldValueSQL() {
		GenRowStruct inputsGRS = this.getCurRow();
		String oldValueSQL = "";
		NounMetadata noun = inputsGRS.getNoun(1);
		PixelDataType nounType = noun.getNounType();
		if (nounType.equals(PixelDataType.CONST_STRING)) {
			oldValueSQL = noun.getValue() + "";
			if (oldValueSQL.contains("'")) {
				oldValueSQL = oldValueSQL.replaceAll("'", "''");
			}
			oldValueSQL = "'" + oldValueSQL + "'";
		} else {
			oldValueSQL = noun.getValue() + "";
		}
		return oldValueSQL;
	}

}
