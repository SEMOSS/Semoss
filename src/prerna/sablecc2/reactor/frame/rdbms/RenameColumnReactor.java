package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;

import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class RenameColumnReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		String originalColName = getOriginalColumn();
		String newColName = getNewColumnName();

		String table = frame.getTableName();
		String column = originalColName;
		if (originalColName.contains("__")) {
			String[] split = originalColName.split("__");
			table = split[0];
			column = split[1];
		}
		// validate column exists and clean new name
		String[] allCol = getColNames(table);
		if (Arrays.asList(allCol).contains(column) != true) {
			throw new IllegalArgumentException("Column doesn't exist.");
		}
		newColName = getCleanNewColName(table, newColName);

		String update = "ALTER TABLE " + table + " RENAME COLUMN " + column + " TO " + newColName + " ; ";
		try {
			frame.getBuilder().runQuery(update);
			// update metadata
			frame.getMetaData().modifyPropertyName(table + "__" + column, table, table + "__" + newColName);
			frame.syncHeaders();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private String getOriginalColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		String originalColName = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			NounMetadata input1 = inputsGRS.getNoun(0);
			originalColName = input1.getValue() + "";
		}
		return originalColName;
	}

	private String getNewColumnName() {
		GenRowStruct inputsGRS = this.getCurRow();
		String newColName = "";
		NounMetadata input2 = inputsGRS.getNoun(1);
		newColName = input2.getValue() + "";
		return newColName;
	}
}
