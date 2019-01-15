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

public class JoinColumnsReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		// JoinColumns("newColumnName", "separator", Col1, Col2...);
		H2Frame frame = (H2Frame) getFrame();
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// get new column name
			NounMetadata colNameNoun = inputsGRS.getNoun(0);
			String colName = colNameNoun.getValue() + "";

			// make sql data type
			String dataType = "STRING";
			dataType = SemossDataType.convertDataTypeToString(SemossDataType.convertStringToDataType(dataType));
			String table = frame.getName();

			// validate new column name
			colName = getCleanNewColName(table, colName);

			// create sql statement to add new column
			String addColumn = "ALTER TABLE " + table + " ADD " + colName + " " + dataType + ";";

			// prepare separator for sql
			String separator = String.valueOf(inputsGRS.getNoun(1).getValue());
			if (separator.contains("'")) {
				separator = separator.replaceAll("'", "''");
			}
			separator = "'" + separator + "'";

			// update new column with concatenated values col1 + separator  col2 ...
			String updateColumn = "UPDATE " + table + " SET " + colName + " = CONCAT (";
			for (int i = 2; i < inputsGRS.size(); i++) {
				String column = inputsGRS.getNoun(i).getValue() + "";
				// Clean column
				if (column.contains("__")) {
					String[] split = column.split("__");
					column = split[1];
				}
				// if only one column
				if (inputsGRS.size() == 3) {
					updateColumn += column + " , " + separator;
				} else if (i == inputsGRS.size() - 1) {
					updateColumn += column;
				} else {
					updateColumn += column + " , " + separator + " , ";
				}
				// does the column exist?
				String[] existCols = getColNames(column);
				if (Arrays.asList(existCols).contains(column) != true) {
					throw new IllegalArgumentException("Column: " + column + " doesn't exist.");
				}

			}
			updateColumn += ");";

			try {
				frame.getBuilder().runQuery(addColumn + updateColumn);
				// set metadata for new column
				OwlTemporalEngineMeta metaData = frame.getMetaData();
				metaData.addProperty(table, table + "__" + colName);
				metaData.setAliasToProperty(table + "__" + colName, colName);
				metaData.setDataTypeToProperty(table + "__" + colName, dataType);
				frame.syncHeaders();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
