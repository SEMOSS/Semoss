package prerna.sablecc2.reactor.frame.rdbms;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class AddColumnReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		// get new column name
		String colName = getNewColumnName();
		String table = frame.getTableName();

		// clean column name
		if (colName.contains("__")) {
			String[] split = colName.split("__");
			table = split[0];
			colName = split[1];
		}
		colName = getCleanNewColName(table, colName);

		// get new column type or set default to string
		String dataType = getDataType();
		// make sql data type
		dataType = SemossDataType.convertDataTypeToString(SemossDataType.convertStringToDataType(dataType));
		if (frame != null) {
			String update = "ALTER TABLE " + table + " ADD " + colName + " " + dataType + ";";
			try {
				frame.getBuilder().runQuery(update);
				// set metadata for new column
				OwlTemporalEngineMeta metaData = frame.getMetaData();
				metaData.addProperty(table, table + "__" + colName);
				metaData.setAliasToProperty(table + "__" + colName, colName);
				metaData.setDataTypeToProperty(table + "__" + colName, dataType);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	/////////////////// PIXEL INPUTS //////////////////////////
	
	private String getNewColumnName() {
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

	private String getDataType() {
		String dataType = "STRING";
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS.size() > 1) {
			NounMetadata colTypeNoun = inputsGRS.getNoun(1);
			dataType = colTypeNoun.getValue() + "";
		}
		return dataType;
	}
}
