package prerna.reactor.frame.rdbms;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddColumnReactor extends AbstractFrameReactor {
	
	/**
	 * This reactor adds an empty column to the frame The inputs to the reactor are: 
	 * 1) the name for the new column 
	 * 2) the new column type
	 * 3) the column to duplicate values from
	 */
	
	public AddColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(), ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		String table = frame.getName();
		// get column
		String newColName = this.keyValue.get(this.keysToGet[0]);
		if (newColName == null || newColName.length() == 0) {
			throw new IllegalArgumentException("Need to define the new column name");
		}
		newColName = getCleanNewColName(frame, newColName);

		// get datatype
		String dataType = this.keyValue.get(this.keysToGet[1]);
		if (dataType == null || dataType.isEmpty()) {
			dataType = "STRING";
		}
		String adtlDataType = this.keyValue.get(this.keysToGet[2]);

		// get new column type or set default to string
		// make sql data type
		dataType = frame.getQueryUtil().cleanType(dataType);
		if (frame != null) {
			try {
				frame.getBuilder().runQuery(frame.getQueryUtil().alterTableAddColumn(table, newColName, dataType));
				// set metadata for new column
				OwlTemporalEngineMeta metaData = frame.getMetaData();
				metaData.addProperty(table, table + "__" + newColName);
				metaData.setAliasToProperty(table + "__" + newColName, newColName);
				metaData.setDataTypeToProperty(table + "__" + newColName, dataType);
				if(adtlDataType != null && !adtlDataType.isEmpty()) {
					metaData.setAddtlDataTypeToProperty(frame.getName() + "__" + newColName, adtlDataType);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
