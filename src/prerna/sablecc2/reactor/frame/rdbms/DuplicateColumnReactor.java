package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class DuplicateColumnReactor extends AbstractFrameReactor {

	/**
	 * This reactor duplicates and existing column and adds it to the frame. The
	 * inputs to the reactor are: 
	 * 1) the name for the column to duplicate 
	 * 2) the new column name
	 */

	public DuplicateColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// get frame
		H2Frame frame = (H2Frame) getFrame();
		String table = frame.getName();

		// get source column to duplicate
		String srcCol = this.keyValue.get(this.keysToGet[0]);

		// make sure source column exists
		String[] allCol = getColNames(table);
		if (srcCol == null || !Arrays.asList(allCol).contains(srcCol)) {
			throw new IllegalArgumentException("Need to define an existing column to duplicate.");
		}

		// clean and validate new column name or use default name
		String newColName = getCleanNewColName(table, srcCol + "_DUPLICATE");
		String inputColName = this.keyValue.get(this.keysToGet[1]);
		if (inputColName != null && !inputColName.isEmpty()) {
			inputColName = getCleanNewColName(table, inputColName);
			// entire new name could be invalid characters
			if (!inputColName.equals("")) {
				newColName = inputColName;
			}
		}

		// get src column data type
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		String dataType = metaData.getHeaderTypeAsString(table + "__" + srcCol);
		dataType = SemossDataType.convertDataTypeToString(SemossDataType.convertStringToDataType(dataType));

		// use existing column data type to make new column
		String addNewCol = "ALTER TABLE " + table + " ADD " + newColName + " " + dataType + ";";
		try {
			frame.getBuilder().runQuery(addNewCol);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// update new column with the duplicate data
		String duplicate = "UPDATE " + table + " SET " + newColName + " = " + srcCol + ";";
		try {
			frame.getBuilder().runQuery(duplicate);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// update meta data
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(table + "__" + newColName, dataType);

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		return retNoun;
	}
}