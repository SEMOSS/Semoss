package prerna.sablecc2.reactor.frame.py;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class AddColumnReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor adds an empty column to the frame The inputs to the reactor are: 
	 * 1) the name for the new column 
	 * 2) the new column type
	 */
	
	public AddColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(), ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		// get new column name from the gen row struct
		String newColName = this.keyValue.get(this.keysToGet[0]);
		if (newColName == null || newColName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the new column name");
		}
		
		// get the column type and standardize
		String colType = this.keyValue.get(this.keysToGet[1]);
		if (colType == null) {
			colType = SemossDataType.convertStringToDataType("STRING").toString();
		}
		String adtlDataType = this.keyValue.get(this.keysToGet[2]);

		String table = frame.getName();
		// clean colName
		if (newColName.contains("__")) {
			String[] split = newColName.split("__");
			newColName = split[1];
		}
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(frame, newColName);

		// define the script to be executed;
		// this assigns a new column name with no data in columns
		String newColumnSelector = table + "['" + newColName + "']";
		String script = newColumnSelector + "= \"\"";
		// execute the script
		frame.runScript(script);

		// update the metadata to include this new column
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		if(adtlDataType != null && !adtlDataType.isEmpty()) {
			metaData.setAddtlDataTypeToProperty(frame.getName() + "__" + newColName, adtlDataType);
		}
		
		if (Utility.isNumericType(colType)) {
			// update the metadata depending on the data type
			metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.DOUBLE.toString());
			script = newColumnSelector + "= pd.to_numeric(" + newColumnSelector + ", errors='coerce')";
			frame.runScript(script);
		} else if (Utility.isDateType(colType)) {
			metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.DATE.toString());
			script = newColumnSelector + "= pd.to_datetime(" + newColumnSelector + ", errors='coerce')";
			frame.runScript(script);
		} else {
			// if not a number or a date then assign to string
			metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.STRING.toString());
		}
		frame.syncHeaders();

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"AddColumn", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		return retNoun;
	}
}
