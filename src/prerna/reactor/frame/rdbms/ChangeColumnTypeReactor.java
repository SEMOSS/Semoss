package prerna.reactor.frame.rdbms;

import java.util.Arrays;

import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

/**
* This reactor changes the data type of an existing column The inputs to the
* reactor are: 
* 1) the column to update 
* 2) the desired column type
* 3) if the desired type is a date, can enter the current date format
*/

public class ChangeColumnTypeReactor extends AbstractFrameReactor {

	public ChangeColumnTypeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(), ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey(), "format"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		
		// get inputs
		String table = frame.getName();
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column == null) {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.COLUMN.getKey());
		}
		if (column.contains("__")) {
			String[] split = column.split("__");
			table = split[0];
			column = split[1];
		}
		String newType = this.keyValue.get(this.keysToGet[1]);
		if (newType == null) {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.DATA_TYPE.getKey());
		}
		newType = frame.getQueryUtil().cleanType(newType);
		
		String additionalDataType = this.keyValue.get(this.keysToGet[2]);
		
		// check the column exists, if not then throw warning
		String[] allCol = getColNames(frame);
		if (!Arrays.asList(allCol).contains(column)) {
			throw new IllegalArgumentException("Column doesn't exist.");
		}

		// if we are changing from a string to a date, we need to parse using the format entered by the user
		if (newType.equalsIgnoreCase("date")) {
			String dateFormat = this.keyValue.get(this.keysToGet[3]);
			if(dateFormat == null || dateFormat.isEmpty()) {
				dateFormat = "yyyy-MM-dd";
			}
			// query of form: UPDATE FRAME SET birthday = PARSEDATETIME(column, 'format');
			String convertString = "UPDATE " + table + " SET " + column + 
					" = PARSEDATETIME (" + column + ", " + "'" + dateFormat + "');";
			try {
				frame.getBuilder().runQuery(convertString);
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException(e.getMessage());
			}
		} else {
			// if we are not working with a date
			String update = "ALTER TABLE " + table + " ALTER COLUMN " + column + " " + newType + " ; ";
			try {
				frame.getBuilder().runQuery(update);
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException(e.getMessage());
			}
		}

		frame.getMetaData().modifyDataTypeToProperty(table + "__" + column, table, newType);
		if(additionalDataType != null && !additionalDataType.isEmpty()) {
			frame.getMetaData().modifyAdditionalDataTypeToProperty(table + "__" + column, table, newType);
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ChangeColumnType", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}
}
