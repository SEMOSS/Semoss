package prerna.sablecc2.reactor.frame.py;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

/**
 * This reactor changes the data type of an existing column The inputs to the
 * reactor are: 
 * 1) the column to update 
 * 2) the desired column type
 */

public class ChangeColumnTypeReactor extends AbstractFrameReactor {
	
	public ChangeColumnTypeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		
		// if it is string super easy
		// if it is int need to see if this is a string if so run regex
		// if it is a int and the current is a float need to run regex
		// if it is float and current is int need to do as type
		// if it datetime - let python handle it
		// if it is datetime - may be can ask what format and go from there ?
		// if it is datetime - 
		
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		// get table name
		String table = frame.getName();
		// get inputs
		String column = this.keyValue.get(this.keysToGet[0]);
		
		if (column == null) {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.COLUMN.getKey());
		}
		if (column.contains("__")) {
			String[] split = column.split("__");
			column = split[1];
		}
		
		String newType = this.keyValue.get(this.keysToGet[1]);
		if (newType == null) {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.DATA_TYPE.getKey());
		}
		
		newType = SemossDataType.convertStringToDataType(newType).toString();
		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		String curType = metadata.getHeaderTypeAsString(table + "__" + column);
		
		//check if there is a new dataType
		if (!curType.equals(newType)) {
			// define the r script to execute
			// script depends on the new data type
			String script = null;
			if (Utility.isStringType(newType)) {
				// df$column <- as.character(df$column);
				// create temp table without scientific format for numeric
				// columns
				//if ((boolean)frame.runScript(table + "w.is_numeric('" + column + "')")) 
				//{
					frame.runScript(table + "['" + column + "'] = " + table + "['" + column + "'].astype('object')" );
				//} 
			} else if (newType.equalsIgnoreCase("factor")) {
				// df$column <- as.factor(df$column);
				frame.runScript(table + "['" + column + "'] = " + table + "['" + column + "'].astype('object')" );
			} else if (Utility.isDoubleType(newType)) {
				// r script syntax cleaning characters with regex
				//mv['RottenTomatoesCritics'].astype('str').str.replace('[^-\\\\.0-9]', 'dflasd', regex=True).astype('float64', errors='ignore')
				String script2 = table + "['" + column + "']" + " = " + table + "['" + column + "'].astype('str').str.replace('[^-\\\\.0-9]', '', regex=True).astype('float64', errors='ignore')";
				frame.runScript(script2);
			} else if (Utility.isDateType(newType)) {
				// we have a different script to run if it is a str to date
				// conversion
				// define date format
				frame.runScript(table + "['" + column + "'] = pd.to_datetime(" + table + "['" + column + "'])");				
			}
			// update the metadata
			metadata.modifyDataTypeToProperty(table + "__" + column, table, newType);
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ChangeColumnType", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
