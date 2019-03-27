package prerna.sablecc2.reactor.frame.py;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class CountIfReactor extends AbstractFrameReactor {

	/**
	 * This reactor creates a new column based on the count of regex matches of
	 * an existing column The inputs to the reactor are: 
	 * 1) the column to count regex instances in 
	 * 2) the regex 
	 * 3) the new column name
	 */
	
	public CountIfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.REGEX.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		// get frame name
		String table = frame.getName();
		System.out.println("Table.. " + table);
		// get inputs
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column == null) {
			column = getExistingColumn();
		}
		// clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		String regexToCount = this.keyValue.get(this.keysToGet[1]);
		if (regexToCount == null) {
			regexToCount = getRegex();
		}
		String newColName = this.keyValue.get(this.keysToGet[2]);
		if (newColName == null) {
			newColName = getNewColumn();
		}
		// check if new colName is valid
		newColName = getCleanNewColName(table, newColName);

		// this function only works on strings, so we must convert the data to a
		// string if it is not already
		boolean numeric = (boolean)frame.runScript(table + ".is_numeric('" + column + "')");

		if(!numeric)
			frame.runScript(table + ".countif('" + column + "', '" + regexToCount + "', '" + newColName + "')");
		//else
			// return an error ?

		// update the metadata
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(frame.getName(), frame.getName() + "__" + newColName);
		metaData.setAliasToProperty(frame.getName() + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frame.getName() + "__" + newColName, SemossDataType.DOUBLE.toString());
		this.getFrame().syncHeaders();

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"CountIf", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}




	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////


	private String getExistingColumn() {
		// first input is the name of the column
		// that the operation is being done on
		NounMetadata noun = this.curRow.getNoun(0);
		String column = noun.getValue().toString();
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		return column;
	}

	private String getRegex() {
		// second input is the regex to count
		NounMetadata noun = this.curRow.getNoun(1);
		return noun.getValue().toString();
	}

	private String getNewColumn() {
		// third input is the new column name
		NounMetadata noun = this.curRow.getNoun(2);
		String column = noun.getValue().toString();
		return column;
	}
	
}
