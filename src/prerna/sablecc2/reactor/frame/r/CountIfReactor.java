package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class CountIfReactor extends AbstractRFrameReactor {

	/**
	 * This reactor creates a new column based on the count of regex matches of
	 * an existing column The inputs to the reactor are: 1) the column to count
	 * regex instances in 2) the regex 3) the new column name
	 */

	@Override
	public NounMetadata execute() {
		// initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String table = frame.getTableName();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {

			// first input is the name of the column 
			// that the operation is being done on
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullColumn = noun1.getValue() + "";
			String column = fullColumn;
			if (fullColumn.contains("__")) {
				column = fullColumn.split("__")[1];
			}

			// second input is the regex to count
			NounMetadata noun2 = inputsGRS.getNoun(1);
			String strToCount = noun2.getValue() + "";

			// third input is the name of the new column that will be created
			NounMetadata noun3 = inputsGRS.getNoun(2);
			String newColName = noun3.getValue() + "";

			// define script to be executed
			// dt$new <- str_count(dt$oldCol, "strToFind");
			String script = table + "$" + newColName + " <- str_count(" + table + "$" + column + ", \"" + strToCount + "\")";
			// execute the script
			frame.executeRScript(script);
			// update the metadata
			OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
			metaData.addProperty(table, table + "__" + newColName);
			metaData.setAliasToProperty(table + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(table + "__" + newColName, "NUMBER");
			this.getFrame().syncHeaders();
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
