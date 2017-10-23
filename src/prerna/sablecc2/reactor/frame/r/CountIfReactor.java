package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class CountIfReactor extends AbstractRFrameReactor {

	/**
	 * This reactor creates a new column based on the count of regex matches of
	 * an existing column The inputs to the reactor are: 
	 * 1) the column to count regex instances in 
	 * 2) the regex 
	 * 3) the new column name
	 */

	@Override
	public NounMetadata execute() {
		// initialize rJavaTranslator
		init();
		
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		
		//get frame name
		String table = frame.getTableName();

		// get inputs
		String column = getExistingColumn();
		//clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		
		String regexToCount = getRegex();
		String newColName = getNewColumn();

		// define script to be executed
		// dt$new <- str_count(dt$oldCol, "strToFind");
		String script = table + "$" + newColName + " <- str_count(" + table + "$" + column + ", \"" + regexToCount + "\")";
		// execute the script
		frame.executeRScript(script);
		// update the metadata
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(table + "__" + newColName, "NUMBER");
		this.getFrame().syncHeaders();

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
