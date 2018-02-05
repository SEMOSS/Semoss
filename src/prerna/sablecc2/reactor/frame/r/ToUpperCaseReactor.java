package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ToUpperCaseReactor extends AbstractRFrameReactor {

	/**
	 * This reactor changes columns to all upper case 
	 * The inputs to the reactor are: 
	 * 1) the columns to update
	 */
	
	public ToUpperCaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		//get table name
		String table = frame.getTableName();
		
		//get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		// keep track of selectors to change to upper case
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				String column = getColumn(selectIndex);
				String script = "";
				// separate the table and column names if necessary
				if (column.contains("__")) {
					String[] split = column.split("__");
					column = split[1];
					table = split[0];
				}

				String dataType = metaData.getHeaderTypeAsString(table + "__" + column);
				if (dataType.equalsIgnoreCase("STRING")) {
					// define the script to be executed
					script = table + "$" + column + " <- toupper(" + table + "$" + column + ")";
					// execute the r script
					// script will be of the form:
					// FRAME$column <- toupper(FRAME$column)
					frame.executeRScript(script);
				}
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getColumn(int i) {
		NounMetadata input = this.getCurRow().getNoun(i);
		String thisSelector = input.getValue() + "";
		return thisSelector;
	}
}
