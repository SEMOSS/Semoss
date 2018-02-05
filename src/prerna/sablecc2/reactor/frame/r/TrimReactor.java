package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TrimReactor extends AbstractRFrameReactor {

	/**
	 * This reactor trims column values
	 * The inputs to the reactor are: 
	 * 1) the columns to update
	 */
	
	public TrimReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String table = frame.getTableName();
		//loop through all columns to trim
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int i = 0; i < inputsGRS.size(); i++) {
				String column = getColumn(i);
				// separate the column name from the table name if necessary
				if (column.contains("__")) {
					column = column.split("__")[1];
				}
				
				String dataType = metaData.getHeaderTypeAsString(table + "__" + column);
				if (dataType.equals("STRING")) {
					String script = table + "$" + column + " <- str_trim(" + table + "$" + column + ")";
					// execute the r script
					// script will be of the form: FRAME$column <-
					// str_trim(FRAME$column)
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
