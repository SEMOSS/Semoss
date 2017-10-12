package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class ToLowerCaseReactor extends AbstractRFrameReactor {
	
	/**
	 * This reactor changes columns to all lower case 
	 * The inputs to the reactor are: 
	 * 1) the columns to update
	 */

	@Override
	public NounMetadata execute() {
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		// keep all selectors that we are changing to lower case
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				NounMetadata input = inputsGRS.getNoun(selectIndex);
				String thisSelector = input.getValue() + "";
				String table = frame.getTableName();
				String column = thisSelector;
				// separate table from column name if necessary
				if (thisSelector.contains("__")) {
					String[] split = thisSelector.split("__");
					table = split[0];
					column = split[1];
				}
				// validate data type
				OwlTemporalEngineMeta metaData = frame.getMetaData();
				String dataType = metaData.getHeaderTypeAsString(thisSelector);
				if (!dataType.equals("STRING")) {
					throw new IllegalArgumentException("Data type not supported.");
				}

				// script will take the form: FRAME$column <- tolower(FRAME$column)
				String script = table + "$" + column + " <- tolower(" + table + "$" + column + ")";
				// execute the r script
				frame.executeRScript(script);
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
