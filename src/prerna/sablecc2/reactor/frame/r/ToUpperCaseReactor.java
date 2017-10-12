package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class ToUpperCaseReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		GenRowStruct inputsGRS = this.getCurRow();

		// keep track of selectors to change to upper case
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				NounMetadata input = inputsGRS.getNoun(selectIndex);
				String thisSelector = input.getValue() + "";
				String script = "";
				String table = frame.getTableName();
				String column = thisSelector;
				// separate the table and column names if necessary
				if (thisSelector.contains("__")) {
					String[] split = thisSelector.split("__");
					table = split[0];
					column = split[1];
				}

				OwlTemporalEngineMeta metaData = frame.getMetaData();
				String dataType = metaData.getHeaderTypeAsString(thisSelector);
				if (!dataType.equals("STRING")) {
					throw new IllegalArgumentException("Data type not supported.");
				}

				// define the script to be executed
				script = table + "$" + column + " <- toupper(" + table + "$" + column + ")";
				// execute the r script
				// script will be of the form: FRAME$column <- toupper(FRAME$column)
				frame.executeRScript(script);
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
