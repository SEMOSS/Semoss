package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class TrimReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String table = frame.getTableName();
		//loop through all columns to trim
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int i = 0; i < inputsGRS.size(); i++) {
				NounMetadata input = inputsGRS.getNoun(i);
				PixelDataType nounType = input.getNounType();
				if (nounType == PixelDataType.COLUMN) {
					String column = input.getValue() + "";
					//separate the column name from the table name if necessary
					if (column.contains("__")) {
						column = column.split("__")[1];
					}
					String script = table + "$" + column + " <- str_trim(" + table + "$" + column + ")";
					//execute the r script
					//script will be of the form: FRAME$Director <- str_trim(FRAME$Director)
					frame.executeRScript(script);
				}
			}
		}
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
