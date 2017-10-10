package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class SortColumnReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {

		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();

		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//the first input will be the column to sort
			NounMetadata input1 = inputsGRS.getNoun(0);
			PixelDataType nounType1 = input1.getNounType();
			String column = "";
			if (nounType1 == PixelDataType.COLUMN) {
				String fullColumn = input1.getValue() + "";
				column = fullColumn.split("__")[1];
			}

			//the second input will be the sort direction
			NounMetadata input2 = inputsGRS.getNoun(1);
			PixelDataType nounType2 = input2.getNounType();
			String sortDir = null; 
			if (nounType2 == PixelDataType.CONST_STRING) {
				sortDir = input2.getValue() + ""; 
			}

			//check that the frame is not null
			if (frame != null) {
				String table = frame.getTableName();

				//define the scripts based on the sort direction
				String script = null;
				if (sortDir == null || sortDir.equalsIgnoreCase("asc")) {
					script = table + " <- " + table + "[order(rank(" + column + "))]";
				} else if (sortDir.equalsIgnoreCase("desc")) {
					script = table + " <- " + table + "[order(-rank(" + column + "))]";
				}
				//execute the r script
				//script will be of the form: FRAME <- FRAME[order(rank(Director))]
				frame.executeRScript(script);
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
