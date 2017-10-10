package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class ToUpperCaseReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();

		//keep track of selectors to change to upper case
		List<String> selectors = new Vector<String>();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				NounMetadata input = inputsGRS.getNoun(selectIndex);
				PixelDataType nounType = input.getNounType();
				if (nounType == PixelDataType.COLUMN) {
					String thisSelector = input.getValue() + "";
					selectors.add(thisSelector);
				}
			}

			String script = ""; 
			if (frame != null) {
				for (int i = 0; i < selectors.size(); i++) {
					String selector = selectors.get(i);
					String table = frame.getTableName();
					String column = selector;
					//separate the table and column names if necessary
					if (selector.contains("__")) {
						String[] split = selector.split("__");
						table = split[0];
						column = split[1];
					}
					// execute update table set column = UPPER(column);
					//define the script to be executed
					script = table + "$" + column + " <- toupper(" + table + "$" + column + ")";
					//execute the r script
					//script will be of the form: FRAME$Director <- toupper(FRAME$Director)
					frame.executeRScript(script);
				}
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
