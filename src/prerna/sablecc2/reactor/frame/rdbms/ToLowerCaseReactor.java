package prerna.sablecc2.reactor.frame.rdbms;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class ToLowerCaseReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		// get frame
		H2Frame frame = (H2Frame) getFrame();
		GenRowStruct inputsGRS = this.getCurRow();
		String update = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				NounMetadata input = inputsGRS.getNoun(selectIndex);
				String thisSelector = input.getValue() + "";
				String table = frame.getTableName();
				String column = thisSelector;
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

				// execute update table set column = UPPER(column);
				update += "UPDATE " + table + " SET " + column + " = LOWER(" + column + ") ; ";
			}
			if (update.length() > 0) {
				try {
					frame.getBuilder().runQuery(update);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
