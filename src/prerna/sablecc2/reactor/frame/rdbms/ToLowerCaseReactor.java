package prerna.sablecc2.reactor.frame.rdbms;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class ToLowerCaseReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		// get frame
		H2Frame frame = (H2Frame) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		GenRowStruct inputsGRS = this.getCurRow();
		String update = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				NounMetadata input = inputsGRS.getNoun(selectIndex);
				String columnName = input.getValue() + "";
				
				String table = frame.getName();
				String column = columnName;
				if (columnName.contains("__")) {
					String[] split = columnName.split("__");
					table = split[0];
					column = split[1];
				}
				
				String dataType = metaData.getHeaderTypeAsString(table + "__" + column);
				if (dataType.equals("STRING")) {
					// execute update table set column = UPPER(column);
					update += "UPDATE " + table + " SET " + column + " = LOWER(" + column + ") ; ";
				}
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
