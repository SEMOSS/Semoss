package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;

import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class DropColumnReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		GenRowStruct inputsGRS = this.getCurRow();
		String update = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				NounMetadata input = inputsGRS.getNoun(selectIndex);
				String thisSelector = input.getValue() + "";
				String table = frame.getName();
				String column = thisSelector;
				if (thisSelector.contains("__")) {
					String[] split = thisSelector.split("__");
					table = split[0];
					column = split[1];
				}
				// ALTER TABLE table DROP column
				update += "ALTER TABLE " + table + " DROP " + column + " ; ";

				// check the column exists, if not then throw warning
				String[] allCol = getColNames(table);
				if (Arrays.asList(allCol).contains(column) != true) {
					throw new IllegalArgumentException("Column doesn't exist.");
				}

				// update meta
				frame.getMetaData().dropProperty(thisSelector, table);
				frame.syncHeaders();
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
