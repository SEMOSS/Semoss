package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class DropColumnReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		// initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// add loop; this would apply if more than one column to drop
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				NounMetadata input = inputsGRS.getNoun(selectIndex);
				String colName = input.getValue() + "";
				// check that the frame is not null
				String table = frame.getTableName();
				// define the r script to be executed
				String script = table + "[," + colName + ":=NULL]";
				// check the column exists, if not then throw warning
				String[] allCol = getColumns(table);
				String column = colName;
				if (colName.contains("__")) {
					String[] split = colName.split("__");
					table = split[0];
					column = split[1];
				}
				if (Arrays.asList(allCol).contains(column) != true) {
					throw new IllegalArgumentException("Column doesn't exist.");
				}

				// execute the script - it will be of the form:
				// FRAME[,FRAME__ColToDrop:=NULL]
				frame.executeRScript(script);

				// update the metadata because the columns are changing
				OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
				metaData.dropProperty(colName, table);
				this.getFrame().syncHeaders();

			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
