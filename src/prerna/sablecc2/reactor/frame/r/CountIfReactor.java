package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;


public class CountIfReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		//initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String table = frame.getTableName();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//first input is the name of the new column that will be created
			NounMetadata noun1 = inputsGRS.getNoun(0); 
			String newColName = noun1.getValue() + "";

			//second input is the name of the column that the operation is being done on
			NounMetadata noun2 = inputsGRS.getNoun(1);
			String fullColumn = noun2.getValue() + "";
			String column = fullColumn; 
			if (fullColumn.contains("__")) {
				column = fullColumn.split("__")[1]; 
			}

			//third input is the string to count
			NounMetadata noun3 = inputsGRS.getNoun(2);
			String strToCount = noun3.getValue() + "";

			//define script to be executed
			// dt$new <- str_count(dt$oldCol, "strToFind");
			String script = table + "$" + newColName + " <- str_count(" + table + "$" + column + ", \""
					+ strToCount + "\")";
			//execute the script
			frame.executeRScript(script);
			//update the metadata
			OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
			metaData.addProperty(table, table + "__" + newColName);
			metaData.setAliasToProperty(table + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(table + "__" + newColName, "NUMBER");
			this.getFrame().syncHeaders(); 
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
