package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class RenameColumnReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		String originalColName = getOriginalColumn();
		String updatedColName = getNewColumnName();

		// check that the frame isn't null
		String table = frame.getTableName();
		if (originalColName.contains("__")) {
			String[] split = originalColName.split("__");
			table = split[0];
			originalColName = split[1];
		}
		// ensure new header name is valid
		// make sure that the new name we want to use is valid
		String validNewHeader = getCleanNewHeader(table, updatedColName);
		// define the r script to be executed
		String script = "names(" + table + ")[names(" + table + ") == \"" + originalColName + "\"] = \""
				+ validNewHeader + "\"";
		// execute the r script
		// script is of the form: names(FRAME)[names(FRAME) == "Director"] = "directing_person"
		frame.executeRScript(script);
		// FE passes the column name
		// but meta will still be table __ column
		// update the metadata because column names have changed
		this.getFrame().getMetaData().modifyPropertyName(table + "__" + originalColName, table,
				table + "__" + validNewHeader);
		this.getFrame().syncHeaders();
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private String getOriginalColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		String originalColName = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			NounMetadata input1 = inputsGRS.getNoun(0);
			originalColName = input1.getValue() + "";
		}
		return originalColName;
	}

	private String getNewColumnName() {
		GenRowStruct inputsGRS = this.getCurRow();
		String newColName = "";
		NounMetadata input2 = inputsGRS.getNoun(1);
		newColName = input2.getValue() + "";
		return newColName;
	}
}
