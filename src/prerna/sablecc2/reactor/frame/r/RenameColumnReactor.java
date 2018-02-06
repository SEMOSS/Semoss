package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.query.querystruct.transform.QSRenameColumnConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.ModifyHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RenameColumnReactor extends AbstractRFrameReactor {

	/**
	 * This reactor renames a column 
	 * 1) the original column
	 * 2) the new column name 
	 */
	
	public RenameColumnReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		String originalColName = getOriginalColumn();
		String newColName = getNewColumnName();
		// check that the frame isn't null
		String table = frame.getTableName();
		// check if new colName is valid
		newColName = getCleanNewColName(table, newColName);
		if (originalColName.contains("__")) {
			String[] split = originalColName.split("__");
			table = split[0];
			originalColName = split[1];
		}
		// ensure new header name is valid
		// make sure that the new name we want to use is valid
		String[] existCols = getColNames(originalColName);
		if (Arrays.asList(existCols).contains(originalColName) != true) {
			throw new IllegalArgumentException("Column doesn't exist.");
		}
		newColName = getCleanNewHeader(table, newColName);
		if (newColName.equals("")){
			throw new IllegalArgumentException("Provide valid new column name (no special characters)");
		}
		// define the r script to be executed
		String script = "names(" + table + ")[names(" + table + ") == \"" + originalColName + "\"] = \"" + newColName + "\"";
		// execute the r script
		// script is of the form: names(FRAME)[names(FRAME) == "Director"] = "directing_person"
		frame.executeRScript(script);
		// FE passes the column name
		// but meta will still be table __ column
		// update the metadata because column names have changed
		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		metadata.modifyPropertyName(table + "__" + originalColName, table, table + "__" + newColName);
		metadata.setAliasToProperty(table + "__" + newColName, newColName);
		this.getFrame().syncHeaders();

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		ModifyHeaderNounMetadata metaNoun = new ModifyHeaderNounMetadata(originalColName, newColName);
		retNoun.addAdditionalReturn(metaNoun);
		
		// also modify the frame filters
		Map<String, String> modMap = new HashMap<String, String>();
		modMap.put(originalColName, newColName);
		frame.setFrameFilters(QSRenameColumnConverter.convertGenRowFilters(frame.getFrameFilters(), modMap));
		
		// return the output
		return retNoun;
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
