package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.query.querystruct.transform.QSRenameColumnConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.ModifyHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RenameColumnReactor extends AbstractRFrameReactor {

	/**
	 * This reactor renames a column 
	 * 1) the original column
	 * 2) the new column name 
	 */
	
	public RenameColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		String originalColName = keyValue.get(this.keysToGet[0]);
		String updatedColName = keyValue.get(this.keysToGet[1]);
		// check that the frame isn't null
		String table = frame.getName();
		// check if new colName is valid
		updatedColName = getCleanNewColName(table, updatedColName);
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
		String validNewHeader = getCleanNewHeader(table, updatedColName);
		if (validNewHeader.equals("")) {
			throw new IllegalArgumentException("Provide valid new column name (no special characters)");
		}
		// define the r script to be executed
		String script = "names(" + table + ")[names(" + table + ") == \"" + originalColName + "\"] = \""
				+ validNewHeader + "\"";
		// execute the r script
		// script is of the form: names(FRAME)[names(FRAME) == "Director"] = "directing_person"
		frame.executeRScript(script);
		// FE passes the column name
		// but meta will still be table __ column
		// update the metadata because column names have changed
		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		metadata.modifyPropertyName(table + "__" + originalColName, table, table + "__" + validNewHeader);
		metadata.setAliasToProperty(table + "__" + validNewHeader, validNewHeader);
		this.getFrame().syncHeaders();

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		ModifyHeaderNounMetadata metaNoun = new ModifyHeaderNounMetadata(originalColName, validNewHeader);
		retNoun.addAdditionalReturn(metaNoun);
		
		// also modify the frame filters
		Map<String, String> modMap = new HashMap<String, String>();
		modMap.put(originalColName, validNewHeader);
		frame.setFrameFilters(QSRenameColumnConverter.convertGenRowFilters(frame.getFrameFilters(), modMap, false));
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"RenameColumn", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// return the output
		return retNoun;
	}
}
