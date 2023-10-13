package prerna.reactor.frame.r;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class TimestampDataReactor extends AbstractRFrameReactor {

	private static final String TIME_KEY = "time";
	
	public TimestampDataReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.NEW_COLUMN.getKey(), TIME_KEY};
	}
	
	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get new column name from the gen row struct
		String newColName = this.keyValue.get(this.keysToGet[0]);
		if (newColName == null || newColName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the new column name");
		}

		String table = frame.getName();
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(frame, newColName);
		
		String includeTime = this.keyValue.get(this.keysToGet[1]);
		Boolean includeT = Boolean.parseBoolean(includeTime);
		
		String script = table + "$"  + newColName + " <- ";
		if(includeT) {
			script += "as.POSIXct(Sys.time())";
		} else {
			script += "as.Date(Sys.Date())";
		}
		
		// execute
		frame.executeRScript(script);
		this.addExecutedCode(script);

		// add the metadata
		// update the metadata to include this new column
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		if(includeT) {
			metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.TIMESTAMP.toString());
		} else {
			metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.DATE.toString());
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"TimestampData", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		return retNoun;
	}

}
