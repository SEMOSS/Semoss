package prerna.reactor.frame.r;

import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class EncodeColumnReactor extends AbstractRFrameReactor {

	public EncodeColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// init R
		init();

		String[] packages = { "digest" };
		this.rJavaTranslator.checkPackages(packages);

		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		List<String> columns = getColumns();
		if(columns == null || columns.isEmpty()) {
			throw new IllegalArgumentException("Need to pass in the columns to encode");
		}

		StringBuilder script = new StringBuilder();
		script.append("library(digest);encode <- function(value) digest(value, algo=\"sha256\");");

		for(String col : columns) {
			String select = frameName + "$" + col;
			script.append(select).append(" <- sapply(").append(select).append(", encode);");
		}

		this.rJavaTranslator.executeEmptyR(script.toString());
		this.addExecutedCode(script.toString());

		// upon successful execution
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		for(String col : columns) {
			// set the type for all the columns to be string
			metadata.modifyDataTypeToProperty(frameName + "__" + col, frameName, SemossDataType.STRING.toString());
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"EncodeColumn", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		return noun;
	}

	private List<String> getColumns() {
		// EncodeColumn(columns=["a","b","c"]);
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		// this is if passed in directly EncodeColumn("a","b","c");
		return this.curRow.getAllStrValues();
	}

}
