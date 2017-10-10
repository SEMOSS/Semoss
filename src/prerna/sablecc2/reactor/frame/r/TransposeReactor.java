package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class TransposeReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {

		// get frame
		//use init to initialize rJavaTranslator object that will be used later
		init();
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		//make sure frame is not null
		if (frame != null) {
			//get table name
			String table = frame.getTableName();
			//define r script string to be executed
			String script = table + " <- " + table + "[, data.table(t(.SD), keep.rownames=TRUE)]";
			//execute the r script
			frame.executeRScript(script);
			//with transpose - the column data is changing so we must reacreate metadata
			recreateMetadata(table);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
