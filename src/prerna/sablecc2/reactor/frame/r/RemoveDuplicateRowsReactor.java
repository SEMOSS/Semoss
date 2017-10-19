package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class RemoveDuplicateRowsReactor extends AbstractRFrameReactor {

	/**
	 * This reactor removes duplicate rows from the data frame
	 * There are no inputs needed
	 */

	@Override
	public NounMetadata execute() {
		// initialize rJavaTranslator
		init();
		
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		
		//get table name
		String table = frame.getTableName();
		
		//define the r script to be executed
		String script = table + " <- unique(" + table + ")";
		
		//execute the r script
		frame.executeRScript(script);
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
