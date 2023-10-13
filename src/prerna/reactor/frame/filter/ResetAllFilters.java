package prerna.reactor.frame.filter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ResetAllFilters extends AbstractFilterReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		GenRowFilters filters = null;
		if (frame != null) {
			frame.setFrameFilters(new GenRowFilters());
		} else {
			throw new IllegalArgumentException("No frame currently exists within the insight");
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
