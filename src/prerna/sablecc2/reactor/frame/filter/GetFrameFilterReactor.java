package prerna.sablecc2.reactor.frame.filter;

import java.util.ArrayList;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetFrameFilterReactor extends AbstractFilterReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		GenRowFilters filters = null;
		if (frame != null) {
			filters = frame.getFrameFilters();
		} else {
			throw new IllegalArgumentException("No frame currently exists within the insight");
		}
		if (filters == null) {
			// just return an empty l ist
			return new NounMetadata(new ArrayList<Object>(), PixelDataType.FILTER, PixelOperationType.FRAME_FILTER);
		}
		return new NounMetadata(filters.getFormatedFilters(), PixelDataType.FILTER, PixelOperationType.FRAME_FILTER);
	}

}
