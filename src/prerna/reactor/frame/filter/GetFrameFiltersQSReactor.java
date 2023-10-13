package prerna.reactor.frame.filter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetFrameFiltersQSReactor extends AbstractFilterReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		GenRowFilters filters = null;
		if (frame != null) {
			filters = frame.getFrameFilters();
		} else {
			throw new IllegalArgumentException("No frame currently exists within the insight");
		}
		SelectQueryStruct tempQs = new SelectQueryStruct();
		tempQs.setExplicitFilters(filters);
		return new NounMetadata(tempQs, PixelDataType.QUERY_STRUCT, PixelOperationType.FRAME_FILTER);
	}

}
