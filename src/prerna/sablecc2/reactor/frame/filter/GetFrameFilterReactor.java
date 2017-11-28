package prerna.sablecc2.reactor.frame.filter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.GenRowFilters;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetFrameFilterReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		GenRowFilters filters = new GenRowFilters();
		if (frame != null) {
			filters = frame.getFrameFilters();
		}
		return new NounMetadata(filters, PixelDataType.FILTER, PixelOperationType.FRAME_FILTER);
	}

}
