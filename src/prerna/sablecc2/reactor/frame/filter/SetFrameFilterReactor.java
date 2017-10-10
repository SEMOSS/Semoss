package prerna.sablecc2.reactor.frame.filter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.GenRowFilters;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class SetFrameFilterReactor extends AbstractFilterReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		
		// get the filters
		GenRowFilters grf = getFilters();
		if(grf.isEmpty()) {
			throw new IllegalArgumentException("No filter found to set to frame");
		}
		frame.setFilter(grf);
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.FRAME_FILTER);
		return noun;
	}
}
