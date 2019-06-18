package prerna.sablecc2.reactor.frame.filter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddFrameFilterReactor extends AbstractFilterReactor {

	public AddFrameFilterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTERS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();

		// get the filters
		GenRowFilters grf = getFilters();
		if (grf.isEmpty()) {
			throw new IllegalArgumentException("No filter found to add to frame");
		}
		frame.addFilter(grf);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.FRAME_FILTER);
		return noun;
	}
}
