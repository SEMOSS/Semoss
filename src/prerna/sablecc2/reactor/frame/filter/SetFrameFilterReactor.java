package prerna.sablecc2.reactor.frame.filter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetFrameFilterReactor extends AbstractFilterReactor {

	public SetFrameFilterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTERS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();

		// get the filters
		GenRowFilters grf = getFilters();
		if (grf.isEmpty()) {
			throw new IllegalArgumentException("No filter found to set to frame");
		}
		// check if we are filtering or actually removing a filter
		if(grf.size() == 1) {
			IQueryFilter singleFilter = grf.getFilters().get(0);
			if(singleFilter instanceof SimpleQueryFilter) {
				boolean unfilter = ((SimpleQueryFilter) singleFilter).isEmptyFilterValues();
				if(unfilter) {
					QueryColumnSelector cSelector = singleFilter.getAllQueryColumns().get(0);
					boolean isValidFilter = frame.unfilter(cSelector.getAlias());

					BooleanValMetadata fFilterVal = BooleanValMetadata.getFrameVal();
					fFilterVal.setName(frame.getName());
					fFilterVal.setFilterVal(isValidFilter);
					NounMetadata noun = new NounMetadata(fFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.FRAME_FILTER_CHANGE);
					return noun;
				}
			}
		}
		
		// we got to this point, apply the filter
		frame.setFilter(grf);
		
		BooleanValMetadata fFilterVal = BooleanValMetadata.getFrameVal();
		fFilterVal.setName(frame.getName());
		fFilterVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(fFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.FRAME_FILTER_CHANGE);
		return noun;
	}
}
