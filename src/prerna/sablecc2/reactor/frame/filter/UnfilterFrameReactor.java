package prerna.sablecc2.reactor.frame.filter;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.FilterBooleanVal;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UnfilterFrameReactor extends AbstractFilterReactor {

	public UnfilterFrameReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		boolean foundFilter = false;
		List<Object> colsToUnfilter = null;
		if (this.curRow.size() > 0) {
			colsToUnfilter = this.curRow.getValuesOfType(PixelDataType.COLUMN);
		}

		if (colsToUnfilter == null || colsToUnfilter.isEmpty()) {
			foundFilter = frame.unfilter();
		} else {
			for (Object col : colsToUnfilter) {
				QueryColumnSelector cSelector = new QueryColumnSelector(col + "");
				boolean isValidFilter = frame.unfilter(cSelector.getAlias());
				if (isValidFilter) {
					foundFilter = true;
				}
			}
		}

		FilterBooleanVal fFilterVal = FilterBooleanVal.getFrameFilter();
		fFilterVal.setName(frame.getName());
		fFilterVal.setFilterVal(foundFilter);
		NounMetadata noun = new NounMetadata(fFilterVal, PixelDataType.FILTER_BOOLEAN_VAL, PixelOperationType.FRAME_FILTER_CHANGE);
		return noun;
	}
}
