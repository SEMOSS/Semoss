package prerna.sablecc2.reactor.frame.filter;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class UnfilterFrameReactor extends AbstractReactor {
	
	public UnfilterFrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		boolean foundFilter = false;
		List<Object> colsToUnfilter = null;
		if(this.curRow.size() > 0) {
			colsToUnfilter = this.curRow.getValuesOfType(PixelDataType.COLUMN);
		}
		
		if(colsToUnfilter == null || colsToUnfilter.isEmpty()) {
			foundFilter = frame.unfilter();
		} else {
			for(Object col : colsToUnfilter) {
				boolean isValidFilter = frame.unfilter(col + "");
				if(isValidFilter) {
					foundFilter = true;
				}
			}
		}
		
		NounMetadata noun = new NounMetadata(foundFilter, PixelDataType.BOOLEAN, PixelOperationType.FRAME_FILTER);
		return noun;
	}
}
