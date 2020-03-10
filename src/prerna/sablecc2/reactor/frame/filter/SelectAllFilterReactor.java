package prerna.sablecc2.reactor.frame.filter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SelectAllFilterReactor extends AbstractFilterReactor {

	public SelectAllFilterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ALL.getKey(), ReactorKeysEnum.COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		boolean selectAll = false;
		ITableDataFrame frame = getFrame();
		// get select all
		GenRowStruct grs = store.getNoun(this.keysToGet[0]);
		if (grs == null || grs.isEmpty()) {
		} else {
			selectAll = (boolean) grs.get(0);
		}
		// get column
		String column = this.keyValue.get(this.keysToGet[1]);
		frame.setSelectAllFilter(column, selectAll);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.FRAME_FILTER);
		return noun;
	}

}