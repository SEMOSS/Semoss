package prerna.sablecc2.reactor.frame;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class HasDuplicatesReactor extends AbstractReactor {
	
	public HasDuplicatesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame dataframe = (ITableDataFrame) getFrame();
		List<String> frameCols = this.curRow.getAllColumns();
		// if one column passed in is unique throughout the flat structure
		// of the frame, then we are done
		for(int i = 0; i < frameCols.size() ; i++) {
			// if we find one column that is not unique
			// we know that the result of all the col
			Boolean isColUnique = dataframe.isUniqueColumn(frameCols.get(i));
			if(isColUnique) {
				// we have a column that is unique
				// return false, we dont have duplicates
				return new NounMetadata(false, PixelDataType.BOOLEAN);
			}
		}
		// no columns were unique
		// return true, we do have duplicates
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	private ITableDataFrame getFrame() {
		// try specific key
		GenRowStruct frameGrs = this.store.getNoun(keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		return (ITableDataFrame) this.insight.getDataMaker();
	}
}