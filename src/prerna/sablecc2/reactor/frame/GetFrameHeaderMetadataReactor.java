package prerna.sablecc2.reactor.frame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetFrameHeaderMetadataReactor extends AbstractReactor {

	private static final String ALL_NUMERIC_KEY = "allNumeric";
	
	@Override
	public NounMetadata execute() {
		ITableDataFrame dm = (ITableDataFrame) this.insight.getDataMaker();
		if(dm == null) {
			NounMetadata noun = new NounMetadata(new HashMap<String, Object>(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS);
			return noun;
		}
		// the frame meta contains the info we want to send to the FE
		boolean onlyNumeric = returnOnlyNumeric();
		Map<String, Object> headersObj = dm.getMetaData().getTableHeaderObjects(onlyNumeric);
		NounMetadata noun = new NounMetadata(headersObj, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS);
		return noun;
	}
	
	private boolean returnOnlyNumeric() {
		GenRowStruct numGrs = this.store.getNoun(ALL_NUMERIC_KEY);
		if(numGrs != null) {
			if(numGrs.size() > 0) {
				List<Object> val = numGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}

}
