package prerna.sablecc2.reactor.frame;

import java.util.HashMap;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class InsightMetamodelReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		
		// this is for you PK -> DEFENSIVE PROGRAMMING! 
		Map<String, Object> metamodel = null;
		if(frame == null) {
			metamodel = new HashMap<String, Object>();
		} else {
			metamodel = frame.getMetaData().getMetamodel();
		}
		
		NounMetadata noun = new NounMetadata(metamodel, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_METAMODEL);
		return noun;
	}
}
