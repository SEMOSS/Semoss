package prerna.sablecc2.reactor.framemeta;

import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetFrameHeaderMetadataReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame dm = (ITableDataFrame) this.insight.getDataMaker();
		// the frame meta contains the info we want to send to the FE
		List<Map<String, Object>> headersObj = dm.getTableHeaderObjects();
		NounMetadata noun = new NounMetadata(headersObj, PkslDataTypes.CUSTOM_DATA_STRUCTURE, PkslOperationTypes.FRAME_HEADERS);
		return noun;
	}

}
