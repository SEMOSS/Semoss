package prerna.sablecc2.reactor.algorithms.xray;

import java.util.HashMap;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetXrayConfigListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		HashMap<String, Object> configMap = MasterDatabaseUtility.getXrayConfigList();
		return new NounMetadata(configMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}
}
