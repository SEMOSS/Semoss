package prerna.sablecc2.reactor.algorithms.xray;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetXrayConfigListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		HashMap<String, Object> configMap = MasterDatabaseUtility.getXrayConfigList();
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		String xRayConfigList = null;
		try {
			xRayConfigList = ow.writeValueAsString(configMap);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new NounMetadata(xRayConfigList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}
}
