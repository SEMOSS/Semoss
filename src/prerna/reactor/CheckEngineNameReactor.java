package prerna.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckEngineNameReactor extends AbstractReactor {

	public CheckEngineNameReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String checkName = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("exists", AbstractSecurityUtils.containsEngineName(checkName));
		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
