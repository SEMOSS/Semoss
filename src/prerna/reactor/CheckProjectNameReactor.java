package prerna.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckProjectNameReactor extends AbstractReactor {

	public CheckProjectNameReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String checkName = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("exists", AbstractSecurityUtils.containsProjectName(checkName));
		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
