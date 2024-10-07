package prerna.reactor.prompt;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityPromptUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPromptMetaValuesReactor extends AbstractReactor {
	
	 public GetPromptMetaValuesReactor() {
	        this.keysToGet = new String[] {ReactorKeysEnum.META_KEYS.getKey()};
	    }

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}
        List<Map<String, Object>> ret = SecurityPromptUtils.getAvailableMetaValues(getListValues(ReactorKeysEnum.META_KEYS.getKey()) );
        return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private List<String> getListValues(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return this.curRow.getAllStrValues();
	}
}
