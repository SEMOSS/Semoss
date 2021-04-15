package prerna.sablecc2.reactor.insights.save;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class CheckInsightNameExistsReactor extends AbstractReactor {

	public CheckInsightNameExistsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String appId = this.keyValue.get(this.keysToGet[0]);
		String insightName = this.keyValue.get(this.keysToGet[1]);
		
		if(appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("Must provide an app id");
		}
		
		if(insightName == null || (insightName = insightName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide an insight name");
		}
		
		String existingInsightId = SecurityInsightUtils.insightNameExists(appId, insightName);
		Map<String, Object> retMap = new HashMap<>();
		if(existingInsightId != null) {
			retMap.put("exists", true);
			retMap.put("insightId", existingInsightId);
			boolean canEdit = SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, existingInsightId);
			retMap.put("userCanEdit", canEdit);
		} else {
			retMap.put("exists", false);
		}
		return new NounMetadata(retMap, PixelDataType.MAP);
	}
	
}
