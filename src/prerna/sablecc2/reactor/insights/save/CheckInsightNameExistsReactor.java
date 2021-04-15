package prerna.sablecc2.reactor.insights.save;

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
		
		boolean insightNameExists = SecurityInsightUtils.insightNameExists(appId, insightName);
		return new NounMetadata(insightNameExists, PixelDataType.BOOLEAN);
	}
	
}
