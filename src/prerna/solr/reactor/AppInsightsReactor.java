package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class AppInsightsReactor extends AbstractReactor {
	
	public AppInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), 
				ReactorKeysEnum.FILTER_WORD.getKey(), 
				ReactorKeysEnum.LIMIT.getKey(), 
				ReactorKeysEnum.OFFSET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		String searchTerm = this.keyValue.get(this.keysToGet[1]);
		String limit = this.keyValue.get(this.keysToGet[2]);
		String offset = this.keyValue.get(this.keysToGet[3]);
		
		List<Map<String, Object>> results = null;
		if(appId != null) {
			if(AbstractSecurityUtils.securityEnabled()) {
				if(SecurityQueryUtils.userCanViewEngine(this.insight.getUser(), appId)) {
					results = SecurityQueryUtils.searchUserInsights(this.insight.getUser(), appId, searchTerm, limit, offset);
				} else {
					throw new IllegalArgumentException("App does not exist or user does not have access to database");
				}
			} else {
				results = SecurityQueryUtils.searchInsights(appId, searchTerm, limit, offset);
			}
		} else {
			if(AbstractSecurityUtils.securityEnabled()) {
				results = SecurityQueryUtils.searchUserInsightDataByName(this.insight.getUser(), searchTerm, limit, offset);
			} else {
				results = SecurityQueryUtils.searchAllInsightDataByName(searchTerm, limit, offset);
			}
		}
		
		return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INSIGHTS);
	}
	
}
