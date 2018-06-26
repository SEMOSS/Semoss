package prerna.solr.reactor;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class MyAppsReactor extends AbstractReactor {

	public MyAppsReactor() {

	}

	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> appInfo = new Vector<Map<String, Object>>();

		if(this.securityEnabled()) {
			appInfo = SecurityQueryUtils.getUserDatabaseList(this.insight.getUserId());
		} else {
			appInfo = SecurityQueryUtils.getAllDatabaseList();
		}
		
		// now we want to add most exeucted insights
		for(Map<String, Object> app : appInfo) {
			String appId = app.get("app_id").toString();
			app.putAll(SecurityQueryUtils.getTopExecutedInsightsForEngine(appId, 10));
		}
		
		return new NounMetadata(appInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INFO);
	}

}
