package prerna.solr.reactor;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.date.SemossDate;
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

		if(AbstractSecurityUtils.securityEnabled()) {
			appInfo = SecurityQueryUtils.getUserDatabaseList(this.insight.getUser());
		} else {
			appInfo = SecurityQueryUtils.getAllDatabaseList();
		}
		
		// now we want to add most exeucted insights
		for(Map<String, Object> app : appInfo) {
			String appId = app.get("app_id").toString();
//			app.putAll(SecurityQueryUtils.getTopExecutedInsightsForEngine(appId, 10));
			SemossDate lmDate = SecurityQueryUtils.getLastExecutedInsightInApp(appId);
			// could be null when there are no insights in an app
			if(lmDate != null) {
				app.put("lastModified", lmDate.getFormattedDate());
			}
		}
		
		return new NounMetadata(appInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INFO);
	}

}
