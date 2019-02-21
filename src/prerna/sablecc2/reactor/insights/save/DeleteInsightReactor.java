package prerna.sablecc2.reactor.insights.save;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class DeleteInsightReactor extends AbstractReactor {

	public DeleteInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		GenRowStruct appGrs = this.store.getNoun(this.keysToGet[0]);
		if(appGrs.isEmpty()) {
			throw new IllegalArgumentException("Must define the app to delete the insights from");
		}
		String appId = appGrs.get(0).toString();
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityQueryUtils.userCanViewEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist or user does not have access to database");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist");
			}
		}
		IEngine engine = Utility.getEngine(appId);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());

		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		int size = grs.size();
		for (int i = 0; i < size; i++) {
			String insightId = grs.get(i).toString();
			if(AbstractSecurityUtils.securityEnabled()) {
				if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, insightId)) {
					throw new IllegalArgumentException("User does not have permission to edit this insight");
				}
			}
			
			// delete from insights database
			try {
				admin.dropInsight(insightId);
			} catch (RuntimeException e) {
				e.printStackTrace();
			}

			SecurityUpdateUtils.deleteInsight(appId, insightId);
		}
		ClusterUtil.reactorPushApp(appId);
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_INSIGHT);
	}

}
