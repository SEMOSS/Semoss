package prerna.sablecc2.reactor.insights.save;

import java.util.HashSet;
import java.util.Set;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class DeleteInsightReactor extends AbstractReactor {

	public DeleteInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		
		int size = grs.size();
		Set<String> appIds = new HashSet<>();
		for (int i = 0; i < size; i++) {
			// id is passed in from solr id where it is defined as engine_id
			// so I need to split it
			String id = grs.get(i).toString();
			String[] split = id.split("_");
			String appId = split[0];
			String insightId = split[1];
			
			if(AbstractSecurityUtils.securityEnabled()) {
				if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, insightId)) {
					throw new IllegalArgumentException("User does not have permission to edit this insight");
				}
			}
			
			IEngine engine = Utility.getEngine(appId);
			// delete from insights database
			InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
			try {
				admin.dropInsight(insightId);
			} catch (RuntimeException e) {
				e.printStackTrace();
			}

			SecurityUpdateUtils.deleteInsight(appId, insightId);
			appIds.add(appId);
		}
		
		ClusterUtil.reactorPushApp(appIds);
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_INSIGHT);
	}

}
