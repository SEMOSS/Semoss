package prerna.solr.reactor;

import java.util.ArrayList;
import java.util.List;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SetAppDescriptionReactor extends AbstractReactor {
	
	public static final String DESCRIPTIONS = "description";

	public SetAppDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), DESCRIPTIONS};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
		}
		
		if(!SecurityQueryUtils.getEngineIds().contains(appId)) {
			throw new IllegalArgumentException("App id does not exist");
		}
		
		String descriptions = this.keyValue.get(this.keysToGet[1]);
		List<String> descList = new ArrayList<String>();
		descList.add(descriptions);
		SecurityUpdateUtils.setEngineMeta(appId, "description", descList);

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.APP_INFO);
	}
}
