package prerna.solr.reactor;

import java.util.ArrayList;
import java.util.List;

import prerna.auth.SecurityUtils;
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
		String appName = this.keyValue.get(this.keysToGet[0]);
		String descriptions = this.keyValue.get(this.keysToGet[1]);
		List<String> descList = new ArrayList<String>();
		descList.add(descriptions);
		if(this.securityEnabled()) {
			if(this.getUserAppFilters().contains(appName)) {
				SecurityUtils.setEngineMeta(appName, "description", descList);
			} else {
				throw new IllegalArgumentException("App does not exist or user does not have access to database");
			}
		} else {
			SecurityUtils.setEngineMeta(appName, "description", descList);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.APP_INFO);
	}
}
