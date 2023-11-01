package prerna.auth.utils.reactors.admin;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetEngineMarkdownReactor extends AbstractReactor {
	
	public AdminGetEngineMarkdownReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null) {
			throw new IllegalArgumentException("Need to define the engine");
		}
		
		String databaseMarkdown = adminUtils.getEngineMarkdown(engineId);
		return new NounMetadata(databaseMarkdown, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);
	}

}
