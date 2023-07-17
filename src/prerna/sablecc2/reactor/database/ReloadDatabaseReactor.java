package prerna.sablecc2.reactor.database;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class ReloadDatabaseReactor extends AbstractReactor {

	public ReloadDatabaseReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		// make sure user has at least edit access
		if (AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
				if (!SecurityEngineUtils.userCanEditDatabase(this.insight.getUser(), databaseId)) {
					throw new IllegalArgumentException("User does not have permission to reload the database");
				}
			}
		}
		
		IEngine database = Utility.getEngine(databaseId);
		String propFile = database.getPropFile();
		database.closeDB();
		database.setProp(null);
		try {
			database.openDB(propFile);
		} catch (Exception e1) {
			throw new IllegalArgumentException("An error occurred reloading the database. Please reach out to an administrator for assistance");
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
