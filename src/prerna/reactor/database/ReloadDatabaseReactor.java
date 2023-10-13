package prerna.reactor.database;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ReloadDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ReloadDatabaseReactor.class);
	
	public ReloadDatabaseReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		// make sure user has at least edit access
		if(!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("User does not have permission to reload the database");
			}
		}
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		String smssFilePath = database.getSmssFilePath();
		try {
			database.close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		database.setSmssProp(null);
		try {
			database.open(smssFilePath);
		} catch (Exception e1) {
			throw new IllegalArgumentException("An error occurred reloading the database. Please reach out to an administrator for assistance");
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
