package prerna.reactor.database;

import java.sql.SQLException;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RdbmsReconnectReactor extends AbstractReactor {

	public RdbmsReconnectReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		// make sure user has at least edit access
		if(!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("User does not have permission to re-establish the connection for this database");
			}
		}
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		if(!(database instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("Database must be an RDBMS native engine");
		}
		
		RDBMSNativeEngine rdbms = (RDBMSNativeEngine) database;
		try {
			if(rdbms.isConnectionPooling()) {
				rdbms.closeDataSource();
			} else {
				rdbms.makeConnection().close();
			}
		} catch (SQLException e) {
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(getError(e.getMessage()));
			return noun;
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
