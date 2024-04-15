package prerna.reactor.database.physicaleditor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class GetDatabaseTablePhysicalStructureReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetDatabaseTablePhysicalStructureReactor.class);

	public GetDatabaseTablePhysicalStructureReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TABLE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String databaseId = this.keyValue.get(this.keysToGet[0]);
		String table = this.keyValue.get(this.keysToGet[1]);
		
		if(table == null || (table=table.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in " + ReactorKeysEnum.TABLE.getKey() + " to pull the schema from");
		}
		
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database" + databaseId + " does not exist or user does not have access to database");
		}
		
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if(!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("This operation only works on relational databases");
		}
		IRDBMSEngine rdbms = (IRDBMSEngine) engine;
		AbstractSqlQueryUtil queryUtil = rdbms.getQueryUtil();
		Connection con = null;
		try {
			con = rdbms.getConnection();
			// the final map
			LinkedHashMap<String, String> columnDetails = queryUtil.getAllTableColumnTypesSimple(con, table, rdbms.getDatabase(), rdbms.getSchema());
			return new NounMetadata(columnDetails, PixelDataType.MAP);
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occured getting the physical table structure. Detailed message = " + e.getMessage());
		} finally {
			if(rdbms.isConnectionPooling()) {
				ConnectionUtils.closeConnection(con);
			}
		}
	}
}
