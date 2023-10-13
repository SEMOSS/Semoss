package prerna.reactor.database.upload.rdbms.internal;

import java.io.File;
import java.util.Map;

import prerna.reactor.database.upload.rdbms.CreateNewRdbmsDatabaseReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.upload.UploadInputUtility;

public class CreateNewRdbmsInternalDatabaseReactor extends CreateNewRdbmsDatabaseReactor {

	public CreateNewRdbmsInternalDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONNECTION_DETAILS.getKey(), UploadInputUtility.DATABASE,
				UploadInputUtility.METAMODEL_ADDITIONS };
	}

	@Override
	public NounMetadata execute() {
		this.internal = true;
		return doExecute();
	}

	@Override
	protected Map<String, Object> editConnectionDetails(Map<String, Object> connectionDetails, RdbmsTypeEnum driverEnum) {
		connectionDetails.put(AbstractSqlQueryUtil.FORCE_FILE, true);
		
		String dbFileName = (String) connectionDetails.get(AbstractSqlQueryUtil.DATABASE);
		connectionDetails.put(Constants.RDBMS_TYPE, driverEnum);
		
		if (dbFileName == null) {
			throw new IllegalArgumentException("Database cannot be null in connection details");
		}
		
		String localHostName = "@BaseFolder@" + File.separator + Constants.DATABASE_FOLDER
				+ File.separator + "@ENGINE@" + File.separator + dbFileName;
		
		if (driverEnum == RdbmsTypeEnum.SQLITE) {
			localHostName += ".sqlite";
		}

		connectionDetails.put(AbstractSqlQueryUtil.HOSTNAME, localHostName);
		
		return connectionDetails;
	}

}
