package prerna.reactor.database.upload.rdbms.external;

import java.util.Map;

import prerna.reactor.database.upload.rdbms.CreateNewRdbmsDatabaseReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.upload.UploadInputUtility;

public class CreateNewRdbmsExternalDatabaseReactor extends CreateNewRdbmsDatabaseReactor {

	public CreateNewRdbmsExternalDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONNECTION_DETAILS.getKey(), UploadInputUtility.DATABASE,
				UploadInputUtility.METAMODEL_ADDITIONS };
	}
	
	@Override
	public NounMetadata execute() {
		this.internal = false;
		return doExecute();
	}

	@Override
	protected Map<String, Object> editConnectionDetails(Map<String, Object> connectionDetails, RdbmsTypeEnum driverEnum) {
		return connectionDetails;
	}
	
}
	

