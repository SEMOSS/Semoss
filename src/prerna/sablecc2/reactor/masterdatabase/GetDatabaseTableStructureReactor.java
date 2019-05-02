package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetDatabaseTableStructureReactor extends AbstractReactor {
	
	public GetDatabaseTableStructureReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null) {
			throw new IllegalArgumentException("Need to define the database to get the structure from from");
		}
		engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
		
		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
		List<String> appFilters = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			appFilters = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
			if(!appFilters.contains(engineId)) {
				throw new IllegalArgumentException("Database does not exist or user does not have access to database");
			}
		}

		List<Object[]> data = MasterDatabaseUtility.getAllTablesAndColumns(engineId);
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TABLE_STRUCTURE);
	}
}
