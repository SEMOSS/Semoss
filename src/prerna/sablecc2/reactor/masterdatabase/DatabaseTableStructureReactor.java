package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseTableStructureReactor extends AbstractReactor {
	
	public DatabaseTableStructureReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null) {
			throw new IllegalArgumentException("Need to define the database to get the structure from from");
		}
		
		List<String> appIds = MasterDatabaseUtility.getEngineIdsForAlias(engineId);
		if(appIds.size() == 1) {
			// actually received an app name
			engineId = appIds.get(0);
		} else if(appIds.size() > 1) {
			throw new IllegalArgumentException("There are 2 databases with the name " + engineId + ". Please pass in the correct id to know which source you want to load from");
		}
		
		List<Object[]> data = MasterDatabaseUtility.getAllTablesAndColumns(engineId);
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TABLE_STRUCTURE);
	}
}
