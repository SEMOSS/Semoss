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
		String engineName = this.keyValue.get(this.keysToGet[0]);
		if(engineName == null) {
			throw new IllegalArgumentException("Need to define the database to get the structure from from");
		}
		List<Object[]> data = MasterDatabaseUtility.getAllTablesAndColumns(engineName);
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TABLE_STRUCTURE);
	}
}
