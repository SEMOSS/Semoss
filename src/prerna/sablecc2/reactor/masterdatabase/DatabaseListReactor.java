package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> databaseList = MasterDatabaseUtility.getAllEnginesRDBMS();
		return new NounMetadata(databaseList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_LIST);
	}

}
