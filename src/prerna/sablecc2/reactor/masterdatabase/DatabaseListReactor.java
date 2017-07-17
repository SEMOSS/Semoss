package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> databaseList = MasterDatabaseUtility.getAllEnginesRDBMS();
		return new NounMetadata(databaseList, PkslDataTypes.CUSTOM_DATA_STRUCTURE, PkslOperationTypes.DATABASE_LIST);
	}

}
