package prerna.reactor.masterdatabase;

import java.util.Collection;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetDatabaseSelectorsReactor extends AbstractReactor {
	
	public GetDatabaseSelectorsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		GenRowStruct eGrs = this.store.getNoun(keysToGet[0]);
		if(eGrs == null) {
			throw new IllegalArgumentException("Need to define the database to get the concepts from");
		}
		if(eGrs.size() > 1) {
			throw new IllegalArgumentException("Can only define one database within this call");
		}
		String engineId = eGrs.get(0).toString();
		engineId = MasterDatabaseUtility.testDatabaseIdIfAlias(engineId);
		
		Collection<String> conceptsWithinEngineList = MasterDatabaseUtility.getSelectorsWithinDatabaseRDBMS(engineId);
		return new NounMetadata(conceptsWithinEngineList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_PIXEL_SELECTORS);
	}

}