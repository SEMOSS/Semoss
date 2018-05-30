package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Set;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseConceptsReactors extends AbstractReactor {
	
	public DatabaseConceptsReactors() {
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
		
		List<String> appIds = MasterDatabaseUtility.getEngineIdsForAlias(engineId);
		if(appIds.size() == 1) {
			// actually received an app name
			engineId = appIds.get(0);
		} else if(appIds.size() > 1) {
			throw new IllegalArgumentException("There are 2 databases with the name " + engineId + ". Please pass in the correct id to know which source you want to load from");
		}
		
		Set<String> conceptsWithinEngineList = MasterDatabaseUtility.getConceptsWithinEngineRDBMS(engineId);
		return new NounMetadata(conceptsWithinEngineList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_CONCEPTS);
	}

}
