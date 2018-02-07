package prerna.sablecc2.reactor.masterdatabase;

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
			throw new IllegalArgumentException("Need to define the engine to get the concepts from");
		}
		if(eGrs.size() > 1) {
			throw new IllegalArgumentException("Can only define one engine within this call");
		}
		String engineName = eGrs.get(0).toString();
		Set<String> conceptsWithinEngineList = MasterDatabaseUtility.getConceptsWithinEngineRDBMS(engineName);
		return new NounMetadata(conceptsWithinEngineList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_CONCEPTS);
	}

}
