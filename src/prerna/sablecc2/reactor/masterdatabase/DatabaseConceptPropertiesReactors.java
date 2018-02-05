package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseConceptPropertiesReactors extends AbstractReactor {
	
	public DatabaseConceptPropertiesReactors() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONCEPTS.getKey(), ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		GenRowStruct conceptNamesGrs = this.store.getNoun(keysToGet[0]);
		if(conceptNamesGrs == null) {
			throw new IllegalArgumentException("Need to define the concepts to find relations");
		}
		List<String> conceptLogicals = new Vector<String>();
		int size = conceptNamesGrs.size();
		for(int i = 0; i < size; i++) {
			conceptLogicals.add(conceptNamesGrs.get(i).toString());
		}
		
		// account for optional engine filter
		GenRowStruct engineFilterGrs = this.store.getNoun(keysToGet[1]);
		String engineFilter = null;
		if(engineFilterGrs != null) {
			engineFilter = engineFilterGrs.get(0).toString();
		}
		
		Map<String, Object[]> conceptProperties = MasterDatabaseUtility.getConceptPropertiesRDBMS(conceptLogicals, engineFilter);
		return new NounMetadata(conceptProperties, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_CONCEPT_PROPERTIES);
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The optional engine filter";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
