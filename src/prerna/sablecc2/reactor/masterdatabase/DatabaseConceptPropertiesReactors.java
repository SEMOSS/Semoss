package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseConceptPropertiesReactors extends AbstractReactor {
	
	public DatabaseConceptPropertiesReactors() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONCEPTS.getKey(), ReactorKeysEnum.DATABASE.getKey()};
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
		List<String> eFilters = null;
		GenRowStruct engineFilterGrs = this.store.getNoun(keysToGet[1]);
		if(engineFilterGrs != null) {
			eFilters = new Vector<String>();
			String engineFilter = engineFilterGrs.get(0).toString();
			if(AbstractSecurityUtils.securityEnabled()) {
				engineFilter = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineFilter);
			} else {
				engineFilter = MasterDatabaseUtility.testEngineIdIfAlias(engineFilter);
			}
			eFilters.add(engineFilter);
		}
		
		if(AbstractSecurityUtils.securityEnabled()) {
			List<String> appFilters = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
			if(eFilters != null) {
				if(!appFilters.contains(eFilters.get(0))) {
					throw new IllegalArgumentException("Databases " + eFilters.get(0) + " does not exist or user does not have access");
				}
			} else {
				eFilters = new Vector<String>();
				eFilters.addAll(appFilters);
			}
		}
		
		Map<String, Object[]> conceptProperties = MasterDatabaseUtility.getConceptPropertiesRDBMS(conceptLogicals, eFilters);
		return new NounMetadata(conceptProperties, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_CONCEPT_PROPERTIES);
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The optional engine filter";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
