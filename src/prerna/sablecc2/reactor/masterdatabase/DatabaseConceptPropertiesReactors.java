package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseConceptPropertiesReactors extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		GenRowStruct conceptNamesGrs = this.store.getNoun("concepts");
		if(conceptNamesGrs == null) {
			throw new IllegalArgumentException("Need to define the concepts to find relations");
		}
		List<String> conceptLogicals = new Vector<String>();
		int size = conceptNamesGrs.size();
		for(int i = 0; i < size; i++) {
			conceptLogicals.add(conceptNamesGrs.get(i).toString());
		}
		
		// account for optional engine filter
		GenRowStruct engineFilterGrs = this.store.getNoun("engine");
		String engineFilter = null;
		if(engineFilterGrs != null) {
			engineFilter = engineFilterGrs.get(0).toString();
		}
		
		Map<String, Object[]> conceptProperties = MasterDatabaseUtility.getConceptProperties(conceptLogicals, engineFilter);
		return new NounMetadata(conceptProperties, PkslDataTypes.CUSTOM_DATA_STRUCTURE, PkslOperationTypes.DATABASE_CONCEPT_PROPERTIES);
	}

}
