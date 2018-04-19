package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseSpecificConceptPropertiesReactor extends AbstractReactor {

	public static final String COLUMN_FILTER = "columnFilter";

	public DatabaseSpecificConceptPropertiesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.DATABASE.getKey(), COLUMN_FILTER };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		GenRowStruct conceptNamesGrs = this.store.getNoun(keysToGet[0]);
		if (conceptNamesGrs == null) {
			throw new IllegalArgumentException("Need to define the concepts to find relations");
		}
		String conceptLogicals = conceptNamesGrs.get(0).toString();

		// account for optional engine filter
		GenRowStruct engineFilterGrs = this.store.getNoun(keysToGet[1]);
		String engineFilter = null;
		if (engineFilterGrs != null) {
			engineFilter = engineFilterGrs.get(0).toString();
		}

		Map<String, List<String>> conceptProperties = MasterDatabaseUtility.getSpecificConceptPropertiesRDBMS(conceptLogicals, engineFilter);
		List<String> values = conceptProperties.get(engineFilter);
		// TODO: not working yet
		String filterCol = "";
		GenRowStruct filterColGrs = this.store.getNoun(keysToGet[2]);
		if (filterColGrs != null) {
			filterCol = filterColGrs.get(0).toString();
		}
		values.remove(filterCol);
		return new NounMetadata(values, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_CONCEPT_PROPERTIES);
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The optional engine filter";
		} else if (key.equals(COLUMN_FILTER)) {
			return "The optional column filter";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
