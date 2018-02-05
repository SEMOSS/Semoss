package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetLogicalNamesReactor extends AbstractReactor {
	
	public GetLogicalNamesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.CONCEPT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String engineName = getEngineName();
		String concept = getConcept();
		List<String> logicalNames = MasterDatabaseUtility.getLogicalNames(engineName, concept);
		return new NounMetadata(logicalNames, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.LOGICAL_NAMES);
	}

	private String getEngineName() {
		GenRowStruct instanceGrs = this.store.getNoun(keysToGet[0]);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			String engine = (String) instanceGrs.get(0);
			if (engine.length() > 0) {
				return engine;
			}
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[0]);
	}

	private String getConcept() {
		GenRowStruct instanceGrs = this.store.getNoun(keysToGet[1]);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			String concept = (String) instanceGrs.get(0);
			if (concept.length() > 0) {
				return concept;
			}
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[1]);
	}

}
