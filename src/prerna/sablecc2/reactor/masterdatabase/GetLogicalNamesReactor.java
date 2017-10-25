package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetLogicalNamesReactor extends AbstractReactor {
	private static final String ENGINE_KEY = "engine";
	private static final String CONCEPT_KEY = "concept";

	@Override
	public NounMetadata execute() {
		String engineName = getEngineName();
		String concept = getConcept();
		List<String> logicalNames = MasterDatabaseUtility.getLogicalNames(engineName, concept);
		return new NounMetadata(logicalNames, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.LOGICAL_NAMES);
	}

	private String getEngineName() {
		GenRowStruct instanceGrs = this.store.getNoun(ENGINE_KEY);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			String engine = (String) instanceGrs.get(0);
			if (engine.length() > 0) {
				return engine;
			}
		}
		throw new IllegalArgumentException("Need to define " + ENGINE_KEY);
	}

	private String getConcept() {
		GenRowStruct instanceGrs = this.store.getNoun(CONCEPT_KEY);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			String concept = (String) instanceGrs.get(0);
			if (concept.length() > 0) {
				return concept;
			}
		}
		throw new IllegalArgumentException("Need to define " + CONCEPT_KEY);
	}

}
