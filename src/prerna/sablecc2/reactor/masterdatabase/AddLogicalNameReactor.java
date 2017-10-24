package prerna.sablecc2.reactor.masterdatabase;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class AddLogicalNameReactor extends AbstractReactor {
	private static final String ENGINE_KEY = "engine";
	private static final String CONCEPT_KEY = "concept";
	private static final String LOGICAL_NAME_KEY = "logicalName";

	@Override
	public NounMetadata execute() {
		String engineName = getEngineName();
		String concept = getConcept();
		String logicalName = getLogicalName();
		boolean success = MasterDatabaseUtility.addLogicalName(engineName, concept, logicalName);
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
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

	private String getLogicalName() {
		GenRowStruct instanceGrs = this.store.getNoun(LOGICAL_NAME_KEY);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			String logicalName = (String) instanceGrs.get(0);
			if (logicalName.length() > 0) {
				return logicalName;
			}
		}
		throw new IllegalArgumentException("Need to define " + LOGICAL_NAME_KEY);
	}

}
