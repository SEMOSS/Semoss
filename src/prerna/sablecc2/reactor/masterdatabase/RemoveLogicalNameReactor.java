package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Vector;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class RemoveLogicalNameReactor extends AbstractReactor {
	private static final String ENGINE_KEY = "engine";
	private static final String CONCEPT_KEY = "concept";
	private static final String LOGICAL_NAME_KEY = "logicalName";

	@Override
	public NounMetadata execute() {
		String engineName = getEngineName();
		String concept = getConcept();
		List<String> logicalNames = getLogicalNames();
		boolean success = false;
		for (String name : logicalNames) {
			success = MasterDatabaseUtility.removeLogicalName(engineName, concept, name);
		}
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

	private List<String> getLogicalNames() {
		Vector<String> logicalNames = new Vector<String>();
		GenRowStruct instanceGrs = this.store.getNoun(LOGICAL_NAME_KEY);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			for (int i = 0; i < instanceGrs.size(); i++) {
				String name = (String) instanceGrs.get(0);
				if (name.length() > 0) {
					logicalNames.add(name);
				}
			}
		}
		return logicalNames;
	}

}
