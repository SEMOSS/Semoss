package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Vector;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class AddLogicalNameReactor extends AbstractReactor {

	public AddLogicalNameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.LOGICAL_NAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String engineName = getEngineName();
		String concept = getConcept();
		List<String> logicalNames = getLogicalNames();
		boolean success = false;
		for(String name: logicalNames) {
			success = MasterDatabaseUtility.addLogicalName(engineName, concept, name);
		}
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}
	
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////// GRAB INPUTS FROM PIXEL REACTOR //////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////

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

	private List<String> getLogicalNames() {
		Vector<String> logicalNames = new Vector<String>();
		GenRowStruct instanceGrs = this.store.getNoun(keysToGet[2]);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			for (int i = 0; i < instanceGrs.size(); i++) {
				String name = (String) instanceGrs.get(i);
				if (name.length() > 0) {
					logicalNames.add(name);
				}
			}
		}
		return logicalNames;
	}
}
