package prerna.sablecc2.reactor.app.metaeditor;

import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddOwlLogicalNamesReactor extends AbstractMetaEditorReactor {

	public AddOwlLogicalNamesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.LOGICAL_NAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String appId = getAppId();
		// we may have an alias
		appId = getAppId(appId);
		
		String concept = getConcept();
		List<String> logicalNames = getLogicalNames();
		
		IEngine engine = Utility.getEngine(appId);
		RDFFileSesameEngine owlEng = engine.getBaseDataEngine();
		
		String uri = engine.getPhysicalUriFromConceptualUri(concept);
//		owlEng.addStatement(args);
		return null;
	}

	
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////// GRAB INPUTS FROM PIXEL REACTOR //////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////

	private String getAppId() {
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			String appId = (String) grs.get(0);
			if (appId != null && !appId.isEmpty()) {
				return appId;
			}
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[0]);
	}

	private String getConcept() {
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			String concept = (String) grs.get(0);
			if (concept != null && !concept.isEmpty()) {
				return concept;
			}
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[1]);
	}

	private List<String> getLogicalNames() {
		Vector<String> logicalNames = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(keysToGet[2]);
		if (grs != null && !grs.isEmpty()) {
			for (int i = 0; i < grs.size(); i++) {
				String name = (String) grs.get(i);
				if (name != null && !name.isEmpty()) {
					logicalNames.add(name);
				}
			}
		}
		return logicalNames;
	}
}
