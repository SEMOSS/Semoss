package prerna.sablecc2.reactor.app.metaeditor;

import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetOwlDescriptionsReactor extends AbstractMetaEditorReactor {

	public GetOwlDescriptionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String appId = getAppId();
		// we may have an alias
		appId = getAppId(appId, true);
		
		String concept = getConcept();
		String prop = getProperty();
		
		IEngine engine = Utility.getEngine(appId);
		String physicalUri = null;
		if(prop == null || prop.isEmpty()) {
			physicalUri = engine.getConceptPhysicalUriFromConceptualUri(concept);
		} else {
			physicalUri = engine.getPropertyPhysicalUriFromConceptualUri(prop, concept);
		}
		
		Set<String> descriptions = engine.getDescriptions(physicalUri);
		
		NounMetadata noun = new NounMetadata(descriptions, PixelDataType.CONST_STRING, PixelOperationType.ENTITY_DESCRIPTIONS);
		return noun;
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
	
	private String getProperty() {
		GenRowStruct grs = this.store.getNoun(keysToGet[2]);
		if (grs != null && !grs.isEmpty()) {
			String prop = (String) grs.get(0);
			if (prop != null && !prop.isEmpty()) {
				return prop;
			}
		}

		return "";
	}
}
