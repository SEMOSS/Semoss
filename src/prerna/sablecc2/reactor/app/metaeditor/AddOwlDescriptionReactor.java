package prerna.sablecc2.reactor.app.metaeditor;

import java.io.IOException;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.OWLER;
import prerna.util.Utility;

public class AddOwlDescriptionReactor extends AbstractMetaEditorReactor {

	public AddOwlDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DESCRIPTION.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String appId = getAppId();
		// we may have an alias
		appId = getAppId(appId, true);
		
		String concept = getConcept();
		String prop = getProperty();
		String[] descriptions = getDescriptions();
		
		OWLER owler = getOWLER(appId);
		// set all the existing values into the OWLER
		// so that its state is updated
		IEngine engine = Utility.getEngine(appId);
		setOwlerValues(engine, owler);
		if(prop == null || prop.isEmpty()) {
			String physicalUri = engine.getPhysicalUriFromConceptualUri("http://semoss.org/ontologies/Concept/" + concept);
			owler.addConceptDescription(concept, Utility.getClassName(physicalUri), descriptions);
		} else {
			owler.addPropDescription(concept, prop, descriptions);
		}
		owler.commit();
		
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
//			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add descriptions : " + Arrays.toString(descriptions),
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add description", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
//		noun.addAdditionalReturn(new NounMetadata("Successfully added descriptions : " + Arrays.toString(descriptions), 
		noun.addAdditionalReturn(new NounMetadata("Successfully added descriptions",
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
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

	private String[] getDescriptions() {
		String[] logicalNames = null;
		GenRowStruct grs = this.store.getNoun(keysToGet[3]);
		if (grs != null && !grs.isEmpty()) {
			logicalNames = new String[grs.size()];
			for (int i = 0; i < grs.size(); i++) {
				String name = (String) grs.get(i);
				if (name != null && !name.isEmpty()) {
					logicalNames[i] = name;
				}
			}
		}
		return logicalNames;
	}
}
