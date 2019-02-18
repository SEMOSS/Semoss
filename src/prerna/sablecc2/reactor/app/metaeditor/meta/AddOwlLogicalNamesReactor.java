package prerna.sablecc2.reactor.app.metaeditor.meta;

import java.io.IOException;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.OWLER;
import prerna.util.Utility;

public class AddOwlLogicalNamesReactor extends AbstractMetaEditorReactor {

	public AddOwlLogicalNamesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.LOGICAL_NAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String appId = getAppId();
		// we may have an alias
		appId = getAppId(appId, true);
		
		String concept = getConcept();
		String prop = getProperty();
		String[] logicalNames = getLogicalNames();
		
		OWLER owler = getOWLER(appId);
		// set all the existing values into the OWLER
		// so that its state is updated
		IEngine engine = Utility.getEngine(appId);
		setOwlerValues(engine, owler);
		if(prop == null || prop.isEmpty()) {
			String physicalUri = engine.getConceptPhysicalUriFromConceptualUri(concept);
			owler.addConceptLogicalNames(concept, Utility.getClassName(physicalUri), logicalNames);
		} else {
			owler.addPropLogicalNames(concept, prop, logicalNames);
		}
		owler.commit();
		
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
//			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add logical names : " + Arrays.toString(logicalNames), 
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add logical names", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
//		noun.addAdditionalReturn(new NounMetadata("Successfully added logical names : " + Arrays.toString(logicalNames), 
		noun.addAdditionalReturn(new NounMetadata("Successfully added logical names", 
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

	private String[] getLogicalNames() {
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
