package prerna.sablecc2.reactor.app.metaeditor.meta;

import java.io.IOException;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.OWLER;
import prerna.util.Utility;

public class EditOwlDescriptionReactor extends AbstractMetaEditorReactor {

	public EditOwlDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DESCRIPTION.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		// we may have an alias
		appId = getAppId(appId, true);
		
		String concept = this.keyValue.get(this.keysToGet[1]);
		String prop = this.keyValue.get(this.keysToGet[2]);
		String description = this.keyValue.get(this.keysToGet[3]);
		
		IEngine engine = Utility.getEngine(appId);
		String physicalUri = null;
		if(prop == null || prop.isEmpty()) {
			physicalUri = engine.getConceptPhysicalUriFromConceptualUri(concept);
		} else {
			physicalUri = engine.getPropertyPhysicalUriFromConceptualUri(concept, prop);
		}
		
		// get the existing value if present
		String existingDescription = engine.getDescription(physicalUri);
		
		OWLER owler = new OWLER(engine);
		owler.deleteDescription(physicalUri, existingDescription);
		owler.addDescription(physicalUri, description);
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add description", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully added descriptions",
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
