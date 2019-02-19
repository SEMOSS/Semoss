package prerna.sablecc2.reactor.app.metaeditor.concepts;

import java.util.List;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.OWLER;
import prerna.util.Utility;

public class EditOwlConceptConceptualNameReactor extends AbstractMetaEditorReactor {

	public EditOwlConceptConceptualNameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), CONCEPTUAL_NAME};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String appId = this.keyValue.get(this.keysToGet[0]);
		// perform translation if alias is passed
		// and perform security check
		appId = getAppId(appId, true);

		String concept = this.keyValue.get(this.keysToGet[1]);
		if(concept == null || concept.isEmpty()) {
			throw new IllegalArgumentException("Must define the concept being modified in the app metadata");
		}
		
		String newConceptualName = this.keyValue.get(this.keysToGet[2]);
		if(newConceptualName == null || newConceptualName.isEmpty()) {
			throw new IllegalArgumentException("Must define the new conceptual name");
		}
		// make sure it conforms
		newConceptualName = newConceptualName.trim();
		if(!newConceptualName.matches("^[a-zA-Z0-9-_]+$")) {
			throw new IllegalArgumentException("Conceptual name must contain only letters, numbers, and underscores");
		}
		newConceptualName = newConceptualName.replaceAll("_{2,}", "_");
		
		String newConceptualURI = "http://semoss.org/ontologies/Concept/" + newConceptualName;

		IEngine engine = Utility.getEngine(appId);
		RDFFileSesameEngine owlEngine = engine.getBaseDataEngine();
		
		// make sure this name isn't currently present in the engine
		if(engine.getConceptPhysicalUriFromConceptualUri(newConceptualURI) != null) {
			throw new IllegalArgumentException("This conceptual name already exists");
		}
		
		String conceptualURI = "http://semoss.org/ontologies/Concept/" + concept;
		String conceptPhysicalURI = engine.getConceptPhysicalUriFromConceptualUri(conceptualURI);
		if(conceptPhysicalURI == null) {
			throw new IllegalArgumentException("Could not find the concept. Please define the concept first before modifying the conceptual name");
		}
		
		String conceptualRel = OWLER.SEMOSS_URI + OWLER.DEFAULT_RELATION_CLASS + "/" + OWLER.CONCEPTUAL_RELATION_NAME;
		
		// okay, not only do i need to change this concept
		// but i have to change all the properties conceptual
		List<String> properties = engine.getProperties4Concept(conceptPhysicalURI, false);
		for(String propertyPhysicalUri : properties) {
			String propertyConceptualUri = engine.getConceptualUriFromPhysicalUri(propertyPhysicalUri);
			
			String propConceptualName = Utility.getClassName(propertyConceptualUri);
			String newPropertyConceptualUri = "http://semoss.org/ontologies/Relation/Contains/" + propConceptualName + "/" + newConceptualName;
			
			// remove the current relationship
			owlEngine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{propertyPhysicalUri, conceptualRel, propertyConceptualUri, true});
			// add the new relationship
			owlEngine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propertyPhysicalUri, conceptualRel, newPropertyConceptualUri, true});
		}
		
		// now update the actual concept
		// remove the current relationship
		owlEngine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{conceptPhysicalURI, conceptualRel, conceptualURI, true});
		// add the new relationship
		owlEngine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{conceptPhysicalURI, conceptualRel, newConceptualURI, true});
		
		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to commit modifications", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully edited concept name from " + concept + " to " + newConceptualName, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
