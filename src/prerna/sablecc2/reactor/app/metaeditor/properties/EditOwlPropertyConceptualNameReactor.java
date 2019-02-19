package prerna.sablecc2.reactor.app.metaeditor.properties;

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

public class EditOwlPropertyConceptualNameReactor extends AbstractMetaEditorReactor {

	public EditOwlPropertyConceptualNameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), CONCEPTUAL_NAME};
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
		
		String property = this.keyValue.get(this.keysToGet[2]);
		if(property == null || property.isEmpty()) {
			throw new IllegalArgumentException("Must define the property being modified in the app metadata");
		}
		
		String newConceptualName = this.keyValue.get(this.keysToGet[3]);
		if(newConceptualName == null || newConceptualName.isEmpty()) {
			throw new IllegalArgumentException("Must define the new conceptual name");
		}
		// make sure it conforms
		newConceptualName = newConceptualName.trim();
		if(!newConceptualName.matches("^[a-zA-Z0-9-_]+$")) {
			throw new IllegalArgumentException("Conceptual name must contain only letters, numbers, and underscores");
		}
		newConceptualName = newConceptualName.replaceAll("_{2,}", "_");
		
		String newConceptualURI = "http://semoss.org/ontologies/Relation/Contains/" + newConceptualName + "/" + concept;

		IEngine engine = Utility.getEngine(appId);
		RDFFileSesameEngine owlEngine = engine.getBaseDataEngine();
		
		String conceptualURI = "http://semoss.org/ontologies/Relation/Contains/" + property + "/" + concept;
		String parentConcepturalURI = "http://semoss.org/ontologies/Concept/" + concept;
		String parentPhysicalURI = engine.getConceptPhysicalUriFromConceptualUri(parentConcepturalURI);
		if(parentPhysicalURI == null) {
			throw new IllegalArgumentException("Could not find the concept");
		}
		
		String propertyPhysicalURI = engine.getPropertyPhysicalUriFromConceptualUri(property, concept);
		if(propertyPhysicalURI == null) {
			throw new IllegalArgumentException("Could not find the property. Please define the property first before modifying the conceptual name");
		}
		
		// make sure this isn't already a name for an existing property
		List<String> otherConceptualProperties = engine.getProperties4Concept(parentPhysicalURI, true);
		if(otherConceptualProperties.contains(newConceptualURI)) {
			throw new IllegalArgumentException("This property conceptual name already exists");
		}
		
		String conceptualRel = OWLER.SEMOSS_URI + OWLER.DEFAULT_RELATION_CLASS + "/" + OWLER.CONCEPTUAL_RELATION_NAME;
		// remove the current relationship
		owlEngine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{propertyPhysicalURI, conceptualRel, conceptualURI, true});
		// add the new relationship
		owlEngine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propertyPhysicalURI, conceptualRel, newConceptualURI, true});
		
		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to commit modifications", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully edited concept name from " + property + " to " + newConceptualName, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
