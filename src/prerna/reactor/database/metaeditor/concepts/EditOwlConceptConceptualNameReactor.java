package prerna.reactor.database.metaeditor.concepts;

import java.util.List;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.impl.util.AbstractOwler;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class EditOwlConceptConceptualNameReactor extends AbstractMetaEditorReactor {

	public EditOwlConceptConceptualNameReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), CONCEPTUAL_NAME };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// perform translation if alias is passed
		// and perform security check
		databaseId = testDatabaseId(databaseId, true);

		String concept = this.keyValue.get(this.keysToGet[1]);
		if (concept == null || concept.isEmpty()) {
			throw new IllegalArgumentException("Must define the concept being modified in the database metadata");
		}

		String newPixelName = this.keyValue.get(this.keysToGet[2]);
		if (newPixelName == null || newPixelName.isEmpty()) {
			throw new IllegalArgumentException("Must define the new conceptual name");
		}
		// make sure it conforms
		newPixelName = newPixelName.trim();
		if (!newPixelName.matches("^[a-zA-Z0-9-_]+$")) {
			throw new IllegalArgumentException("Conceptual name must contain only letters, numbers, and underscores");
		}
		newPixelName = newPixelName.replaceAll("_{2,}", "_");

		String newPixelURI = "http://semoss.org/ontologies/Concept/" + newPixelName;

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		ClusterUtil.pullOwl(databaseId);
		RDFFileSesameEngine owlEngine = database.getBaseDataEngine();

		// make sure this name isn't currently present in the engine
		if (database.getPhysicalUriFromPixelSelector(newPixelName) != null) {
			throw new IllegalArgumentException("This conceptual name already exists");
		}

		String existingPixelURI = "http://semoss.org/ontologies/Concept/" + concept;
		String conceptPhysicalURI = database.getPhysicalUriFromPixelSelector(concept);
		if (conceptPhysicalURI == null) {
			throw new IllegalArgumentException("Could not find the concept. Please define the concept first before modifying the conceptual name");
		}

		// okay, not only do i need to change this concept
		// but i have to change all the properties conceptual
		List<String> properties = database.getPropertyUris4PhysicalUri(conceptPhysicalURI);
		for (String propertyPhysicalUri : properties) {
			String propertyPixelUri = database.getPropertyPixelUriFromPhysicalUri(conceptPhysicalURI, propertyPhysicalUri);
			String propPixelName = Utility.getClassName(propertyPixelUri);
			String newPropertyPixelUri = "http://semoss.org/ontologies/Relation/Contains/" + propPixelName + "/" + newPixelName;

			// remove the current relationship
			owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[] { propertyPhysicalUri, AbstractOwler.PIXEL_RELATION_URI, propertyPixelUri, true });
			// add the new relationship
			owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { propertyPhysicalUri, AbstractOwler.PIXEL_RELATION_URI, newPropertyPixelUri, true });
		}

		// now update the actual concept
		// remove the current relationship
		owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[] { conceptPhysicalURI, AbstractOwler.PIXEL_RELATION_URI, existingPixelURI, true });
		// add the new relationship
		owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { conceptPhysicalURI, AbstractOwler.PIXEL_RELATION_URI, newPixelURI, true });

		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to commit modifications", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.pushOwl(databaseId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully edited concept name from " + concept + " to " + newPixelName, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
