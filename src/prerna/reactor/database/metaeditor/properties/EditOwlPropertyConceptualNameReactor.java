package prerna.reactor.database.metaeditor.properties;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.AbstractOWLEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class EditOwlPropertyConceptualNameReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(EditOwlPropertyConceptualNameReactor.class);
	
	public EditOwlPropertyConceptualNameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), CONCEPTUAL_NAME};
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
		
		String property = this.keyValue.get(this.keysToGet[2]);
		if (property == null || property.isEmpty()) {
			throw new IllegalArgumentException("Must define the property being modified in the database metadata");
		}
		
		String newPixelName = this.keyValue.get(this.keysToGet[3]);
		if (newPixelName == null || newPixelName.isEmpty()) {
			throw new IllegalArgumentException("Must define the new conceptual name");
		}
		// make sure it conforms
		newPixelName = newPixelName.trim();
		if (!newPixelName.matches("^[a-zA-Z0-9-_]+$")) {
			throw new IllegalArgumentException("Conceptual name must contain only letters, numbers, and underscores");
		}
		newPixelName = newPixelName.replaceAll("_{2,}", "_");
		
		String newPixelURI = "http://semoss.org/ontologies/Relation/Contains/" + newPixelName + "/" + concept;

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		try(WriteOWLEngine owlEngine = database.getOWLEngineFactory().getWriteOWL()) {
			ClusterUtil.pullOwl(databaseId, owlEngine);

			String pixelURI = "http://semoss.org/ontologies/Relation/Contains/" + property + "/" + concept;
			String parentPhysicalURI = database.getPhysicalUriFromPixelSelector(concept);
			if (parentPhysicalURI == null) {
				throw new IllegalArgumentException("Could not find the concept");
			}
			
			String propertyPhysicalURI = database.getPhysicalUriFromPixelSelector(concept + "__" + property);
			if (propertyPhysicalURI == null) {
				throw new IllegalArgumentException("Could not find the property. Please define the property first before modifying the conceptual name");
			}
			
			// make sure this isn't already a name for an existing property
			List<String> otherConceptualProperties = database.getPropertyPixelSelectors(concept);
			if (otherConceptualProperties.contains(newPixelURI)) {
				throw new IllegalArgumentException("This property conceptual name already exists");
			}
			
			// remove the current relationship
			owlEngine.removeFromBaseEngine(new Object[]{propertyPhysicalURI, AbstractOWLEngine.PIXEL_RELATION_URI, pixelURI, true});
			// add the new relationship
			owlEngine.addToBaseEngine(new Object[]{propertyPhysicalURI, AbstractOWLEngine.PIXEL_RELATION_URI, newPixelURI, true});
			
			try {
				owlEngine.export();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
				noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to commit modifications", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				return noun;
			}
			
			EngineSyncUtility.clearEngineCache(databaseId);
			ClusterUtil.pushOwl(databaseId, owlEngine);
			
		} catch (IOException | InterruptedException e1) {
			classLogger.error(Constants.STACKTRACE, e1);
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to modify the OWL", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully edited concept name from " + property + " to " + newPixelName, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
