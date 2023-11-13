package prerna.reactor.database.metaeditor.concepts;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;

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

public class EditOwlConceptDataTypeReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(EditOwlConceptDataTypeReactor.class);
	
	public EditOwlConceptDataTypeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.DATA_TYPE.getKey(), ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey() };
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

		String newDataType = this.keyValue.get(this.keysToGet[2]);
		if (newDataType == null || newDataType.isEmpty()) {
			throw new IllegalArgumentException("Must define the new data type");
		}
		// minor clean up
		newDataType = newDataType.trim();

		String newAdditionalDataType = this.keyValue.get(this.keysToGet[3]);

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		ClusterUtil.pullOwl(databaseId);
		
		try(WriteOWLEngine owlEngine = database.getOWLEngineFactory().getWriteOWL()) {
			String conceptPhysicalURI = database.getPhysicalUriFromPixelSelector(concept);
			if (conceptPhysicalURI == null) {
				throw new IllegalArgumentException("Could not find the concept. Please define the concept first before modifying the conceptual name");
			}
	
			// remove the current data type
			String currentDataType = database.getDataTypes(conceptPhysicalURI);
			if (currentDataType != null) {
				owlEngine.removeFromBaseEngine(new Object[] { conceptPhysicalURI, RDFS.CLASS.stringValue(), currentDataType, true });
			}
			// add the new data type
			owlEngine.addToBaseEngine(new Object[] { conceptPhysicalURI, RDFS.CLASS.stringValue(), "TYPE:" + newDataType, true });
	
			if (newAdditionalDataType != null && !newAdditionalDataType.isEmpty()) {
				newAdditionalDataType = newAdditionalDataType.trim();
				String adtlTypeObject = "ADTLTYPE:" + newAdditionalDataType;
	
				// remove if additional data type is present
				String currentAdditionalDataType = database.getAdtlDataTypes(conceptPhysicalURI);
				if (currentAdditionalDataType != null) {
					currentAdditionalDataType = "ADTLTYPE:" + currentAdditionalDataType;
					owlEngine.removeFromBaseEngine(new Object[] { conceptPhysicalURI, AbstractOWLEngine.ADDITIONAL_DATATYPE_RELATION_URI, currentAdditionalDataType, false });
				}
				owlEngine.addToBaseEngine(new Object[] { conceptPhysicalURI, AbstractOWLEngine.ADDITIONAL_DATATYPE_RELATION_URI, adtlTypeObject, false });
			}
	
			try {
				owlEngine.export();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
				noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to commit modifications", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				return noun;
			}
			
			EngineSyncUtility.clearEngineCache(databaseId);
			ClusterUtil.pushOwl(databaseId);
		
		} catch (IOException | InterruptedException e1) {
			classLogger.error(Constants.STACKTRACE, e1);
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to modify the OWL", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully edited data type of " + concept + " to " + newDataType, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
