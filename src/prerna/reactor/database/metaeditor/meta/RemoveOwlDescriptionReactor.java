package prerna.reactor.database.metaeditor.meta;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class RemoveOwlDescriptionReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(RemoveOwlDescriptionReactor.class);

	public RemoveOwlDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DESCRIPTION.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have an alias
		databaseId = testDatabaseId(databaseId, true);
		
		String concept = this.keyValue.get(this.keysToGet[1]);
		String prop = this.keyValue.get(this.keysToGet[2]);
		String description = this.keyValue.get(this.keysToGet[3]);
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		try(WriteOWLEngine owlEngine = database.getOWLEngineFactory().getWriteOWL()) {
			ClusterUtil.pullOwl(databaseId, owlEngine);

			String physicalUri = null;
			if(prop == null || prop.isEmpty()) {
				physicalUri = owlEngine.getPhysicalUriFromPixelSelector(concept);
			} else {
				physicalUri = owlEngine.getPhysicalUriFromPixelSelector(concept + "__" + prop);
			}
			
			owlEngine.deleteDescription(physicalUri, description);
			
			try {
				owlEngine.export();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
				noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to remove descriptions : " + description, 
						PixelDataType.CONST_STRING, PixelOperationType.ERROR));
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
		noun.addAdditionalReturn(new NounMetadata("Successfully removed descriptions : " + description, 
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
