package prerna.sablecc2.reactor.app.metaeditor.meta;

import java.io.IOException;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class EditOwlDescriptionReactor extends AbstractMetaEditorReactor {

	public EditOwlDescriptionReactor() {
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

		IEngine database = Utility.getEngine(databaseId);
		ClusterUtil.reactorPullOwl(databaseId);
		String physicalUri = null;
		if (prop == null || prop.isEmpty()) {
			physicalUri = database.getPhysicalUriFromPixelSelector(concept);
		} else {
			physicalUri = database.getPhysicalUriFromPixelSelector(concept + "__" + prop);
		}

		// get the existing value if present
		String existingDescription = database.getDescription(physicalUri);
		
		Owler owler = new Owler(database);
		owler.deleteDescription(physicalUri, existingDescription);
		owler.addDescription(physicalUri, description);
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add description", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.reactorPushOwl(databaseId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully added descriptions", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
