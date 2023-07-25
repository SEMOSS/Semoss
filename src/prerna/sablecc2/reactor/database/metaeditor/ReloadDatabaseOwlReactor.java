package prerna.sablecc2.reactor.database.metaeditor;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabase;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class ReloadDatabaseOwlReactor extends AbstractMetaEditorReactor {

	/*
	 * This class is when you make local changes to the OWL file and want to
	 * reload the owl for the database with that change
	 * 
	 * This is because the RC on the database OWL will not be synchronized
	 */

	public ReloadDatabaseOwlReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		databaseId = testDatabaseId(databaseId, true);

		IDatabase database = Utility.getDatabase(databaseId);
		ClusterUtil.reactorPullOwl(databaseId);
		RDFFileSesameEngine oldOwlEngine = database.getBaseDataEngine();
		// load a new owl engine from the file
		RDFFileSesameEngine updatedOwlEngine = loadOwlEngineFile(databaseId);
		// replace
		database.setBaseDataEngine(updatedOwlEngine);
		// close the old database
		// assuming it was loaded properly
		if (oldOwlEngine != null) {
			oldOwlEngine.close();
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.reactorPushOwl(databaseId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully reloaded database owl", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
