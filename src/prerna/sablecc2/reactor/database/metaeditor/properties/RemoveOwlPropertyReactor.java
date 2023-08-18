package prerna.sablecc2.reactor.database.metaeditor.properties;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabase;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class RemoveOwlPropertyReactor extends AbstractMetaEditorReactor {

	public RemoveOwlPropertyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey() };
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
			throw new IllegalArgumentException("Must define the concept being added to the database metadata");
		}
		String column = this.keyValue.get(this.keysToGet[2]);
		if( column == null || column.isEmpty()) {
			throw new IllegalArgumentException("Must define the property being added to the database metadata");
		}
		// if RDBMS, we need to know the prime key of the column
		IDatabase database = Utility.getDatabase(databaseId);
		ClusterUtil.pullOwl(databaseId);
		RDFFileSesameEngine owlEngine = database.getBaseDataEngine();
		String physicalPropUri = database.getPhysicalUriFromPixelSelector(concept + "__" + column);
		if(physicalPropUri == null) {
			throw new IllegalArgumentException("Cannot find property in existing metadata to remove");
		}
		
		// remove everything downstream of the property
		{
			String query = "select ?s ?p ?o where "
					+ "{ "
					+ "bind(<" + physicalPropUri + "> as ?s) "
					+ "{?s ?p ?o} "
					+ "}";
		
			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
				while(it.hasNext()) {
					IHeadersDataRow headerRows = it.next();
					executeRemoveQuery(headerRows, owlEngine);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(it != null) {
					it.cleanUp();
				}
			}
			
		}
		
		// repeat for upstream of the property
		{
			String query = "select ?s ?p ?o where "
					+ "{ "
					+ "bind(<" + physicalPropUri + "> as ?o) "
					+ "{?s ?p ?o} "
					+ "}";
		
			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
				while(it.hasNext()) {
					IHeadersDataRow headerRows = it.next();
					executeRemoveQuery(headerRows, owlEngine);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(it != null) {
					it.cleanUp();
				}
			}
		}
		
		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to remove the desired property", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.pushOwl(databaseId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully removed property", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
