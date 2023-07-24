package prerna.sablecc2.reactor.database.metaeditor.concepts;

import java.util.List;

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

public class RemoveOwlConceptReactor extends AbstractMetaEditorReactor {

	public RemoveOwlConceptReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey() };
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

		// since we are deleting the node
		// i need to delete the properties of this node
		// and then everything related to this node
		IDatabase database = Utility.getDatabase(databaseId);
		ClusterUtil.reactorPullOwl(databaseId);
		RDFFileSesameEngine owlEngine = database.getBaseDataEngine();
		String conceptPhysical = database.getPhysicalUriFromPixelSelector(concept);
		List<String> properties = database.getPropertyUris4PhysicalUri(conceptPhysical);
		StringBuilder bindings = new StringBuilder();
		for (String prop : properties) {
			bindings.append("(<").append(prop).append(">)");
		}

		if (bindings.length() > 0) {
			// get everything downstream of the props
			{
				String query = "select ?s ?p ?o where " + "{ " + "{?s ?p ?o} " + "} bindings ?s {" + bindings.toString() + "}";

				IRawSelectWrapper it = null;
				try {
					it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
					while (it.hasNext()) {
						IHeadersDataRow headerRows = it.next();
						executeRemoveQuery(headerRows, owlEngine);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (it != null) {
						it.cleanUp();
					}
				}
			}

			// repeat for upstream of prop
			{
				String query = "select ?s ?p ?o where " + "{ " + "{?s ?p ?o} " + "} bindings ?o {" + bindings.toString() + "}";

				IRawSelectWrapper it = null;
				try {
					it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
					while (it.hasNext()) {
						IHeadersDataRow headerRows = it.next();
						executeRemoveQuery(headerRows, owlEngine);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (it != null) {
						it.cleanUp();
					}
				}
			}
		}

		boolean hasTriple = false;

		// now repeat for the node itself
		// remove everything downstream of the node
		{
			String query = "select ?s ?p ?o where " + "{ " + "bind(<" + conceptPhysical + "> as ?s) " + "{?s ?p ?o} " + "}";

			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
				while (it.hasNext()) {
					hasTriple = true;
					IHeadersDataRow headerRows = it.next();
					executeRemoveQuery(headerRows, owlEngine);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (it != null) {
					it.cleanUp();
				}
			}
		}

		// repeat for upstream of the node
		{
			String query = "select ?s ?p ?o where " + "{ " + "bind(<" + conceptPhysical + "> as ?o) " + "{?s ?p ?o} " + "}";

			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
				while (it.hasNext()) {
					hasTriple = true;
					IHeadersDataRow headerRows = it.next();
					executeRemoveQuery(headerRows, owlEngine);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (it != null) {
					it.cleanUp();
				}
			}
		}

		if (!hasTriple) {
			throw new IllegalArgumentException("Cannot find concept in existing metadata to remove");
		}

		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to remove the desired concept", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.reactorPushOwl(databaseId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully removed concept and all its dependencies", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
