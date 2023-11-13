package prerna.reactor.database.metaeditor.concepts;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class RemoveOwlConceptReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(RemoveOwlConceptReactor.class);

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
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		ClusterUtil.pullOwl(databaseId);
		
		try(WriteOWLEngine owlEngine = database.getOWLEngineFactory().getWriteOWL()) {
			
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
						it = owlEngine.query(query);
						while (it.hasNext()) {
							IHeadersDataRow headerRows = it.next();
							executeRemoveQuery(headerRows, owlEngine);
						}
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						if (it != null) {
							try {
								it.close();
							} catch (IOException e) {
								classLogger.error(Constants.STACKTRACE, e);
							}
						}
					}
				}
	
				// repeat for upstream of prop
				{
					String query = "select ?s ?p ?o where " + "{ " + "{?s ?p ?o} " + "} bindings ?o {" + bindings.toString() + "}";
	
					IRawSelectWrapper it = null;
					try {
						it = owlEngine.query(query);
						while (it.hasNext()) {
							IHeadersDataRow headerRows = it.next();
							executeRemoveQuery(headerRows, owlEngine);
						}
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						if (it != null) {
							try {
								it.close();
							} catch (IOException e) {
								classLogger.error(Constants.STACKTRACE, e);
							}
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
					it = owlEngine.query(query);
					while (it.hasNext()) {
						hasTriple = true;
						IHeadersDataRow headerRows = it.next();
						executeRemoveQuery(headerRows, owlEngine);
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if (it != null) {
						try {
							it.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
	
			// repeat for upstream of the node
			{
				String query = "select ?s ?p ?o where " + "{ " + "bind(<" + conceptPhysical + "> as ?o) " + "{?s ?p ?o} " + "}";
	
				IRawSelectWrapper it = null;
				try {
					it = owlEngine.query(query);
					while (it.hasNext()) {
						hasTriple = true;
						IHeadersDataRow headerRows = it.next();
						executeRemoveQuery(headerRows, owlEngine);
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if (it != null) {
						try {
							it.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
	
			if (!hasTriple) {
				throw new IllegalArgumentException("Cannot find concept in existing metadata to remove");
			}
	
			try {
				owlEngine.commit();
				owlEngine.export();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
				noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to remove the desired concept", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
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
		noun.addAdditionalReturn(new NounMetadata("Successfully removed concept and all its dependencies", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
