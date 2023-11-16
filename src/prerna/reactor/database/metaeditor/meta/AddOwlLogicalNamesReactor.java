package prerna.reactor.database.metaeditor.meta;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class AddOwlLogicalNamesReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(AddOwlLogicalNamesReactor.class);

	public AddOwlLogicalNamesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.LOGICAL_NAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String databaseId = getDatabaseId();
		// we may have an alias
		databaseId = testDatabaseId(databaseId, true);
		
		String concept = getConcept();
		String prop = getProperty();
		String[] logicalNames = getLogicalNames();
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		try(WriteOWLEngine owlEngine = database.getOWLEngineFactory().getWriteOWL()) {
			ClusterUtil.pullOwl(databaseId, owlEngine);

			String physicalUri = null;
			if (prop == null || prop.isEmpty()) {
				physicalUri = owlEngine.getPhysicalUriFromPixelSelector(concept);
			} else {
				physicalUri = owlEngine.getPhysicalUriFromPixelSelector(concept + "__" + prop);
			}
			
			owlEngine.addLogicalNames(physicalUri, logicalNames);
			
			try {
				owlEngine.export();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
				noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to add logical names", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
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
		noun.addAdditionalReturn(new NounMetadata("Successfully added logical names", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

	
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////// GRAB INPUTS FROM PIXEL REACTOR //////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////

	private String getDatabaseId() {
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			String databaseId = (String) grs.get(0);
			if (databaseId != null && !databaseId.isEmpty()) {
				return databaseId;
			}
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[0]);
	}

	private String getConcept() {
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			String concept = (String) grs.get(0);
			if (concept != null && !concept.isEmpty()) {
				return concept;
			}
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[1]);
	}
	
	private String getProperty() {
		GenRowStruct grs = this.store.getNoun(keysToGet[2]);
		if (grs != null && !grs.isEmpty()) {
			String prop = (String) grs.get(0);
			if (prop != null && !prop.isEmpty()) {
				return prop;
			}
		}

		return "";
	}

	private String[] getLogicalNames() {
		String[] logicalNames = null;
		GenRowStruct grs = this.store.getNoun(keysToGet[3]);
		if (grs != null && !grs.isEmpty()) {
			logicalNames = new String[grs.size()];
			for (int i = 0; i < grs.size(); i++) {
				String name = (String) grs.get(i);
				if (name != null && !name.isEmpty()) {
					logicalNames[i] = name;
				}
			}
		}
		return logicalNames;
	}
}
