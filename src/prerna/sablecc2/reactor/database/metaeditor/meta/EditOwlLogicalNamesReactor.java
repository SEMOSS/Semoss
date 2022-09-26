package prerna.sablecc2.reactor.database.metaeditor.meta;

import java.io.IOException;
import java.util.Set;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class EditOwlLogicalNamesReactor extends AbstractMetaEditorReactor {

	public EditOwlLogicalNamesReactor() {
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

		IEngine database = Utility.getEngine(databaseId);
		ClusterUtil.reactorPullOwl(databaseId);
		String physicalUri = null;
		if (prop == null || prop.isEmpty()) {
			physicalUri = database.getPhysicalUriFromPixelSelector(concept);
		} else {
			physicalUri = database.getPhysicalUriFromPixelSelector(concept + "__" + prop);
		}
		
		// get the existing value if present
		Set<String> existingLogicalNames = database.getLogicalNames(physicalUri);

		Owler owler = new Owler(database);
		owler.deleteLogicalNames(physicalUri, existingLogicalNames);
		owler.addLogicalNames(physicalUri, logicalNames);
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add logical names", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.reactorPushOwl(databaseId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully added logical names",
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
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
			String id = (String) grs.get(0);
			if (id != null && !id.isEmpty()) {
				return id;
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
