package prerna.sablecc2.reactor.app.metaeditor.properties;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.impl.util.MetadataUtility;
import prerna.engine.api.impl.util.Owler;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class AddOwlPropertyReactor extends AbstractMetaEditorReactor {

	public AddOwlPropertyReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), 
				ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.DATA_TYPE.getKey(), 
				ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey(), 
				CONCEPTUAL_NAME,
				ReactorKeysEnum.DESCRIPTION.getKey(), 
				ReactorKeysEnum.LOGICAL_NAME.getKey()
			};
		this.keyRequired = new int[]{1, 1, 1, 1, 0, 0, 0, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String appId = this.keyValue.get(this.keysToGet[0]);
		// perform translation if alias is passed
		// and perform security check
		appId = testAppId(appId, true);

		String concept = this.keyValue.get(this.keysToGet[1]);
		String column = this.keyValue.get(this.keysToGet[2]);
		String dataType = this.keyValue.get(this.keysToGet[3]);
		String additionalDataType = this.keyValue.get(this.keysToGet[4]);
		
//		String conceptual = this.keyValue.get(this.keysToGet[5]);
//		if(conceptual != null) {
//			conceptual = conceptual.trim();
//			if(!conceptual.matches("^[a-zA-Z0-9-_]+$")) {
//				throw new IllegalArgumentException("Conceptual name must contain only letters, numbers, and underscores");
//			}
//			conceptual = conceptual.replaceAll("_{2,}", "_");
//		}

		IEngine engine = Utility.getEngine(appId);
		ClusterUtil.reactorPullOwl(appId);
		// make sure the concept exists
		String conceptPhysicalUri = engine.getPhysicalUriFromPixelSelector(concept);
		if (conceptPhysicalUri == null) {
			throw new IllegalArgumentException("Could not find the concept. Please define the concept first before adding properties");
		}
		
		// make sure this property doesn't already exist for this concept
		if (MetadataUtility.propertyExistsForConcept(engine, conceptPhysicalUri, column)) {
			throw new IllegalArgumentException("A property already exists for this concept with this name. "
					+ "Add a new unique property or edit the existing property");
		}

		// set the owler
		Owler owler = getOWLER(appId);
		setOwlerValues(engine, owler);
		// add the property
		String tableName = Utility.getInstanceName(conceptPhysicalUri);
//		String colName = null;
//		if(engine.getEngineType() == ENGINE_TYPE.RDBMS || engine.getEngineType() == ENGINE_TYPE.IMPALA) {
//			colName = Utility.getClassName(conceptPhysicalUri);
//		}
		String physicalUri = owler.addProp(tableName, column, dataType, additionalDataType);

		// now add the description (checks done in method)
		String description = this.keyValue.get(this.keysToGet[6]);
		owler.addDescription(physicalUri, description);
		// add the logical names (additional checks done in method)
		List<String> logicalNames = getLogicalNames();
		if (!logicalNames.isEmpty()) {
			owler.addLogicalNames(physicalUri, logicalNames.toArray(new String[] {}));
		}

		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add the desired property", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(appId);
		ClusterUtil.reactorPushOwl(appId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully added new property", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	/**
	 * Get the logical names
	 * 
	 * @return
	 */
	private List<String> getLogicalNames() {
		Vector<String> logicalNames = new Vector<String>();
		GenRowStruct instanceGrs = this.store.getNoun(keysToGet[7]);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			for (int i = 0; i < instanceGrs.size(); i++) {
				String name = (String) instanceGrs.get(i);
				if (name.length() > 0) {
					logicalNames.add(name);
				}
			}
		}
		return logicalNames;
	}
}
