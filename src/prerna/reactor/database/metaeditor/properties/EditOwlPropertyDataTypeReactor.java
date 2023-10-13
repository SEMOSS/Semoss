package prerna.reactor.database.metaeditor.properties;

import org.openrdf.model.vocabulary.RDFS;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class EditOwlPropertyDataTypeReactor extends AbstractMetaEditorReactor {

	public EditOwlPropertyDataTypeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(), ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// perform translation if alias is passed
		// and perform security check
		databaseId = testDatabaseId(databaseId, true);

		String concept = this.keyValue.get(this.keysToGet[1]);
		if(concept == null || concept.isEmpty()) {
			throw new IllegalArgumentException("Must define the concept being modified in the database metadata");
		}
		
		String property = this.keyValue.get(this.keysToGet[2]);
		if(property == null || property.isEmpty()) {
			throw new IllegalArgumentException("Must define the property being modified in the database metadata");
		}
		
		String newDataType = this.keyValue.get(this.keysToGet[3]);
		if(newDataType == null || newDataType.isEmpty()) {
			throw new IllegalArgumentException("Must define the new data type");
		}
		// minor clean up
		newDataType = newDataType.trim();
				
		String newAdditionalDataType = this.keyValue.get(this.keysToGet[4]);

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		ClusterUtil.pullOwl(databaseId);
		RDFFileSesameEngine owlEngine = database.getBaseDataEngine();
		
		String parentPhysicalURI = database.getPhysicalUriFromPixelSelector(concept);
		if (parentPhysicalURI == null) {
			throw new IllegalArgumentException("Could not find the concept");
		}

		String propertyPhysicalURI = database.getPhysicalUriFromPixelSelector(concept + "__" + property);
		if (propertyPhysicalURI == null) {
			throw new IllegalArgumentException("Could not find the property. Please define the property first before modifying the conceptual name");
		}

		// remove if current data type is present
		String currentDataType = database.getDataTypes(propertyPhysicalURI);
		if (currentDataType != null) {
			owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{propertyPhysicalURI, RDFS.CLASS.stringValue(), currentDataType, true});
		}
		owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propertyPhysicalURI, RDFS.CLASS.stringValue(), "TYPE:" + newDataType, true});

		if (newAdditionalDataType != null && !newAdditionalDataType.isEmpty()) {
			newAdditionalDataType = newAdditionalDataType.trim();
			String adtlTypeObject = "ADTLTYPE:" + Owler.encodeAdtlDataType(newAdditionalDataType);

			// remove if additional data type is present
			String currentAdditionalDataType = database.getAdtlDataTypes(propertyPhysicalURI);
			if (currentAdditionalDataType != null) {
				currentAdditionalDataType = "ADTLTYPE:" + Owler.encodeAdtlDataType(currentAdditionalDataType);
				owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{propertyPhysicalURI, Owler.ADDITIONAL_DATATYPE_RELATION_URI, currentAdditionalDataType, false});
			}
			owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propertyPhysicalURI, Owler.ADDITIONAL_DATATYPE_RELATION_URI, adtlTypeObject, false});
		}

		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to commit modifications", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.pushOwl(databaseId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully edited data type of " + property + " to " + newDataType, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
