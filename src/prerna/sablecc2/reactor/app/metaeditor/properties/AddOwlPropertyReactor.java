package prerna.sablecc2.reactor.app.metaeditor.properties;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.OWLER;
import prerna.util.Utility;

public class AddOwlPropertyReactor extends AbstractMetaEditorReactor {

	public AddOwlPropertyReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(), 
				ReactorKeysEnum.ADDITIONAL_DATA_TYPES.getKey(), CONCEPTUAL_NAME
			};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String appId = this.keyValue.get(this.keysToGet[0]);
		// perform translation if alias is passed
		// and perform security check
		appId = getAppId(appId, true);
		
		String concept = this.keyValue.get(this.keysToGet[1]);
		if(concept == null || concept.isEmpty()) {
			throw new IllegalArgumentException("Must define the concept this property is added to in the app metadata");
		}
		String column = this.keyValue.get(this.keysToGet[2]);
		if( column == null || column.isEmpty()) {
			throw new IllegalArgumentException("Must define the property being added to the app metadata");
		}
		String dataType = this.keyValue.get(this.keysToGet[3]);
		if(dataType == null || dataType.isEmpty()) {
			throw new IllegalArgumentException("Must define the data type for the concept being added to the app metadata");
		}
		String additionalDataType = this.keyValue.get(this.keysToGet[4]);
		String conceptual = this.keyValue.get(this.keysToGet[5]);
		if(conceptual != null) {
			conceptual = conceptual.trim();
			if(!conceptual.matches("^[a-zA-Z0-9-_]+$")) {
				throw new IllegalArgumentException("Conceptual name must contain only letters, numbers, and underscores");
			}
			conceptual = conceptual.replaceAll("_{2,}", "_");
		}
		
		IEngine engine = Utility.getEngine(appId);
		// make sure the concept exists
		String conceptPhysicalUri = null;
		Vector<String> concepts = engine.getConcepts(false);
		for(String conceptUri : concepts) {
			String table = Utility.getInstanceName(conceptUri);
			if(table.equalsIgnoreCase(concept)) {
				conceptPhysicalUri = conceptUri;
				break;
			}
		}

		if(conceptPhysicalUri == null) {
			throw new IllegalArgumentException("Could not find the concept. Please define the concept first before adding properties");
		}
		
		// make sure this property doesn't already exist for this concept
		List<String> properties = engine.getProperties4Concept(conceptPhysicalUri, true);
		for(String prop : properties) {
			if(column.equalsIgnoreCase(Utility.getInstanceName(prop))) {
				throw new IllegalArgumentException("A property already exists for this concept with this name. "
						+ "Add a new unique property or edit the existing property");
			}
		}
		
		// set the owler
		OWLER owler = getOWLER(appId);
		setOwlerValues(engine, owler);
		// add the property
		
		String tableName = Utility.getInstanceName(conceptPhysicalUri);
		String colName = null;
		if(engine.getEngineType() == ENGINE_TYPE.RDBMS || engine.getEngineType() == ENGINE_TYPE.IMPALA) {
			colName = Utility.getClassName(conceptPhysicalUri);
		}
		owler.addProp(tableName, colName, column, dataType, additionalDataType);

		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add the desired property", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
	
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully added new property", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
