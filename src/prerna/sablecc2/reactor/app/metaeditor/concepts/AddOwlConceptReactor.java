package prerna.sablecc2.reactor.app.metaeditor.concepts;

import java.io.IOException;
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

public class AddOwlConceptReactor extends AbstractMetaEditorReactor {

	public AddOwlConceptReactor() {
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
			throw new IllegalArgumentException("Must define the concept being added to the app metadata");
		}
		
		// if RDBMS, we need to know the prim key of the column
		IEngine engine = Utility.getEngine(appId);
		String column = this.keyValue.get(this.keysToGet[2]);
		if( (column == null || column.isEmpty()) && engine.getEngineType() == ENGINE_TYPE.RDBMS ) {
			throw new IllegalArgumentException("Must define the column for the concept being added to the app metadata");
		}
		String dataType = this.keyValue.get(this.keysToGet[3]);
		if(dataType == null || dataType.isEmpty()) {
			throw new IllegalArgumentException("Must define the data type for the concept being added to the app metadata");
		}
		// TODO: need to account for this on concepts!!!!
		String additionalDataType = this.keyValue.get(this.keysToGet[4]);
		String conceptual = this.keyValue.get(this.keysToGet[5]);
		if(conceptual != null) {
			conceptual = conceptual.trim();
			if(!conceptual.matches("^[a-zA-Z0-9-_]+$")) {
				throw new IllegalArgumentException("Conceptual name must contain only letters, numbers, and underscores");
			}
			conceptual = conceptual.replaceAll("_{2,}", "_");
		}

		Vector<String> concepts = engine.getConcepts(false);
		for(String conceptUri : concepts) {
			String table = Utility.getInstanceName(conceptUri);
			if(table.equalsIgnoreCase(concept)) {
				throw new IllegalArgumentException("A concept already exists with this name. "
						+ "Add a new unique concept or edit the existing concept");
			}
		}

		OWLER owler = getOWLER(appId);
		owler.addConcept(concept, column, OWLER.BASE_URI, dataType, conceptual);

		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add the desired concept", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
	
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully added new concept", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
