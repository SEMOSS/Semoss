package prerna.sablecc2.reactor.app.metaeditor.concepts;

import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.Utility;

public class EditOwlConceptDataTypeReactor extends AbstractMetaEditorReactor {

	public EditOwlConceptDataTypeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.DATA_TYPE.getKey()};
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
			throw new IllegalArgumentException("Must define the concept being modified in the app metadata");
		}
		
		String newDataType = this.keyValue.get(this.keysToGet[2]);
		if(newDataType == null || newDataType.isEmpty()) {
			throw new IllegalArgumentException("Must define the new data type");
		}
		// make sure it conforms
		newDataType = newDataType.trim();
		
		IEngine engine = Utility.getEngine(appId);
		RDFFileSesameEngine owlEngine = engine.getBaseDataEngine();
		
		String conceptualURI = "http://semoss.org/ontologies/Concept/" + concept;
		String conceptPhysicalURI = engine.getConceptPhysicalUriFromConceptualUri(conceptualURI);
		if(conceptPhysicalURI == null) {
			throw new IllegalArgumentException("Could not find the concept. Please define the concept first before modifying the conceptual name");
		}
		
		// get the current data type
		String curDataType = engine.getDataTypes(conceptPhysicalURI);
		
		// remove the current relationship
		owlEngine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{conceptPhysicalURI, RDFS.CLASS.stringValue(), curDataType, true});
		// add the new relationship
		owlEngine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{conceptPhysicalURI, RDFS.CLASS.stringValue(), "TYPE:" + newDataType, true});
		
		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to commit modifications", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully edited data type of " + concept + " to " + newDataType, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

}
