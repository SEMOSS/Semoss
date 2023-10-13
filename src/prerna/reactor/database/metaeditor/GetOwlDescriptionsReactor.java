package prerna.reactor.database.metaeditor;

import java.util.List;
import java.util.Vector;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetOwlDescriptionsReactor extends AbstractMetaEditorReactor {

	public GetOwlDescriptionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String databaseId = getDatabaseId();
		// we may have an alias
		databaseId = testDatabaseId(databaseId, true);
		
		String concept = getConcept();
		String prop = getProperty();
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		String physicalUri = null;
		if(prop == null || prop.isEmpty()) {
			physicalUri = database.getPhysicalUriFromPixelSelector(concept);
		} else {
			physicalUri = database.getPhysicalUriFromPixelSelector(concept + "__" + prop);
		}
		
		String description = database.getDescription(physicalUri);
		List<String> desList = new Vector<String>();
		desList.add(description);
		NounMetadata noun = new NounMetadata(desList, PixelDataType.CONST_STRING, PixelOperationType.ENTITY_DESCRIPTIONS);
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
}
