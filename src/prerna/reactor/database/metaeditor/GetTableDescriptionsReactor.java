@ -0,0 +1,86 @@
package prerna.reactor.database.metaeditor;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Vector;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetTableDescriptionsReactor extends AbstractMetaEditorReactor {

	public GetTableDescriptionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String databaseId = getDatabaseId();
		// we may have an alias
		databaseId = testDatabaseId(databaseId, true);
		
		String concept = getConcept();
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		String conceptPhysicalUri = database.getPhysicalUriFromPixelSelector(concept);
		
		// Get the description for the table itself
		String tableDescription = database.getDescription(conceptPhysicalUri);
		
		// Create a map to hold column names and their descriptions
		Map<String, String> descriptions = new HashMap<String, String>();
		
		// Add table description with special key
		descriptions.put("__table__", tableDescription);
		
		// Get all properties for this concept
		List<String> propertyUris = database.getPropertyUris4PhysicalUri(conceptPhysicalUri);
		
		// Get descriptions for each property
		if (propertyUris != null) {
			for (String propertyUri : propertyUris) {
				String columnName = Utility.getClassName(propertyUri);
				String columnDescription = database.getDescription(propertyUri);
				descriptions.put(columnName, columnDescription);
			}
		}
		
		NounMetadata noun = new NounMetadata(descriptions, PixelDataType.CONST_STRING, PixelOperationType.ENTITY_DESCRIPTIONS);
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
}