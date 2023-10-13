package prerna.reactor.database.metaeditor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IDatabaseEngine;
import prerna.nameserver.utility.MetamodelVertex;
import prerna.reactor.masterdatabase.util.GenerateMetamodelLayout;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetOwlMetamodelReactor extends AbstractMetaEditorReactor {

	public GetOwlMetamodelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		databaseId = testDatabaseId(databaseId, false);
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		Map<String, Object[]> metamodelObject = database.getMetamodel();
		Object[] nodes = metamodelObject.get("nodes");
		Object[] relationships = metamodelObject.get("edges");
		Map<String, Collection<String>> concepts = new HashMap<String, Collection<String>>();

		for(Object node : nodes) {
			MetamodelVertex v = (MetamodelVertex) node;
			concepts.put(v.getConceptualName(), new ArrayList<String>(v.getPropSet()));
		}
		List<Map<String, String>> rels = new ArrayList<>();

		for(Object relation : relationships) {
			rels.add((Map<String,String>) relation);
		}

		HashMap<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put(Constants.NODE_PROP, nodes);
		returnMap.put(Constants.RELATION_PROP, relationships);
		Map<String, Map<String, Double>> positions = GenerateMetamodelLayout.generateOWLMetamodelLayout(concepts, rels);
		returnMap.put(Constants.POSITION_PROP, positions);

		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
	

}
