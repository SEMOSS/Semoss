package prerna.sablecc2.reactor.algorithms.xray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.algorithm.learning.matching.DomainValues;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetLocalDBSchemaReactor extends AbstractReactor {
	public GetLocalDBSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineName = this.keyValue.get(this.keysToGet[0]);
		if (engineName == null) {
			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.DATABASE.getKey());
		}
		IEngine engine = Utility.getEngine(engineName);
		List<String> concepts = DomainValues.getConceptList(engine);
		SelectQueryStruct qs = engine.getDatabaseQueryStruct();
		Map<String, Map<String, List>> relations = qs.getRelations();
		// get relations
		Map<String, List<String>> relationshipMap = new HashMap<String, List<String>>();
		// structure is Title = {inner.join={Genre, Studio, Nominated}}

		for (String concept : concepts) {
			concept = DomainValues.determineCleanConceptName(concept, engine);
			if (concept.equals("Concept")) {
				continue;
			}
			// check if concept is in the relationship hashmap, if not just add
			// an empty list
			List<String> conceptRelations = new ArrayList<String>();
			for (String key : relations.keySet()) {
				if (concept.equalsIgnoreCase(key)) {
					conceptRelations = relations.get(key).get("inner.join");
					// TODO check if this changes
				}
			}
			relationshipMap.put(concept, conceptRelations);
		}

		// tablename: [{name, type}]
		HashMap<String, ArrayList<HashMap>> tableDetails = new HashMap<String, ArrayList<HashMap>>();
		for (String conceptURI : concepts) {
			String cleanConcept = DomainValues.determineCleanConceptName(conceptURI, engine);
			// ignore default concept value
			if (cleanConcept.equals("Concept")) {
				continue;
			}
			ArrayList<HashMap> allCols = new ArrayList<HashMap>();
			HashMap<String, String> colInfo = new HashMap<String, String>();
			colInfo.put("name", cleanConcept);
			String dataType = engine.getDataTypes(conceptURI);
			if (dataType != null) {
				dataType = SemossDataType.convertStringToDataType(dataType).toString();
			} else {
				dataType = SemossDataType.STRING.toString();
			}
			colInfo.put("type", dataType);
			allCols.add(colInfo);
			List<String> properties = DomainValues.getPropertyList(engine, conceptURI);
			for (String prop : properties) {
				String cleanProp = DomainValues.determineCleanPropertyName(prop, engine);
				HashMap<String, String> propInfo = new HashMap<String, String>();
				propInfo.put("name", cleanProp);
				dataType = engine.getDataTypes(prop);
				if (dataType != null) {
					if(dataType.contains("TYPE:")) {
						dataType = dataType.replace("TYPE:", "");
					}
					dataType = SemossDataType.convertStringToDataType(dataType).toString();
				} else {
					dataType = SemossDataType.STRING.toString();
				}
				propInfo.put("type", dataType);
				allCols.add(propInfo);
			}
			tableDetails.put(cleanConcept, allCols);
		}

		HashMap<String, Object> ret = new HashMap<String, Object>();
		ret.put("databaseName", engine.getEngineId());
		ret.put("tables", tableDetails);
		ret.put("relationships", relationshipMap);

		return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

}
