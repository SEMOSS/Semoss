package prerna.nameserver;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class ConnectedConcepts {

	//TODO: add methods for retrieval specific data
	//TODO: should hold different pieces of information separately, and only combine to ugly hashtable at end
	
	private Map<String, Object> data = new HashMap<String, Object>();
	
	private static final String EQUIVALENT_CONCEPT = "equivalentURI";
	private static final String SIMILARITY_VALUE = "similarity";
	private static final String ENGINE_API = "api";
	
	public ConnectedConcepts() {
		
	}
	
	public void addSimilarity(String engine, String concept, double simVal) {
		if(data.containsKey(engine)) {
			Map<String, Object> engineMap = (Map<String, Object>) data.get(engine);
			Map<String, Object> conceptMap = (Map<String, Object>) engineMap.get(EQUIVALENT_CONCEPT);
			if(conceptMap.containsKey(concept)) {
				Map<String, Object> innerMap = (Map<String, Object>) conceptMap.get(concept);
				innerMap.put(SIMILARITY_VALUE, simVal);
			} else {
				Map<String, Object> innerMap = new Hashtable<String, Object>();
				innerMap.put(SIMILARITY_VALUE, simVal);
				conceptMap.put(concept, innerMap);
			}
		} else {
			Map<String, Object> engineMap = new Hashtable<String, Object>();
			Map<String, Object> conceptMap = new Hashtable<String, Object>();
			Map<String, Object> innerMap = new Hashtable<String, Object>();
			innerMap.put(SIMILARITY_VALUE, simVal);
			conceptMap.put(concept, innerMap);
			engineMap.put(EQUIVALENT_CONCEPT, conceptMap);
			data.put(engine, engineMap);
		}
	}
	
	public void addAPI(String engine, String api) {
		if(data.containsKey(engine)) {
			Map<String, Object> engineMap = (Map<String, Object>) data.get(engine);
			engineMap.put(ENGINE_API, api);
		} else {
			Map<String, Object> engineMap = new Hashtable<String, Object>();
			engineMap.put(ENGINE_API, api);
			data.put(engine, engineMap);
		}
	}
	
	public void addData(String engine, String conceptURI, Map<String, Map<String, Set<String>>> connections) {
		if(data.containsKey(engine)) {
			Map<String, Object> engineMap = (Map<String, Object>) data.get(engine);
			// need to check if relationship name already exists
			Map<String, Map<String, Map<String, Set<String>>>> conceptMap = (Map<String, Map<String, Map<String, Set<String>>>>) engineMap.get(EQUIVALENT_CONCEPT);
			if(conceptMap.containsKey(conceptURI)) {
				// need to check if already existing relationships
				Map<String, Map<String, Set<String>>> currConnections = conceptMap.get(conceptURI);
				for(String key : connections.keySet()) {
					if(currConnections.containsKey(key)) {
						Map<String, Set<String>> newRels = connections.get(key);
						// if found, add to existing set of possible connections
						Map<String, Set<String>> relMap = currConnections.get(key);
						for(String relKey : newRels.keySet()) {
							if(relMap.containsKey(relKey)) {
								relMap.get(relKey).addAll(newRels.get(relKey));
							} else {
								relMap.put(relKey, newRels.get(relKey));
							}
						}
					} else {
						// if not found, just add upstream/downstream and values
						currConnections.put(key, connections.get(key));
					}
				}
			} else {
				conceptMap.put(conceptURI, connections);
			}
		} else {
			Map<String, Object> engineMap = new Hashtable<String, Object>();
			Map<String, Object> innerMap = new Hashtable<String, Object>();
			innerMap.put(conceptURI, connections);
			engineMap.put(EQUIVALENT_CONCEPT, innerMap);
			data.put(engine, engineMap);
		}
	}
	
	public Map<String, Object> getData() {
		return data;
	}
}
