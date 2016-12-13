package prerna.sablecc.services;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

import com.hp.hpl.jena.vocabulary.RDFS;

import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DatabasePkqlService {

	private static enum DIRECTION_KEYS {
		UPSTREAM ("upstream"), 
		DOWNSTREAM ("downstream");
		
		private final String name;
		
		private DIRECTION_KEYS(String name) {
			this.name = name;
		}
		
		public String toString() {
			return this.name;
		}
	};
	
	/**
	 * Get the list of engines available within SEMOSS
	 * @return
	 */
	public static List<String> getDatabaseList() {
		/*
		 * On start up, we store a string to hold all the engines in DIHelper
		 * Grab the string from DIHelper and split based on ";" since that is the 
		 * delimiter used to separate the engine names
		 * 
		 * Sometimes, if it starts with a ";" or ends with a ";" the methods split will
		 * add an empty cell.  Remove start and ending ";" to avoid this
		 */
		String allEngines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
		if(allEngines.startsWith(";")) {
			allEngines = allEngines.substring(1);
		}
		if(allEngines.endsWith(";")) {
			allEngines = allEngines.substring(0, allEngines.length()-1);
		}
		String[] engines = allEngines.split(";");
		return Arrays.asList(engines);
	}
	
	public static Map<String, Set<String>> getAllConceptsFromEngines() {
		/*
		 * Grab the local master engine and query for the concepts
		 * We do not want to load the engine until the user actually wants to use it
		 */
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);

		/*
		 * The return structure should be the following:
		 * 
		 * { engineName1 -> [conceptualConceptName1, conceptualConceptName2, ...],
		 * 	 engineName2 -> [conceptualConceptName1, conceptualConceptName2, ...],
		 * 	 ...
		 * }
		 */
		Map<String, Set<String>> returnHash = new TreeMap<String, Set<String>>();

		String query = "SELECT DISTINCT ?engine ?conceptConceptual WHERE { "
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
				+ "} ORDER BY ?engine";

		// iterate through the results and append onto the list
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext()) {
			IHeadersDataRow ss = wrapper.next();
			Object[] values = ss.getValues();
			
			// grab values
			String engineName = values[0] + "";
			String conceptConceptual = values[1] + "";
			
			Set<String> conceptSet = null;
			if(returnHash.containsKey(engineName)) {
				conceptSet = returnHash.get(engineName);
			} else {
				conceptSet = new TreeSet<String>();
				// add the concept set to the return map
				returnHash.put(engineName, conceptSet);
			}
			
			conceptSet.add(conceptConceptual);
		}
		
		return returnHash;
	}
	
	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static Set<String> getAllLogicalNamesFromConceptual(String conceptualName, String parentConceptualName) {
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);

		String conceptualUri = "http://semoss.org/ontologies/Concept/" + conceptualName;
		if(parentConceptualName != null && !parentConceptualName.trim().isEmpty()) {
			conceptualUri += "/" + parentConceptualName;
		}
		
		String query = "SELECT DISTINCT ?conceptLogical WHERE { "
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?conceptLogical}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> <" + conceptualUri + ">}"
				+ "} ORDER BY ?conceptLogical";
		
		Set<String> logicalNames = new TreeSet<String>();

		// iterate through the results and append onto the list
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext()) {
			IHeadersDataRow ss = wrapper.next();
			Object[] values = ss.getValues();
			// this will be the logical names
			logicalNames.add(values[0] + "");
		}
		
		return logicalNames;
	}
	
	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static Set<String> getAllLogicalNamesFromConceptual(List<String> conceptualName, List<String> parentConceptualName) {
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);

		// create list of bindings
		List<String> conceptualUriSuffix = new Vector<String>();
		int size = conceptualName.size();
		for(int i = 0; i < size; i++) {
			String uriSuffix = conceptualName.get(i);
			if(parentConceptualName != null && parentConceptualName.get(i) != null && !parentConceptualName.get(i).trim().isEmpty()) {
				uriSuffix += "/" + parentConceptualName.get(i);
			}
			// add to list
			conceptualUriSuffix.add(uriSuffix);
		}
		
		StringBuilder bindings = createUriBindings("conceptualUri", conceptualUriSuffix);
		
		String query = "SELECT DISTINCT ?conceptLogical WHERE { "
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?conceptLogical}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptualUri}"
				+ "} ORDER BY ?conceptLogical " + bindings.toString();
		
		Set<String> logicalNames = new TreeSet<String>();

		// iterate through the results and append onto the list
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext()) {
			IHeadersDataRow ss = wrapper.next();
			Object[] values = ss.getValues();
			// this will be the logical names
			logicalNames.add(values[0] + "");
		}
		
		return logicalNames;
	}
	
	/**
	 * Get the list of concepts for a given engine
	 * @param engineName
	 * @return
	 */
	public static Set<String> getConceptsWithinEngine(String engineName) {
		/*
		 * Grab the local master engine and query for the concepts
		 * We do not want to load the engine until the user actually wants to use it
		 * 
		 */
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);

		Set<String> conceptsList = new TreeSet<String>();

		String query = "SELECT DISTINCT ?conceptConceptual WHERE { "
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
				+ "} ORDER BY ?conceptConceptual";

		// iterate through the results and append onto the list
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext()) {
			IHeadersDataRow ss = wrapper.next();
			Object[] values = ss.getValues();
			// this will be the logical names
			conceptsList.add(values[0] + "");
		}
		
		return conceptsList;
	}
	
	/**
	 * Get the properties for a given concept across all the databases
	 * @param conceptName
	 * @return
	 */
	public static Map<String, Object[]> getConceptProperties(List<String> conceptLogicalNames) {
		// get the bindings based on the input list
		StringBuilder logicalNameBindings = createUriBindings("conceptLogical", conceptLogicalNames);
		
		String propQuery = "SELECT DISTINCT ?engine ?conceptConceptual ?propConceptual WHERE "
				+ "{"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?conceptLogical}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
				
				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
				+ "{?propComposite <" + RDF.TYPE + "> ?conceptProp}"
				+ "{?propComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
				+ "{?propComposite <http://semoss.org/ontologies/Relation/conceptual> ?propConceptual}"

				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
				+ " && ?concept != <" + RDFS.Class + "> "
				+ " && ?concept != <" + RDFS.Resource + "> "
				+ " && ?conceptProp != <http://www.w3.org/2000/01/rdf-schema#Resource>"
				+ ")"
				+ "}"
				+ logicalNameBindings.toString();
		
		Map<String, Map<String, MetamodelVertex>> queryData = new TreeMap<String, Map<String, MetamodelVertex>>();
		
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, propQuery);
		while(wrapper.hasNext())
		{
			IHeadersDataRow stmt = wrapper.next();
			Object[] values = stmt.getValues();
			// we need the raw values for properties since we need to get the "class name"
			// as they are all defined (regardless of db type) as propName/parentName
			Object[] rawValues = stmt.getRawValues();
			
			// grab values
			String engineName = values[0] + "";
			String conceptConceptual = values[1] + "";
			String propConceptual = Utility.getClassName(rawValues[2] + "");
			
			Map<String, MetamodelVertex> engineMap = null;
			if(queryData.containsKey(engineName)) {
				engineMap = queryData.get(engineName);
			} else {
				engineMap = new TreeMap<String, MetamodelVertex>();
				// add to query data map
				queryData.put(engineName, engineMap);
			}
			
			// get or create the vertex
			MetamodelVertex vert = null;
			if(engineMap.containsKey(conceptConceptual)) {
				vert = engineMap.get(conceptConceptual);
			} else {
				vert = new MetamodelVertex(conceptConceptual);
				// add to the engine map
				engineMap.put(conceptConceptual, vert);
			}
			
			// add the property conceptual name
			vert.addProperty(propConceptual);
		}
		
		Map<String, Object[]> returnHash = new TreeMap<String, Object[]>();
		for(String engineName : queryData.keySet()) {
			returnHash.put(engineName, queryData.get(engineName).values().toArray());
		}
		
		return returnHash;
	}
	
	/**
	 * Get the list of  connected concepts for a given concept
	 * 
	 * Direction upstream/downstream is always in reference to the node being searched
	 * For example, if the relationship in the direction Title -> Genre
	 * The result would be { upstream -> [Genre] } because Title is upstream of Genre
	 * 
	 * @param conceptType
	 * @return
	 */
	public static Map getConnectedConcepts(List<String> conceptLogicalNames) {
		// get the bindings based on the input list
		StringBuilder logicalNameBindings = createUriBindings("toLogical", conceptLogicalNames);
		
		IEngine masterDB = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);

		// this is the final return object
		Map<String, Map> retMap = new TreeMap<String, Map>();
		
		String upstreamQuery = "SELECT DISTINCT ?someEngine ?fromConceptual ?toConceptual WHERE {"
				+ "{?fromConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"

				+ "{?fromConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				
				+ "{?toConceptComposite <" + RDF.TYPE + "> ?toConcept}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> ?toLogical}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?toConceptual}"

				+ "{?fromConceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?fromConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?fromConceptual}"
				
				+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?toConceptComposite ?someRel ?fromConceptComposite}"
				
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
				+ "&& ?toConcept != ?someEngine)"
				+ "}"
				+ logicalNameBindings;
		
		retMap = assimilateNodes(upstreamQuery, masterDB, retMap, DIRECTION_KEYS.UPSTREAM);
		
		// this query is identical except the from and to in the relationship triple are switched
		String downstreamQuery = "SELECT DISTINCT ?someEngine ?fromConceptual ?toConceptual WHERE {"
				+ "{?fromConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"

				+ "{?fromConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				
				+ "{?toConceptComposite <" + RDF.TYPE + "> ?toConcept}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> ?toLogical}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?toConceptual}"

				+ "{?fromConceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?fromConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?fromConceptual}"
				
				+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?fromConceptComposite ?someRel ?toConceptComposite}"
				
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
				+ "&& ?toConcept != ?someEngine)"
				+ "}"
				+ logicalNameBindings;
		
		retMap = assimilateNodes(downstreamQuery, masterDB, retMap, DIRECTION_KEYS.DOWNSTREAM);		
		return retMap;
	}
	
	private static Map assimilateNodes(String query, IEngine engine, Map<String, Map> enginesHash, DIRECTION_KEYS direction)
	{
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext())
		{
			// given the structure of the query
			// the first output is the engine
			// the second output is the node to traverse to
			// stream key tells us the direction
			// equivalent concept tells us the start concept since this is across databases
			
			IHeadersDataRow stmt = wrapper.next();
			Object[] values = stmt.getValues();
					
			String engineName = values[0] + "";
			String traverseConceptual = values[1] + "";
			String equivConceptual = values[2] + "";

			// grab existing hash for the engine if it exists
			Map<String, Object> thisEngineHash = null;
			if(enginesHash.containsKey(engineName)) {
				thisEngineHash = enginesHash.get(engineName);
			} else {
				thisEngineHash = new TreeMap<String, Object>();
				// add engine to overall map
				enginesHash.put(engineName,  thisEngineHash);
			}
			
			// within each engine
			// we need a separate map for each equivalent concept name
			// that gets mapped since we are searching using logical names
			// which doesn't guarantee uniqueness of the conceptual name within an engine
			
			Map<String, Object> equivConceptHash = null;
			if(thisEngineHash.containsKey(equivConceptual)) {
				equivConceptHash = (Map<String, Object>) thisEngineHash.get(equivConceptual);
			} else {
				equivConceptHash = new Hashtable<String, Object>();
				// add equiv concept to this engine hash
				thisEngineHash.put(equivConceptual, equivConceptHash);
			}
			
			// now for this equivalent conceptual
			// need to add the traverse in the appropriate upstream/downstream direction
			Set<String> directionList = null;
			if(equivConceptHash.containsKey(direction.toString())) {
				directionList = (Set<String>) equivConceptHash.get(direction.toString());
			} else {
				directionList = new TreeSet<String>();
				// add direction to equivalent conceptual map
				equivConceptHash.put(direction.toString(), directionList);
			}
			
			// add the conceptual name to the list for the FE to show
			directionList.add(traverseConceptual); 
		}
		
		return enginesHash;
	}
	
	private static StringBuilder createUriBindings(String varName, List<String> nameBindings) {
		StringBuilder logicalNameBindings = new StringBuilder(" BINDINGS ?" + varName + " {");
		int size = nameBindings.size();
		for(int i = 0; i < size; i++) {
			logicalNameBindings.append("(<http://semoss.org/ontologies/Concept/").append(nameBindings.get(i)).append(">)");
		}
		logicalNameBindings.append("}");
		return logicalNameBindings;
	}
	
	
	public static Map<String, Object[]> getMetamodel(String engineName)
	{
		// this needs to be moved to the name server
		// and this needs to be based on local master database
		// need this to be a simple OWL data
		// I dont know if it is worth it to load the engine at this point ?
		// or should I just load it ?
		// need to get local master and pump out the metamodel
		
		// final map to return
		// TODO: outline the structure
		Map<String, Map> edgeAndVertex = new Hashtable<String, Map>();
		
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		
		// query to get all the concepts and their properties
		String vertexQuery = "SELECT DISTINCT ?conceptConceptual (COALESCE(?propConceptual, ?noprop) as ?propConceptual) "
				+ " WHERE "
				+ "{BIND(<http://semoss.org/ontologies/Relation/contains/noprop/noprop> AS ?noprop)"
				
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
				
				+ "OPTIONAL{"
				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
				+ "{?propComposite <" + RDF.TYPE + "> ?prop}"
				+ "{?propComposite <http://semoss.org/ontologies/Relation/conceptual> ?propConceptual}"
				+ "}"
				
				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
				+ " && ?concept != <" + RDFS.Class + "> "
				+ " && ?concept != <" + RDFS.Resource + "> "
				+ ")"
				+ "}";

		// get all the vertices and their properties
		makeVertices(engine, vertexQuery, edgeAndVertex);
		
		// query to get all the edges between nodes
		String edgeQuery = "SELECT DISTINCT ?fromConceptual ?toConceptual ?someRel WHERE {"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
				+ "{?conceptComposite ?someRel ?toConceptComposite}"

				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?fromConceptual}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?toConceptual}"			
				
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?fromConcept != ?toConcept"
				+ "&& ?fromConcept != <" + RDFS.Class + "> "
				+ "&& ?toConcept != <" + RDFS.Class + "> "
				+ "&& ?fromConcept != <" + RDFS.Resource + "> "
				+ "&& ?toConcept != <" + RDFS.Resource + "> "
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
				+ "&& ?someRel != <" + RDFS.subClassOf + ">)}";

		// make the edges
		makeEdges(engine, edgeQuery, edgeAndVertex);
		
		Map<String, Object[]> finalHash = new Hashtable<String, Object[]>();
		finalHash.put("nodes", edgeAndVertex.get("nodes").values().toArray());
		finalHash.put("edges", edgeAndVertex.get("edges").values().toArray());

		// return the map
		return finalHash;
	}
	
	private static void makeVertices(IEngine engine, String query, Map<String, Map> edgesAndVertices)
	{		
		Map<String, MetamodelVertex> nodes = new TreeMap<String, MetamodelVertex>();
		if(edgesAndVertices.containsKey("nodes")) {
			nodes = (Map) edgesAndVertices.get("nodes");
		}
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext())
		{
			// based on query being passed in
			// only 2 values in array
			// first output is the concept conceptual name
			// second output is the property conceptual name
			IHeadersDataRow stmt = wrapper.next();
			Object[] values = stmt.getValues();
			// we need the raw values for properties since we need to get the "class name"
			// as they are all defined (regardless of db type) as propName/parentName
			Object[] rawValues = stmt.getRawValues();
			
			String conceptualConceptName = values[0] + "";
			String concpetualPropertyName = Utility.getClassName(rawValues[1] + "");

			// get or create the metamodel vertex
			MetamodelVertex node = null;
			if(nodes.containsKey(conceptualConceptName)) {
				node = nodes.get(conceptualConceptName);
			} else {
				node = new MetamodelVertex(conceptualConceptName);
				// add to the map of nodes
				nodes.put(conceptualConceptName, node);
			}
			
			// add the property 
			node.addProperty(concpetualPropertyName);
		}
		
		// add all the nodes back into the main map
		edgesAndVertices.put("nodes", nodes);
	}
	
	
	private static void makeEdges(IEngine engine, String query, Map<String, Map> edgesAndVertices)
	{	
		Map<String, Map<String, String>> edges = new Hashtable<String, Map<String, String>>();
		if(edgesAndVertices.containsKey("edges")) {
			edges = (Map<String, Map<String, String>>) edgesAndVertices.get("edges");
		}
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext())
		{
			// based on query being passed in
			// only 3 values in array
			// first output is the from conceptual name
			// second output is the to conceptual name
			// third output is the relationship name
			IHeadersDataRow stmt = wrapper.next();
			Object[] values = stmt.getValues();
			
			String fromConceptualName = values[0] + "";
			String toConceptualName = values[1] + "";
			String relName = values[2] + "";
			
			Map<String, String> edge = new Hashtable<String, String>();
			edge.put("source", fromConceptualName);
			edge.put("target", toConceptualName);

			// add to edges map
			edges.put(relName, edge);
		}
		// add to overall map
		edgesAndVertices.put("edges", edges);
	}
}



/**
 * Internal class to simplify the implementation
 * of combining alias names and properties 
 * on vertices
 * @author mahkhalil
 *
 */
class MetamodelVertex {
	
	// store the property conceptual names
	private Set<String> propSet = new TreeSet<String>();
	// the conceptual name for the concept
	private String conceptualName;
	
	public MetamodelVertex(String conceptualName) {
		this.conceptualName = conceptualName;
	}
	
	/**
	 * Add to the properties for the vertex
	 * @param propertyConceptual
	 * @param propertyAlias
	 */
	public void addProperty(String propertyConceptual) {
		if(propertyConceptual.equals("noprop")) {
			return;
		}
		propSet.add(propertyConceptual);
	}
	
	public String toString() {
		Map<String, Object> vertexMap = new Hashtable<String, Object>();
		vertexMap.put("conceptualName", this.conceptualName);
		vertexMap.put("propSet", this.propSet);
		return vertexMap.toString();
	}
}

