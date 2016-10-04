package prerna.sablecc.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

import com.hp.hpl.jena.vocabulary.RDFS;

import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DatabasePkqlService {

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
	
	/**
	 * Get the list of concepts for a given engine
	 * @param engineName
	 * @return
	 */
	public static List<String> getConceptsWithinEngine(String engineName) {
		/*
		 * Grab the local master engine and query for the concepts
		 * We do not want to load the engine until the user actually wants to use it
		 * 
		 * We have a query that will return all the results with a key "@ENGINE@" to be replaced with the engine name
		 * passed into the method
		 */
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);

		List<String> conceptsList = new ArrayList<String>();

		String query = "SELECT ?conceptLogical ?concept WHERE { "
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?concept <http://semoss.org/ontologies/Relation/conceptual> ?conceptLogical}"
				+ "}";

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
	public static Map<String, Map<String, String>> getConceptProperties(String conceptName) {
		String conceptURI = "http://semoss.org/ontologies/Concept/" + conceptName;

		String propQuery = "SELECT DISTINCT ?engine ?conceptProp ?concept ?propLogical ?conceptLogical WHERE "
				+ "{"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> <"+conceptURI + ">}"
				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
				+ "{?propComposite <" + RDF.TYPE + "> ?conceptProp}"
				+ "{?propComposite <http://semoss.org/ontologies/Relation/logical> ?propLogical}"
				+ "{?propComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
				+ " && ?concept != <" + RDFS.Class + "> "
				+ " && ?concept != <" + RDFS.Resource + "> "
				+" && ?conceptProp != <http://www.w3.org/2000/01/rdf-schema#Resource>"
				+")"
				+ "}";

		Map<String, Map<String, String>> returnHash = new Hashtable <String, Map<String, String>>();
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper((IEngine)DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME), propQuery);
		String [] vars = wrapper.getDisplayVariables();
		while(wrapper.hasNext())
		{
			ISelectStatement stmt = wrapper.next();
			String engineURI = stmt.getRawVar(vars[0]) + "";
			String engineName = Utility.getInstanceName(engineURI);
			String prop = stmt.getRawVar(vars[1]) + ""; // << this is physical
			String propLogical = stmt.getRawVar(vars[3])+ "";
			
			Map<String, String> propHash = new Hashtable <String, String>();
			if(returnHash.containsKey(engineName))
				propHash = returnHash.get(engineName);
			
			// need to find what the engine type for this engine is
			// and then suggest what to do
			String type = DIHelper.getInstance().getCoreProp().getProperty(engineName + "_" + Constants.TYPE);

			String propInstance = null;
			propInstance = Utility.getInstanceName(propLogical); // interestingly it is giving movie csv everytime
			propHash.put(propInstance, propLogical);
			returnHash.put(engineName, propHash);
		}
		return returnHash;
	}
	
	
	public static Map<String, Object[]> getMetamodel(String engineName)
	{
		// this needs to be moved to the name server
		// and this needs to be based on local master database
		// need this to be a simple OWL data
		// I dont know if it is worth it to load the engine at this point ?
		// or should I just load it ?
		// need to get local master and pump out the metamodel
		
		Hashtable <String, Hashtable> edgeAndVertex = new Hashtable<String, Hashtable>();
		
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		
		String engineString = "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}";

		if(engineName != null)
			engineString = "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}";


		String vertexQuery = "SELECT DISTINCT ?concept (COALESCE(?prop, ?noprop) as ?conceptProp) (COALESCE(?propLogical, ?noprop) as ?propLogicalF) ?conceptLogical WHERE "
				+ "{BIND(<http://semoss.org/ontologies/Relation/contains/noprop> AS ?noprop)"
				+ engineString
				//+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?conceptLogical}"
				+ "OPTIONAL{"
				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
				+ "{?propComposite <" + RDF.TYPE + "> ?prop}"
				+ "{?propComposite <http://semoss.org/ontologies/Relation/logical> ?propLogical}"
				+ "}"
				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
				+ " && ?concept != <" + RDFS.Class + "> "
				+ " && ?concept != <" + RDFS.Resource + "> "
				//+ "FILTER(
				//+" && ?conceptProp != <http://www.w3.org/2000/01/rdf-schema#Resource>"
				+")"
				+ "}";

		
		
		/*String vertexQuery = "SELECT DISTINCT ?concept (COALESCE(?prop, ?noprop) as ?conceptProp) WHERE {BIND(<http://semoss.org/ontologies/Relation/contains/noprop> AS ?noprop)"
				+ engineString
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?concept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "OPTIONAL{"
				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
				+ "{?propComposite <" + RDF.TYPE + "> ?prop}"
				+ "}"
				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept>"
				//+ "FILTER(
				//+"?conceptProp != <http://www.w3.org/2000/01/rdf-schema#Resource>
				+")"
				+"}";
		*/
		
		makeVertices(engine, vertexQuery, edgeAndVertex);
		
		if(engineName != null)
			engineString =  "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
					+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}";
		else
			engineString =  "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
					+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}";
		
		// all concepts with no database
		/*
		String edgeQuery = "SELECT DISTINCT ?fromConcept ?someRel ?toConcept WHERE {"
				+ engineString
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
				+ "{?conceptComposite ?someRel ?toConceptComposite}"
				+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>)}";
		*/
	
		String edgeQuery = "SELECT DISTINCT ?fromConcept ?someRel ?toConcept ?fromLogical ?toLogical WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?fromLogical}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> ?toLogical}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
				+ "{?conceptComposite ?someRel ?toConceptComposite}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
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
		// get everything linked to a keyword
		// so I dont have a logical concept
		// I cant do this
		
		Object [] vertArray = (Object[])edgeAndVertex.get("nodes").values().toArray();
		Object [] edgeArray = (Object[])edgeAndVertex.get("edges").values().toArray();
		Map<String, Object[]> finalArray = new Hashtable<String, Object[]>();
		finalArray.put("nodes", vertArray);
		finalArray.put("edges", edgeArray);

		return finalArray;
	}
	
	private static void makeVertices(IEngine engine, String query, Hashtable <String, Hashtable>edgesAndVertices)
	{		
		System.out.println("Executing Query.. ");
		System.out.println(query);
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		Hashtable nodes = new Hashtable();
		if(edgesAndVertices.containsKey("nodes"))
			nodes = (Hashtable)edgesAndVertices.get("nodes");
		while(wrapper.hasNext())
		{
			//?concept (COALESCE(?prop, ?noprop) as ?conceptProp) (COALESCE(?propLogical, ?noprop) as ?propLogicalF) ?conceptLogical 
					
			ISelectStatement stmt = wrapper.next();
			String concept = stmt.getRawVar("concept") + "";
			String prop = stmt.getRawVar("conceptProp") + "";
			String logicalProp = stmt.getRawVar("propLogicalF") + "";
			String logicalConcept = stmt.getRawVar("conceptLogical") + ""; // <<-- this is the URI he is looking for
			
			
			String physicalName = Utility.getInstanceName(logicalConcept); // << changing this to get it based on actual name - this is wrong I think
			String propName = Utility.getInstanceName(logicalProp);

			SEMOSSVertex thisVert = null;
			if(nodes.containsKey(logicalConcept)) // stupid
				thisVert = (SEMOSSVertex)nodes.get(logicalConcept); // <<- this should be logical not physical
			else
			{
				thisVert = new SEMOSSVertex(logicalConcept);
				thisVert.propHash.put("PhysicalName", physicalName);
				thisVert.propHash.put("LOGICAL", logicalConcept);
			}
			if(!prop.equalsIgnoreCase("http://semoss.org/ontologies/Relation/contains/noprop") && !prop.equalsIgnoreCase("http://www.w3.org/2000/01/rdf-schema#Resource"))
			{
				thisVert.setProperty(prop, propName);
				thisVert.propHash.put(propName, propName); // << Seems like this is the one that gets picked up
				Hashtable <String, String> propUriHash = (Hashtable<String, String>) thisVert.propHash.get("propUriHash");
				Hashtable <String, String> logHash = new Hashtable<String, String>();
  				if(thisVert.propHash.containsKey("propLogHash"))
  					logHash = (Hashtable <String, String>)thisVert.propHash.get("propLogHash");
					
				logHash.put(propName+"_PHYSICAL", prop);
				propUriHash.put(propName,  logicalProp);
				//propUriHash.put(propName,  logicalProp);
				thisVert.propHash.put("propLogHash", logHash);
			}
			nodes.put(logicalConcept, thisVert);
			System.out.println("Made a vertex....  " + concept);
		}
		edgesAndVertices.put("nodes", nodes);
	}
	
	
	private static void makeEdges(IEngine engine, String query, Hashtable <String, Hashtable> edgesAndVertices)
	{	
		Hashtable nodes = new Hashtable();
		Hashtable edges = new Hashtable();
		if(edgesAndVertices.containsKey("nodes"))
			nodes = (Hashtable)edgesAndVertices.get("nodes");
		
		if(edgesAndVertices.containsKey("edges"))
			edges = (Hashtable)edgesAndVertices.get("edges");
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		while(wrapper.hasNext())
		{
			ISelectStatement stmt = wrapper.next();
			String fromConcept = stmt.getRawVar("fromLogical") + "";
			String toConcept = stmt.getRawVar("toLogical") + "";
			String relName = stmt.getRawVar("someRel") + "";
			
			SEMOSSVertex outVertex = (SEMOSSVertex)nodes.get(fromConcept);
			SEMOSSVertex inVertex = (SEMOSSVertex)nodes.get(toConcept);
			
			if(outVertex != null && inVertex != null) // there is only so much inferencing one can filter
			{
				SEMOSSEdge edge = new SEMOSSEdge(outVertex, inVertex, relName);
				edges.put(relName, edge);
			}
		}
		edgesAndVertices.put("edges", edges);
	}

	
	
}
