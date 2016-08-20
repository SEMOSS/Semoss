package prerna.engine.impl;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;

import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.api.IExplorable;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.util.Constants;
import prerna.util.Utility;

public class MetaHelper implements IExplorable {

	public RDFFileSesameEngine baseDataEngine = null;
	Logger logger = Logger.getLogger(getClass());
	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
	private transient Map<String, String> tableUriCache = new HashMap<String, String>();
	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	IEngine.ENGINE_TYPE engineType = IEngine.ENGINE_TYPE.RDBMS;
	String engineName = null;
	private static final String FROM_SPARQL = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel  ?y} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
			+ "}";

	private static final String TO_SPARQL = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel ?y} "
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
			+ "}";

	
	protected Hashtable<String,String> transformedNodeNames = new Hashtable<String,String>();

	public MetaHelper(RDFFileSesameEngine baseDataEngine, IEngine.ENGINE_TYPE engineType, String engineName)
	{
		this.baseDataEngine = baseDataEngine;
		if(engineType !=  null)
			this.engineType = engineType;
		if(engineName != null)
			this.engineName = engineName;
		else
			this.engineName = "Unassigned";
	}
	
	@Override
	public Vector<String> getPerspectives() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getInsights(String perspective) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getInsights() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<Insight> getInsight(String... id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getInsight4Type(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getInsight4Tag(String tag) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getFromNeighbors(String physicalNodeType, int neighborHood) {
		// this is where this node is the from node
		//String physicalNodeType = getTransformedNodeName(Constants.DISPLAY_URI + Utility.getInstanceName(nodeType), false);
		String query = "SELECT DISTINCT ?node WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
		+ "{?node ?rel ?start}}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	// gets the to nodes
	public Vector<String> getToNeighbors(String physicalNodeType, int neighborHood) {
		// this is where this node is the to node
		String query = "SELECT DISTINCT ?node WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
		+ "{?start ?rel ?node}}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	// gets the from and to nodes
	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
		Vector<String> from = getFromNeighbors(nodeType, 0);
		Vector<String> to = getToNeighbors(nodeType, 0);
		from.addAll(to);
		return from;
	}


	@Override
	public Vector<SEMOSSParam> getParams(String insightName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setDreamer(String dreamer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setOWL(String owl) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getOWLDefinition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void commitOWL() {
		// TODO Auto-generated method stub

	}

	@Override
	public void addProperty(String key, String value) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getProperty(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<Object> getParamOptions(String parameterURI) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IQueryBuilder getQueryBuilder() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public IEngine getInsightDatabase() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> getAllInsightsMetaData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInsightDefinition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<SEMOSSParam> getParams(String... paramIds) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Vector<String> executeInsightQuery(String sparqlQuery,
			boolean isDbQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNodeBaseUri() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public Vector<String> getConcepts() {
		String query = "SELECT ?concept WHERE {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}
	
	/**
	 * Goes to the owl using a regex sparql query to get the physical uri
	 * @param physicalName e.g. Studio
	 * @return e.g. http://semoss.org/ontologies/Concept/Studio
	 */
	public String getConceptUri4PhysicalName(String physicalName){
		if(tableUriCache.containsKey(physicalName)){
			return tableUriCache.get(physicalName);
		}
		Vector<String> cons = this.getConcepts();
		for(String checkUri : cons){
			if(Utility.getInstanceName(checkUri).equals(physicalName)){
				tableUriCache.put(physicalName, checkUri);
				return checkUri;
			}
		}

		return "unable to get table uri for " + physicalName;
	}

	public List<String> getProperties4Concept(String concept, Boolean logicalNames) {
		String uri = concept;
		if(!uri.contains("http://")){
			uri = Constants.DISPLAY_URI + uri;
		}
		String query = "SELECT ?property WHERE { {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?concept  <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property}"
				+ "{?property a <" + CONTAINS_BASE_URI + ">}}"
				+ "BINDINGS ?concept {(<"+ this.getTransformedNodeName(uri, false) +">)}";
		List<String> uriProps = Utility.getVectorOfReturn(query, baseDataEngine, true);
		if(!logicalNames){
			return uriProps;
		}
		else{
			// need to go through each one and translate
			List<String> propNames = new Vector<String>();
			for(String uriProp : uriProps){
				String logicalName = this.getTransformedNodeName(uriProp, true);
				propNames.add(logicalName);
			}
			return propNames;
		}
	}
	
	/**
	 * query the owl to get the display name or the physical name
	 */
	public String getTransformedNodeName(String nodeURI, boolean getDisplayName){
		//String returnNodeURI = nodeURI;
		
		//these validation peices are seperated out intentionally for readability.
		if(baseDataEngine == null || nodeURI == null || nodeURI.isEmpty() ){ 
			return nodeURI;
		}
		
		//for rdbms normalize the URI... for concepts and relation uris
		if (nodeURI.startsWith(Constants.CONCEPT_URI) || nodeURI.startsWith(Constants.PROPERTY_URI) && engineType.equals(IEngine.ENGINE_TYPE.RDBMS)) {
			for(String displayName: this.transformedNodeNames.keySet()){
				String physicalName = this.transformedNodeNames.get(displayName);
				if(physicalName.equalsIgnoreCase(nodeURI)){
					nodeURI = physicalName;
					break;
				}
			}
		}
		
		//if you are trying to get the physical name, but you came in here with out the display name uri component, exit out
		if(!nodeURI.startsWith(Constants.DISPLAY_URI) && !getDisplayName){
			return nodeURI;
		}
		//if you are trying to get a display name but you came in with out the physical URI component, exit out
		if(getDisplayName && !(nodeURI.startsWith(Constants.CONCEPT_URI) || nodeURI.startsWith(Constants.PROPERTY_URI))){
			return nodeURI;
		}
		
		//if uri coming in is just a base URI...
		if(nodeURI.equals(Constants.DISPLAY_URI) || nodeURI.equals(Constants.CONCEPT_URI) || nodeURI.equals(Constants.PROPERTY_URI)){
			return nodeURI;
		}
		
		//first check the Hashtable to see if its already existing, so you dont need to query any databases.
		//the key is the logical name since those can be unique (properties names may be the same across types)
		return findTransformedNodeName(nodeURI, getDisplayName);

	}

	public void setTransformedNodeNames(Hashtable transformedNodeNames){
		this.transformedNodeNames = transformedNodeNames;
	}
	
	@Override
	public void loadTransformedNodeNames(){
		String query = "SELECT DISTINCT ?object (COALESCE(?DisplayName, ?object) AS ?Display) WHERE { "
				+ " { {?object <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ "OPTIONAL{?object <http://semoss.org/ontologies/DisplayName> ?DisplayName } } UNION { "
				+ "{ ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> } "
				+ "OPTIONAL {?object <http://semoss.org/ontologies/DisplayName> ?DisplayName }"
				+ "} }"; 
		if(this.baseDataEngine!=null){
			Vector<String[]> transformedNode = Utility.getVectorObjectOfReturn(query, this.baseDataEngine);
		
			if(transformedNode.size()!=0){
				transformedNodeNames.clear();
				for(String[] node: transformedNode){
					String logicalName = node[1];
					if(logicalName.equals("http://semoss.org/ontologies/Concept")) {
						this.transformedNodeNames.put(logicalName, Constants.DISPLAY_URI + "Concept");
						continue;
					} else if(logicalName.startsWith("http://semoss.org/ontologies/Relation/Contains/")) {
						logicalName = Utility.getInstanceName(logicalName);
					} else {
						logicalName = logicalName.replaceAll(".*/Concept/", "");
					}
					if(logicalName.contains("/")) {
						// this is for RDBMS engine OWL file concepts
						// this is for properties that are also "concepts"
						logicalName = logicalName.substring(0, logicalName.lastIndexOf("/"));
					} 
					logicalName = Utility.cleanVariableString(logicalName);
					logicalName = Constants.DISPLAY_URI + logicalName;
					if(this.transformedNodeNames.containsKey(logicalName)) {
						// this occurs when we have a property that is both a prop and a concept
						// keep the concept one i guess?
						if(node[0].contains("Relation/Contains")) {
							continue;
						}
						this.transformedNodeNames.put(logicalName, node[0]); //map contains display name : physical name
					} else {
						this.transformedNodeNames.put(logicalName, node[0]); //map contains display name : physical name
					}
				}
				this.baseDataEngine.setTransformedNodeNames(this.transformedNodeNames);
			}
		}
	}
	
	public String findTransformedNodeName(String nodeURI, boolean getDisplayName){
		
		if(this.transformedNodeNames.containsKey(nodeURI) && !getDisplayName){
			String physicalName = this.transformedNodeNames.get(nodeURI); 
			if(!physicalName.equalsIgnoreCase(nodeURI)){ // I have to do this because of RDBMS and its inconsistency with capitalizing concepts
				return physicalName;
			} else {
				return nodeURI;
			}
		} else if(this.transformedNodeNames.contains(nodeURI) && getDisplayName){
			for(String displayName: this.transformedNodeNames.keySet()){
				String physicalName = this.transformedNodeNames.get(displayName);
				if(physicalName.equalsIgnoreCase(nodeURI)){
					if(!displayName.equalsIgnoreCase(nodeURI)){ // I have to do this because of RDBMS and its inconsistency with capitalizing concepts
						return displayName;
					} else {
						return nodeURI;
					}
				}
			}
		} else if (nodeURI.startsWith(Constants.DISPLAY_URI)) {
			for(String displayName: this.transformedNodeNames.keySet()){
				if(Utility.getInstanceName(displayName).equalsIgnoreCase(Utility.getInstanceName(nodeURI))){
					return this.transformedNodeNames.get(displayName);
				}
			}
		}
		
		return nodeURI;
	}
	
	/**
	 * Runs a select query on the base data engine of this engine
	 */
	public Object execOntoSelectQuery(String query) {
		logger.debug("Running select query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		return this.baseDataEngine.execQuery(query);
	}

	/**
	 * Runs insert query on base data engine of this engine
	 */
	public void ontoInsertData(String query) {
		logger.debug("Running insert query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		baseDataEngine.insertData(query);
	}

	/**
	 * This method runs an update query on the base data engine which contains all owl and metamodel information
	 */
	public void ontoRemoveData(String query) {
		logger.debug("Running update query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		baseDataEngine.removeData(query);
	}

	@Override
	public Vector<String> getConcepts2(boolean conceptualNames) {
		String query = "";
		if(!conceptualNames) {
			query = "SELECT ?concept WHERE {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }";
		} else {
			query = "SELECT DISTINCT (COALESCE(?conceptual, ?concept) AS ?retConcept) WHERE { "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
						+ "OPTIONAL {"
							+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual }"
						+ "}" // end optional for conceputal names if present
					+ "}"; // end where
		}		
		
		
		
		
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	@Override
	public List<String> getProperties4Concept2(String concept,
			Boolean conceptualNames) {
		// get the physical URI for the concept
		String conceptPhysical = getPhysicalUriFromConceptualUri(concept);
		
		String query = null;
		// instead of getting the URIs in a set format and having to do a conversion afterwards
		// just have the query get the values directly
		if(conceptualNames) {
			query = "SELECT DISTINCT ?propertyConceptual WHERE { "
					+ "BIND(<" + conceptPhysical + "> AS ?concept) "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
					+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
					+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
//					+ "{?propertyConceptual <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
					+ "{?property <http://semoss.org/ontologies/Relation/Conceptual> ?propertyConceptual} "
					+ "}";
		} else {
			query = "SELECT DISTINCT ?property WHERE { "
					+ "BIND(<" + conceptPhysical + "> AS ?concept) "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
					+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
					+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
					+ "}";
		}
		
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	@Override
	public String getPhysicalUriFromConceptualUri(String conceptualURI) {
		// the URI is not valid if it doesn't start with http://
		// if it is not valid, assume only the last part of the URI was passed and create the URI
		// the URI is not valid if it doesn't start with http://
		// if it is not valid, assume only the last part of the URI was passed and create the URI
		if(!conceptualURI.startsWith("http://")) {
			conceptualURI = "http://semoss.org/ontologies/Concept/" + conceptualURI;
		}
		
		// this query needs to take into account if the conceptual URI is a concept or a property
		String query = "SELECT DISTINCT ?uri WHERE { "
				+ "BIND(<" + conceptualURI + "> AS ?conceptual) "
				+ "{"
					+ "{?uri <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
					+ "{?uri <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual } "
				+ "} UNION {"
					+ "{?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> } "
					+ "{?uri <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual } "
				+ "}"
				+ "}";

		Vector<String> queryReturn = Utility.getVectorOfReturn(query, baseDataEngine, true);
		// if it is empty, either the URI is bad or it is already the physical URI
		if(queryReturn.isEmpty()) {
			return conceptualURI;
		}
		// there should only be one return in the vector since conceptual URIs are a one-to-one match with the physical URIs
		return queryReturn.get(0);
	}

	@Override
	public String getDataTypes(String uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getDataTypes(String... uris) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getParentOfProperty(String property) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueryStruct getDatabaseQueryStruct() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getMetamodel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getConceptualUriFromPhysicalUri(String physicalURI) {

		String query = "SELECT DISTINCT ?conceptual WHERE { "
				+ "BIND(<" + physicalURI + "> AS ?uri) "
				+ "{?uri <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual } "
				+ "}"; // end where

		Vector<String> queryReturn = Utility.getVectorOfReturn(query, baseDataEngine, true);
		// there should only be one return in the vector since conceptual URIs are a one-to-one match with the physical URIs
		if(queryReturn.isEmpty()) {
			return physicalURI;
		}
		return queryReturn.get(0);
	}

	@Override
	public String getOWL() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
